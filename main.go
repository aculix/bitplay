package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/exec"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"net/url"
	"path/filepath"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"
	"golang.org/x/net/proxy"
)

var (
	currentSettings Settings
	settingsMutex   sync.RWMutex
)

type TorrentSession struct {
	Client   *torrent.Client
	Torrent  *torrent.Torrent
	Port     int
	LastUsed time.Time
	ActiveStreams int32
	ProbeMu       sync.Mutex
	ProbeCache    map[int]*ProbeResult
}

type ProbeStream struct {
	Index    int    `json:"index"` // absolute ffmpeg stream index
	Type     string `json:"type"`  // video | audio | subtitle
	Codec    string `json:"codec"`
	Profile  string `json:"profile,omitempty"`
	Language string `json:"language,omitempty"`
	Title    string `json:"title,omitempty"`
	Channels int    `json:"channels,omitempty"`
	Default  bool   `json:"default"`
}

type ProbeResult struct {
	Container string        `json:"container"`
	Duration  float64       `json:"duration"`
	Streams   []ProbeStream `json:"streams"`
}

type Settings struct {
	EnableProxy    bool   `json:"enableProxy"`
	ProxyURL       string `json:"proxyUrl"`
	EnableProwlarr bool   `json:"enableProwlarr"`
	ProwlarrHost   string `json:"prowlarrHost"`
	ProwlarrApiKey string `json:"prowlarrApiKey"`
	EnableJackett  bool   `json:"enableJackett"`
	JackettHost    string `json:"jackettHost"`
	JackettApiKey  string `json:"jackettApiKey"`
	SearchTimeoutSeconds int  `json:"searchTimeoutSeconds"`
	SkipTLSVerify        bool `json:"skipTlsVerify"`
}

func searchTimeout() time.Duration {
	settingsMutex.RLock()
	defer settingsMutex.RUnlock()
	if currentSettings.SearchTimeoutSeconds > 0 {
		return time.Duration(currentSettings.SearchTimeoutSeconds) * time.Second
	}
	return 30 * time.Second
}

type ProxySettings struct {
	EnableProxy bool   `json:"enableProxy"`
	ProxyURL    string `json:"proxyUrl"`
}

type ProwlarrSettings struct {
	EnableProwlarr bool   `json:"enableProwlarr"`
	ProwlarrHost   string `json:"prowlarrHost"`
	ProwlarrApiKey string `json:"prowlarrApiKey"`
	// Shared search options, editable from either indexer tab
	SearchTimeoutSeconds int  `json:"searchTimeoutSeconds"`
	SkipTLSVerify        bool `json:"skipTlsVerify"`
}

type JackettSettings struct {
	EnableJackett bool   `json:"enableJackett"`
	JackettHost   string `json:"jackettHost"`
	JackettApiKey string `json:"jackettApiKey"`
	// Shared search options, editable from either indexer tab
	SearchTimeoutSeconds int  `json:"searchTimeoutSeconds"`
	SkipTLSVerify        bool `json:"skipTlsVerify"`
}

var (
	sessions  sync.Map
	usedPorts sync.Map
	portMutex sync.Mutex
)

type ringLogBuffer struct {
	mu    sync.Mutex
	lines []string
	max   int
}

func (b *ringLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, line := range strings.Split(strings.TrimRight(string(p), "\n"), "\n") {
		b.lines = append(b.lines, line)
	}
	if over := len(b.lines) - b.max; over > 0 {
		b.lines = b.lines[over:]
	}
	return len(p), nil
}

func (b *ringLogBuffer) Lines() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]string, len(b.lines))
	copy(out, b.lines)
	return out
}

var logBuffer = &ringLogBuffer{max: 500}

var (
	serverPort      = 3347
	ffmpegPath      string
	ffprobePath     string
	ffmpegAvailable bool
	ffmpegVersion   string
	remuxSlots      chan struct{}
	loopbackAuthHeader string
)

func checkFFmpeg() {
	maxRemux := 3
	if v, err := strconv.Atoi(os.Getenv("BITPLAY_MAX_REMUX")); err == nil && v > 0 {
		maxRemux = v
	}
	remuxSlots = make(chan struct{}, maxRemux)

	var err error
	ffmpegPath, err = exec.LookPath("ffmpeg")
	if err != nil {
		log.Println("ffmpeg not found in PATH - MKV remuxing and audio track selection disabled")
		return
	}
	ffprobePath, err = exec.LookPath("ffprobe")
	if err != nil {
		log.Println("ffprobe not found in PATH - MKV remuxing and audio track selection disabled")
		return
	}
	if out, verr := exec.Command(ffmpegPath, "-version").Output(); verr == nil {
		fields := strings.Fields(strings.SplitN(string(out), "\n", 2)[0])
		if len(fields) >= 3 {
			ffmpegVersion = fields[2]
		}
	}
	ffmpegAvailable = true
	log.Printf("ffmpeg %s detected - remuxing enabled (max %d concurrent, BITPLAY_MAX_REMUX to change)", ffmpegVersion, maxRemux)
}

func capabilitiesHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	respondWithJSON(w, http.StatusOK, map[string]interface{}{
		"ffmpeg":  ffmpegAvailable,
		"version": ffmpegVersion,
	})
}

// ffmpeg reads torrent files through the stream endpoint so the input is
// seekable and partially-downloaded files work
func loopbackStreamURL(sessionID string, fileIndex int) string {
	return fmt.Sprintf("http://127.0.0.1:%d/api/v1/torrent/%s/stream/%d", serverPort, sessionID, fileIndex)
}

type limitedWriter struct {
	buf bytes.Buffer
	max int
}

func (l *limitedWriter) Write(p []byte) (int, error) {
	if l.buf.Len() < l.max {
		l.buf.Write(p[:min(len(p), l.max-l.buf.Len())])
	}
	return len(p), nil
}

// Helper function to format file sizes
func formatSize(sizeInBytes float64) string {
	if sizeInBytes < 1024 {
		return fmt.Sprintf("%.0f B", sizeInBytes)
	}

	sizeInKB := sizeInBytes / 1024
	if sizeInKB < 1024 {
		return fmt.Sprintf("%.2f KB", sizeInKB)
	}

	sizeInMB := sizeInKB / 1024
	if sizeInMB < 1024 {
		return fmt.Sprintf("%.2f MB", sizeInMB)
	}

	sizeInGB := sizeInMB / 1024
	return fmt.Sprintf("%.2f GB", sizeInGB)
}

var proxyTransport = &http.Transport{
	TLSHandshakeTimeout:   10 * time.Second,
	ExpectContinueTimeout: 1 * time.Second,
	IdleConnTimeout:       30 * time.Second,
	MaxIdleConnsPerHost:   10,
}

// Basic auth for the whole app when BITPLAY_AUTH_USERNAME and
// BITPLAY_AUTH_PASSWORD are both set; a no-op otherwise
func basicAuthMiddleware(next http.Handler) http.Handler {
	username := os.Getenv("BITPLAY_AUTH_USERNAME")
	password := os.Getenv("BITPLAY_AUTH_PASSWORD")
	if username == "" || password == "" {
		return next
	}

	userHash := sha256.Sum256([]byte(username))
	passHash := sha256.Sum256([]byte(password))
	loopbackAuthHeader = "Basic " + base64.StdEncoding.EncodeToString([]byte(username+":"+password))
	log.Println("Basic authentication enabled (BITPLAY_AUTH_USERNAME/BITPLAY_AUTH_PASSWORD)")

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if ok {
			uh := sha256.Sum256([]byte(user))
			ph := sha256.Sum256([]byte(pass))
			userMatch := subtle.ConstantTimeCompare(userHash[:], uh[:]) == 1
			passMatch := subtle.ConstantTimeCompare(passHash[:], ph[:]) == 1
			if userMatch && passMatch {
				next.ServeHTTP(w, r)
				return
			}
		}
		w.Header().Set("WWW-Authenticate", `Basic realm="BitPlay", charset="UTF-8"`)
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
	})
}

func copyHeadersOnRedirect(req *http.Request, via []*http.Request) error {
	if len(via) >= 10 {
		return errors.New("too many redirects")
	}
	for k, vv := range via[0].Header {
		if _, ok := req.Header[k]; !ok {
			req.Header[k] = vv
		}
	}
	return nil
}

func createSelectiveProxyClient() *http.Client {
	settingsMutex.RLock()
	enableProxy := currentSettings.EnableProxy
	proxyURL := currentSettings.ProxyURL
	skipVerify := currentSettings.SkipTLSVerify
	settingsMutex.RUnlock()

	timeout := searchTimeout()

	var tlsConfig *tls.Config
	if skipVerify {
		tlsConfig = &tls.Config{InsecureSkipVerify: true}
	}

	if !enableProxy {
		return &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				TLSHandshakeTimeout:   10 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
				IdleConnTimeout:       30 * time.Second,
				MaxIdleConnsPerHost:   10,
				TLSClientConfig:       tlsConfig,
			},
		}
	}
	// Reconfigure proxyTransport’s DialContext if URL changed:
	dialer, _ := createProxyDialer(proxyURL)
	proxyTransport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		return dialer.Dial(network, addr)
	}
	proxyTransport.TLSClientConfig = tlsConfig
	// Drop any old idle conns after reconfiguration:
	proxyTransport.CloseIdleConnections()

	return &http.Client{
		Transport:     proxyTransport,
		Timeout:       timeout,
		CheckRedirect: copyHeadersOnRedirect,
	}
}

// Create a proxy dialer for SOCKS5
func createProxyDialer(proxyURL string) (proxy.Dialer, error) {
	proxyURLParsed, err := url.Parse(proxyURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse proxy URL: %v", err)
	}

	// Extract auth information
	auth := &proxy.Auth{}
	if proxyURLParsed.User != nil {
		auth.User = proxyURLParsed.User.Username()
		if password, ok := proxyURLParsed.User.Password(); ok {
			auth.Password = password
		}
	}

	// Create a SOCKS5 dialer
	return proxy.SOCKS5("tcp", proxyURLParsed.Host, auth, proxy.Direct)
}

// Implement a port allocation function to prevent conflicts
func getAvailablePort() int {
	portMutex.Lock()
	defer portMutex.Unlock()

	// Try up to 50 times to find an unused port
	for i := 0; i < 50; i++ {
		// Generate a random port in the high range
		port := 10000 + rand.Intn(50000)

		// Check if this port is already in use by our app
		if _, exists := usedPorts.Load(port); !exists {
			// Mark this port as used
			usedPorts.Store(port, true)
			return port
		}
	}

	// If we can't find an available port, return a very high random port
	// as a last resort
	return 60000 + rand.Intn(5000)
}

// Release a port when we're done with it
func releasePort(port int) {
	portMutex.Lock()
	defer portMutex.Unlock()
	usedPorts.Delete(port)
}

// Initialize the torrent client with proxy settings
func initTorrentWithProxy() (*torrent.Client, int, error) {
	settingsMutex.RLock()
	enableProxy := currentSettings.EnableProxy
	proxyURL := currentSettings.ProxyURL
	settingsMutex.RUnlock()

	config := torrent.NewDefaultClientConfig()
	config.DefaultStorage = storage.NewFile("./torrent-data")
	port := getAvailablePort()
	config.ListenPort = port

	if enableProxy {
		log.Println("Creating torrent client with proxy...")
		os.Setenv("ALL_PROXY", proxyURL)
		os.Setenv("SOCKS_PROXY", proxyURL)
		os.Setenv("HTTP_PROXY", proxyURL)
		os.Setenv("HTTPS_PROXY", proxyURL)

		proxyDialer, err := createProxyDialer(proxyURL)
		if err != nil {
			releasePort(port)
			return nil, port, fmt.Errorf("could not create proxy dialer: %v", err)
		}

		config.HTTPProxy = func(*http.Request) (*url.URL, error) {
			return url.Parse(proxyURL)
		}

		client, err := torrent.NewClient(config)
		if err != nil {
			releasePort(port)
			return nil, port, err
		}

		setValue(client, "dialerNetwork", func(ctx context.Context, network, addr string) (net.Conn, error) {
			return proxyDialer.Dial(network, addr)
		})

		return client, port, nil
	}

	log.Println("Creating torrent client without proxy...")
	os.Unsetenv("ALL_PROXY")
	os.Unsetenv("SOCKS_PROXY")
	os.Unsetenv("HTTP_PROXY")
	os.Unsetenv("HTTPS_PROXY")

	client, err := torrent.NewClient(config)
	if err != nil {
		releasePort(port)
		return nil, port, err
	}
	return client, port, nil
}

// Helper function to try to set a field value using reflection
// This is a bit hacky but might help override the client's dialer
func setValue(obj interface{}, fieldName string, value interface{}) {
	// This is a best-effort approach that may not work with all library versions
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Warning: Could not set %s field: %v", fieldName, r)
		}
	}()

	reflectValue := reflect.ValueOf(obj).Elem()
	field := reflectValue.FieldByName(fieldName)

	if field.IsValid() && field.CanSet() {
		field.Set(reflect.ValueOf(value))
		log.Printf("Successfully set %s to use proxy", fieldName)
	}
}

// Override system settings with our proxy
func init() {

	// check if settings.json exists
	if _, err := os.Stat("config/settings.json"); os.IsNotExist(err) {
		log.Println("settings.json not found, creating default settings")
		defaultSettings := Settings{
			EnableProxy:    false,
			ProxyURL:       "",
			EnableProwlarr: false,
			ProwlarrHost:   "",
			ProwlarrApiKey: "",
			EnableJackett:  false,
			JackettHost:    "",
			JackettApiKey:  "",
		}
		// Create the config directory if it doesn't exist
		if err := os.MkdirAll("config", 0755); err != nil {
			log.Fatalf("Failed to create config directory: %v", err)
		}
		settingsFile, err := os.Create("config/settings.json")
		if err != nil {
			log.Fatalf("Failed to create settings.json: %v", err)
		}
		defer settingsFile.Close()
		encoder := json.NewEncoder(settingsFile)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(defaultSettings); err != nil {
			log.Fatalf("Failed to encode default settings: %v", err)
		}
		log.Println("Default settings created in settings.json")
	}

	// Load settings from settings.json
	settingsFile, err := os.Open("config/settings.json")
	if err != nil {
		log.Fatalf("Failed to open settings.json: %v", err)
	}
	defer settingsFile.Close()

	var s Settings
	if err := json.NewDecoder(settingsFile).Decode(&s); err != nil {
		log.Fatalf("Failed to decode settings.json: %v", err)
	}

	settingsMutex.Lock()
	currentSettings = s
	settingsMutex.Unlock()
}

func main() {
	log.SetOutput(io.MultiWriter(os.Stderr, logBuffer))

	checkFFmpeg()

	// Seed random number generator
	rand.Seed(time.Now().UnixNano())

	// Force proxy for all Go HTTP connections
	setGlobalProxy()

	// Set up endpoint handlers
	http.HandleFunc("/api/v1/torrent/add", addTorrentHandler)
	http.HandleFunc("/api/v1/torrent/", torrentHandler)
	http.HandleFunc("/api/v1/settings", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			settingsMutex.RLock()
			snapshot := currentSettings
			settingsMutex.RUnlock()

			resp := struct {
				Settings
				ProwlarrApiKeySet bool `json:"prowlarrApiKeySet"`
				JackettApiKeySet  bool `json:"jackettApiKeySet"`
			}{snapshot, snapshot.ProwlarrApiKey != "", snapshot.JackettApiKey != ""}
			resp.ProwlarrApiKey = ""
			resp.JackettApiKey = ""
			respondWithJSON(w, http.StatusOK, resp)
		} else {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})
	http.HandleFunc("/api/v1/settings/proxy", saveProxySettingsHandler)
	http.HandleFunc("/api/v1/settings/prowlarr", saveProwlarrSettingsHandler)
	http.HandleFunc("/api/v1/settings/jackett", saveJackettSettingsHandler)
	http.HandleFunc("/api/v1/prowlarr/search", searchFromProwlarr)
	http.HandleFunc("/api/v1/jackett/search", searchFromJackett)
	http.HandleFunc("/api/v1/prowlarr/test", testProwlarrConnection)
	http.HandleFunc("/api/v1/jackett/test", testJackettConnection)
	http.HandleFunc("/api/v1/proxy/test", testProxyConnection)
	http.HandleFunc("/api/v1/torrent/convert", convertTorrentToMagnetHandler)
	http.HandleFunc("/api/v1/logs", logsHandler)
	http.HandleFunc("/api/v1/sessions", sessionsHandler)
	http.HandleFunc("/api/v1/cache/purge", purgeCacheHandler)
	http.HandleFunc("/api/v1/capabilities", capabilitiesHandler)

	// Set up client file serving
	// no-cache: revalidate on every load so updates aren't stuck behind stale assets
	fileServer := http.FileServer(http.Dir("./client"))
	noCacheFiles := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-cache")
		fileServer.ServeHTTP(w, r)
	})
	http.Handle("/", noCacheFiles)
	http.Handle("/client/", http.StripPrefix("/client/", noCacheFiles))
	http.HandleFunc("/favicon.ico", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "./client/favicon.ico")
	})

	go cleanupSessions()

	addr := fmt.Sprintf(":%d", serverPort)
	log.Printf("Attempting to start server on %s", addr)

	// Create channel to signal if server started successfully
	serverStarted := make(chan bool, 1)

	// Create a server with graceful shutdown
	server := &http.Server{
		Addr: addr,
		Handler: basicAuthMiddleware(http.DefaultServeMux),
	}

	// Start the server in a goroutine
	go func() {
		err := server.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			log.Printf("Server failed on %s: %v", addr, err)
			serverStarted <- false
		}
	}()

	// Give the server a moment to start or fail
	select {
	case success := <-serverStarted:
		if !success {
			log.Printf("Server failed to start on %s", addr)
			return
		}
	case <-time.After(1 * time.Second):
		// No immediate error, assume it started successfully
		log.Printf("🚀 Server successfully started on %s", addr)

		// Create a simple message to display in the browser
		fmt.Printf("\n------------------------------------------------\n")
		fmt.Printf("✅ Server started! Open in your browser:\n")
		fmt.Printf("   http://localhost:%d\n", serverPort)
		fmt.Printf("------------------------------------------------\n\n")

		// Block forever (the server is running in a goroutine)
		select {}
	}
}

// Set up global proxy for all Go HTTP calls
func setGlobalProxy() {
	settingsMutex.RLock()
	enableProxy := currentSettings.EnableProxy
	proxyURL := currentSettings.ProxyURL
	settingsMutex.RUnlock()

	if !enableProxy {
		log.Println("Proxy is disabled, not setting global HTTP proxy.")
		return
	}

	proxyDialer, err := createProxyDialer(proxyURL)
	if err != nil {
		log.Printf("Warning: Could not create proxy dialer: %v", err)
		return
	}

	httpTransport, ok := http.DefaultTransport.(*http.Transport)
	if ok {
		httpTransport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			return proxyDialer.Dial(network, addr)
		}
		log.Printf("Successfully configured SOCKS5 proxy for all HTTP traffic: %s", proxyURL)
	} else {
		log.Println("⚠️ Warning: Could not override HTTP transport")
	}
}

// Handler to add a torrent using a magnet link
func addTorrentHandler(w http.ResponseWriter, r *http.Request) {
	var request struct{ Magnet string }
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		respondWithJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid request"})
		return
	}

	magnet := html.UnescapeString(request.Magnet)
	if magnet == "" {
		respondWithJSON(w, http.StatusBadRequest, map[string]string{"error": "No magnet link provided"})
		return
	}

	// handle http links like Prowlarr or Jackett
	if strings.HasPrefix(request.Magnet, "http") {
		// Use the client that bypasses proxy for Prowlarr
		httpClient := createSelectiveProxyClient()

		httpClient.CheckRedirect = func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		}

		// Make the HTTP request to follow the Prowlarr link
		req, err := http.NewRequest("GET", request.Magnet, nil)
		if err != nil {
			log.Printf("Error creating request: %v", err)
			respondWithJSON(w, http.StatusBadRequest, map[string]string{
				"error": "Invalid URL: " + err.Error(),
			})
			return
		}

		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

		// Follow the Prowlarr link
		log.Printf("Following Prowlarr URL: %s", request.Magnet)
		resp, err := httpClient.Do(req)
		if err != nil {
			log.Printf("Error following URL: %v", err)
			respondWithJSON(w, http.StatusBadRequest, map[string]string{
				"error": "Failed to download: " + err.Error(),
			})
			return
		}
		defer resp.Body.Close()

		log.Printf("Got response: %d %s", resp.StatusCode, resp.Status)

		// Check for redirects to magnet links
		if resp.StatusCode >= 300 && resp.StatusCode < 400 {
			location := resp.Header.Get("Location")
			log.Printf("Found redirect to: %s", location)

			if strings.HasPrefix(location, "magnet:") {
				log.Printf("Found magnet redirect: %s", location)
				magnet = location
			} else {
				log.Printf("Non-magnet redirect: %s", location)
				respondWithJSON(w, http.StatusBadRequest, map[string]string{
					"error": "URL redirects to non-magnet content",
				})
				return
			}
		}
	}

	// check if magnet link is valid
	if magnet == "" || !strings.HasPrefix(magnet, "magnet:") {
		respondWithJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid magnet link"})
		return
	}

	// Reuse a live session for the same infohash instead of spinning up a
	// second client and re-fetching metadata
	if m, err := metainfo.ParseMagnetUri(magnet); err == nil {
		if existing, ok := sessions.Load(m.InfoHash.HexString()); ok {
			session := existing.(*TorrentSession)
			session.LastUsed = time.Now()
			log.Printf("Reusing session: %s", m.InfoHash.HexString())
			respondWithJSON(w, http.StatusOK, map[string]string{"sessionId": m.InfoHash.HexString()})
			return
		}
	}

	// Use the simpler, more secure proxy configuration
	client, port, err := initTorrentWithProxy()
	if err != nil {
		log.Printf("Client creation error: %v", err)
		respondWithJSON(w, http.StatusInternalServerError,
			map[string]string{"error": "Failed to create client with proxy"})
		return
	}

	// if we bail out before session‑storage, make sure to release both client & port
	defer func() {
		if client != nil {
			releasePort(port)
			client.Close()
		}
	}()

	t, err := client.AddMagnet(magnet)
	if err != nil {
		respondWithJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid magnet url"})
		return
	}
	log.Printf("Torrent added: %s", t.InfoHash().HexString())

	select {
	case <-t.GotInfo():
		log.Printf("Successfully got torrent info for %s", t.InfoHash().HexString())
	case <-time.After(3 * time.Minute):
		respondWithJSON(w, http.StatusGatewayTimeout, map[string]string{"error": "Timeout getting info - proxy might be blocking BitTorrent traffic"})
		return
	}

	sessionID := t.InfoHash().HexString()
	log.Printf("Creating new session with ID: %s", sessionID)
	sessions.Store(sessionID, &TorrentSession{
		Client:     client,
		Torrent:    t,
		Port:       port,
		LastUsed:   time.Now(),
		ProbeCache: map[int]*ProbeResult{},
	})

	// Log successful storage
	log.Printf("Successfully stored session: %s", sessionID)

	// Set client to nil so it doesn't get closed by the defer function
	// since it's now stored in the sessions map
	client = nil

	respondWithJSON(w, http.StatusOK, map[string]string{"sessionId": sessionID})
}

// Torrent handler to serve torrent files and stream content
func torrentHandler(w http.ResponseWriter, r *http.Request) {
	// Log the entire URL path for debugging
	log.Printf("Torrent handler called with path: %s", r.URL.Path)

	// Extract sessionId and possibly fileIndex from the URL
	parts := strings.Split(r.URL.Path, "/")

	// Debug the path parts
	log.Printf("Path parts: %v (length: %d)", parts, len(parts))

	// The URL structure is /api/v1/torrent/[sessionId]/...
	if len(parts) < 5 { // Changed from 4 to 5
		log.Printf("Invalid path: not enough parts")
		respondWithJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid path"})
		return
	}

	// The session ID is at position 4, not 3 (because array is 0-indexed and path starts with /)
	sessionID := parts[4] // Changed from parts[3] to parts[4]

	log.Printf("Looking for session with ID: %s", sessionID)

	// Debug: Print all sessions that we have
	var sessionKeys []string
	sessions.Range(func(key, value interface{}) bool {
		keyStr, ok := key.(string)
		if ok {
			sessionKeys = append(sessionKeys, keyStr)
		}
		return true
	})
	log.Printf("Available sessions: %v", sessionKeys)

	// Get the torrent session from our sessions map
	sessionValue, ok := sessions.Load(sessionID)
	if !ok {
		log.Printf("Session not found with ID: %s", sessionID)
		respondWithJSON(w, http.StatusNotFound, map[string]string{
			"error":              "Session not found",
			"id":                 sessionID,
			"available_sessions": strings.Join(sessionKeys, ", "),
		})
		return
	}

	log.Printf("Found session with ID: %s", sessionID)
	session := sessionValue.(*TorrentSession)
	session.LastUsed = time.Now() // Update last used time

	// Files() panics if metadata never arrived
	if session.Torrent.Info() == nil {
		respondWithJSON(w, http.StatusServiceUnavailable, map[string]string{
			"error": "Torrent metadata not available yet",
		})
		return
	}

	if len(parts) > 6 && parts[5] == "probe" {
		handleProbe(w, r, session, sessionID, parts[6])
		return
	}
	if len(parts) > 6 && parts[5] == "remux" {
		handleRemux(w, r, session, sessionID, parts[6])
		return
	}

	// If there's a streaming request, handle it
	if len(parts) > 5 && parts[5] == "stream" { // Changed from parts[4] to parts[5]
		if len(parts) < 7 { // Changed from 6 to 7
			http.Error(w, "Invalid stream path", http.StatusBadRequest)
			return
		}

		fileIndexString := parts[6]
		// remove .vtt from fileIndex if it exists
		fileIndexString = strings.TrimSuffix(fileIndexString, ".vtt")

		fileIndex, err := strconv.Atoi(fileIndexString)

		if err != nil {
			http.Error(w, "Invalid file index", http.StatusBadRequest)
			return
		}

		if fileIndex < 0 || fileIndex >= len(session.Torrent.Files()) {
			http.Error(w, "File index out of range", http.StatusBadRequest)
			return
		}

		file := session.Torrent.Files()[fileIndex]

		// Set appropriate Content-Type based on file extension
		fileName := file.DisplayPath()
		extension := strings.ToLower(filepath.Ext(fileName))

		log.Printf("Streaming file: %s (type: %s)", fileName, extension)

		// video.js switches media requests to CORS mode when text tracks are attached
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Expose-Headers", "Content-Range, Accept-Ranges, Content-Length")

		switch extension {
		case ".mp4":
			w.Header().Set("Content-Type", "video/mp4")
		case ".webm":
			w.Header().Set("Content-Type", "video/webm")
		case ".mkv":
			w.Header().Set("Content-Type", "video/x-matroska")
		case ".avi":
			w.Header().Set("Content-Type", "video/x-msvideo")
		case ".srt":
			// For SRT, convert to VTT on-the-fly if requested as VTT
			if r.URL.Query().Get("format") == "vtt" {
				w.Header().Set("Content-Type", "text/vtt")

				// Read the SRT file with size limit
				reader := file.NewReader()
				defer reader.Close()
				// Wrap with limiting reader to prevent memory issues (10MB max)
				limitReader := io.LimitReader(reader, 10*1024*1024) // 10MB limit for subtitles
				srtBytes, err := io.ReadAll(limitReader)
				if err != nil {
					http.Error(w, "Failed to read subtitle file", http.StatusInternalServerError)
					return
				}

				// Convert from SRT to VTT
				vttBytes := convertSRTtoVTT(srtBytes)
				w.Write(vttBytes)
				return
			} else {
				w.Header().Set("Content-Type", "text/plain")
			}
		case ".vtt":
			w.Header().Set("Content-Type", "text/vtt")
		case ".sub":
			w.Header().Set("Content-Type", "text/plain")
		default:
			w.Header().Set("Content-Type", "application/octet-stream")
		}

		// Stream the file
		reader := file.NewReader()
		// ServeContent will close the reader when done but we need to
		// ensure it gets closed if there's a panic or other error
		defer func() {
			if closer, ok := reader.(io.Closer); ok {
				closer.Close()
			}
		}()
		// keep the session off the idle GC while this response is open
		atomic.AddInt32(&session.ActiveStreams, 1)
		defer atomic.AddInt32(&session.ActiveStreams, -1)
		http.ServeContent(w, r, fileName, time.Time{}, reader)
		return
	}

	// If we get here, just return file list
	var files []map[string]interface{}
	for i, file := range session.Torrent.Files() {
		files = append(files, map[string]interface{}{
			"index": i,
			"name":  file.DisplayPath(),
			"size":  file.Length(),
		})
	}

	respondWithJSON(w, http.StatusOK, files)
}

// Add a function to convert SRT to VTT format
func convertSRTtoVTT(srtBytes []byte) []byte {
	// a BOM after the WEBVTT header breaks cue parsing
	srtBytes = bytes.TrimPrefix(srtBytes, []byte{0xEF, 0xBB, 0xBF})
	srtContent := strings.ReplaceAll(string(srtBytes), "\r\n", "\n")

	var vtt strings.Builder
	vtt.WriteString("WEBVTT\n\n")

	for _, block := range strings.Split(srtContent, "\n\n") {
		lines := strings.Split(strings.TrimSpace(block), "\n")
		if len(lines) == 0 || lines[0] == "" {
			continue
		}
		if _, err := strconv.Atoi(strings.TrimSpace(lines[0])); err == nil {
			lines = lines[1:]
		}
		if len(lines) == 0 || !strings.Contains(lines[0], "-->") {
			continue
		}
		// SRT: 00:00:20,000 --> 00:00:24,400
		// VTT: 00:00:20.000 --> 00:00:24.400
		vtt.WriteString(strings.ReplaceAll(lines[0], ",", "."))
		vtt.WriteString("\n")
		for _, textLine := range lines[1:] {
			vtt.WriteString(textLine)
			vtt.WriteString("\n")
		}
		vtt.WriteString("\n")
	}

	return []byte(vtt.String())
}

// Helper function to respond with JSON
func respondWithJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

// Update cleanupSessions with safer reflection
func cleanupSessions() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		log.Printf("Checking for unused sessions...")
		sessions.Range(func(key, value interface{}) bool {
			session := value.(*TorrentSession)

			// never reap a session with an open stream response
			if atomic.LoadInt32(&session.ActiveStreams) > 0 {
				session.LastUsed = time.Now()
				return true
			}

			if time.Since(session.LastUsed) > 15*time.Minute {
				teardownSession(key, session)
				log.Printf("Removed unused session: %s", key)
			}
			return true
		})
		runtime.GC()
	}
}

// reads block until the header pieces arrive, hence the generous timeout
func runFFprobe(ctx context.Context, streamURL string) (*ProbeResult, error) {
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()

	args := []string{"-v", "error", "-of", "json", "-show_format", "-show_streams",
		"-probesize", "16M", "-analyzeduration", "20M"}
	if loopbackAuthHeader != "" {
		args = append(args, "-headers", "Authorization: "+loopbackAuthHeader+"\r\n")
	}
	args = append(args, streamURL)

	cmd := exec.CommandContext(ctx, ffprobePath, args...)
	var stdout bytes.Buffer
	stderr := &limitedWriter{max: 4096}
	cmd.Stdout = &stdout
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("ffprobe: %v (%s)", err, strings.TrimSpace(stderr.buf.String()))
	}

	var raw struct {
		Format struct {
			FormatName string `json:"format_name"`
			Duration   string `json:"duration"`
		} `json:"format"`
		Streams []struct {
			Index       int               `json:"index"`
			CodecType   string            `json:"codec_type"`
			CodecName   string            `json:"codec_name"`
			Profile     string            `json:"profile"`
			Channels    int               `json:"channels"`
			Tags        map[string]string `json:"tags"`
			Disposition struct {
				Default int `json:"default"`
			} `json:"disposition"`
		} `json:"streams"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &raw); err != nil {
		return nil, fmt.Errorf("ffprobe output parse: %v", err)
	}

	result := &ProbeResult{Container: raw.Format.FormatName}
	result.Duration, _ = strconv.ParseFloat(raw.Format.Duration, 64)
	for _, s := range raw.Streams {
		ps := ProbeStream{
			Index:    s.Index,
			Type:     s.CodecType,
			Codec:    s.CodecName,
			Profile:  s.Profile,
			Channels: s.Channels,
			Default:  s.Disposition.Default == 1,
		}
		if s.Tags != nil {
			ps.Language = s.Tags["language"]
			ps.Title = s.Tags["title"]
		}
		result.Streams = append(result.Streams, ps)
	}
	return result, nil
}

func handleProbe(w http.ResponseWriter, r *http.Request, session *TorrentSession, sessionID, indexStr string) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	if !ffmpegAvailable {
		respondWithJSON(w, http.StatusNotImplemented, map[string]string{"error": "ffmpeg is not installed on the server"})
		return
	}

	fileIndex, err := strconv.Atoi(indexStr)
	if err != nil || fileIndex < 0 || fileIndex >= len(session.Torrent.Files()) {
		respondWithJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid file index"})
		return
	}

	session.ProbeMu.Lock()
	cached := session.ProbeCache[fileIndex]
	session.ProbeMu.Unlock()
	if cached != nil {
		respondWithJSON(w, http.StatusOK, cached)
		return
	}

	result, err := runFFprobe(r.Context(), loopbackStreamURL(sessionID, fileIndex))
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			respondWithJSON(w, http.StatusGatewayTimeout, map[string]string{"error": "Timed out reading media metadata - the torrent may still be fetching its first pieces, retry shortly"})
			return
		}
		log.Printf("Probe failed for session %s file %d: %v", sessionID, fileIndex, err)
		respondWithJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to analyze media file"})
		return
	}

	session.ProbeMu.Lock()
	if session.ProbeCache == nil {
		session.ProbeCache = map[int]*ProbeResult{}
	}
	session.ProbeCache[fileIndex] = result
	session.ProbeMu.Unlock()

	respondWithJSON(w, http.StatusOK, result)
}

// Stream-copy remux into progressive fragmented MP4. ?audio=N picks an
// audio track, ?t=S restarts from an offset.
func handleRemux(w http.ResponseWriter, r *http.Request, session *TorrentSession, sessionID, indexStr string) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	if !ffmpegAvailable {
		respondWithJSON(w, http.StatusNotImplemented, map[string]string{"error": "ffmpeg is not installed on the server"})
		return
	}

	fileIndex, err := strconv.Atoi(indexStr)
	if err != nil || fileIndex < 0 || fileIndex >= len(session.Torrent.Files()) {
		respondWithJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid file index"})
		return
	}

	// params end up in the ffmpeg argv, numeric only
	audioStream := -1
	if a := r.URL.Query().Get("audio"); a != "" {
		audioStream, err = strconv.Atoi(a)
		if err != nil || audioStream < 0 || audioStream > 512 {
			respondWithJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid audio stream index"})
			return
		}
	}
	startSeconds := 0.0
	if t := r.URL.Query().Get("t"); t != "" {
		startSeconds, err = strconv.ParseFloat(t, 64)
		if err != nil || startSeconds < 0 || startSeconds > 360000 {
			respondWithJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid start time"})
			return
		}
	}

	select {
	case remuxSlots <- struct{}{}:
		defer func() { <-remuxSlots }()
	default:
		respondWithJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "Too many active streams - try again shortly"})
		return
	}

	atomic.AddInt32(&session.ActiveStreams, 1)
	defer atomic.AddInt32(&session.ActiveStreams, -1)

	videoCodec := ""
	session.ProbeMu.Lock()
	if probe := session.ProbeCache[fileIndex]; probe != nil {
		for _, s := range probe.Streams {
			if s.Type == "video" {
				videoCodec = s.Codec
				break
			}
		}
	}
	session.ProbeMu.Unlock()

	args := []string{"-v", "error"}
	if loopbackAuthHeader != "" {
		args = append(args, "-headers", "Authorization: "+loopbackAuthHeader+"\r\n")
	}
	if startSeconds > 0 {
		args = append(args, "-ss", strconv.FormatFloat(startSeconds, 'f', 3, 64))
	}
	args = append(args, "-i", loopbackStreamURL(sessionID, fileIndex), "-map", "0:v:0")
	if audioStream >= 0 {
		args = append(args, "-map", fmt.Sprintf("0:%d", audioStream))
	} else {
		args = append(args, "-map", "0:a:0?")
	}
	args = append(args, "-c", "copy", "-sn")
	if videoCodec == "hevc" {
		args = append(args, "-tag:v", "hvc1") // Safari rejects the default hev1 tag
	}
	args = append(args,
		"-avoid_negative_ts", "make_zero",
		"-movflags", "frag_keyframe+empty_moov+default_base_moof",
		"-f", "mp4", "pipe:1",
	)

	// client disconnect cancels the context and kills ffmpeg
	cmd := exec.CommandContext(r.Context(), ffmpegPath, args...)
	cmd.WaitDelay = 3 * time.Second
	stderr := &limitedWriter{max: 4096}
	cmd.Stderr = stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		respondWithJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to start remux"})
		return
	}
	if err := cmd.Start(); err != nil {
		respondWithJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to start ffmpeg"})
		return
	}
	log.Printf("Remux started for session %s file %d (audio=%d t=%.1f)", sessionID, fileIndex, audioStream, startSeconds)

	w.Header().Set("Content-Type", "video/mp4")
	w.Header().Set("Accept-Ranges", "none")
	w.Header().Set("Cache-Control", "no-store")
	io.Copy(w, stdout)

	if err := cmd.Wait(); err != nil && r.Context().Err() == nil {
		log.Printf("ffmpeg remux for session %s file %d exited: %v (%s)", sessionID, fileIndex, err, strings.TrimSpace(stderr.buf.String()))
	}
}

func teardownSession(key interface{}, session *TorrentSession) {
	releasePort(session.Port)
	if session.Torrent != nil {
		session.Torrent.Drop()
	}
	if session.Client != nil {
		session.Client.Close()
	}
	sessions.Delete(key)
}

func logsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	respondWithJSON(w, http.StatusOK, map[string]interface{}{"lines": logBuffer.Lines()})
}

func sessionsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	list := []map[string]interface{}{}
	sessions.Range(func(key, value interface{}) bool {
		session := value.(*TorrentSession)
		entry := map[string]interface{}{
			"id":       key,
			"name":     "(fetching metadata)",
			"size":     int64(0),
			"complete": int64(0),
			"peers":    0,
		}
		if t := session.Torrent; t != nil && t.Info() != nil {
			stats := t.Stats()
			entry["name"] = t.Name()
			entry["size"] = t.Length()
			entry["complete"] = t.BytesCompleted()
			entry["peers"] = stats.ActivePeers
		}
		list = append(list, entry)
		return true
	})
	respondWithJSON(w, http.StatusOK, list)
}

func dirSize(path string) int64 {
	var total int64
	filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err == nil && info != nil && !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	return total
}

// stops every session and wipes torrent-data; nothing else ever deletes it
func purgeCacheHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	sessions.Range(func(key, value interface{}) bool {
		teardownSession(key, value.(*TorrentSession))
		log.Printf("Purged session: %s", key)
		return true
	})

	const dataDir = "./torrent-data"
	freed := dirSize(dataDir)

	entries, err := os.ReadDir(dataDir)
	if err != nil && !os.IsNotExist(err) {
		respondWithJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to read cache directory: " + err.Error()})
		return
	}
	for _, entry := range entries {
		if err := os.RemoveAll(filepath.Join(dataDir, entry.Name())); err != nil {
			respondWithJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to delete " + entry.Name() + ": " + err.Error()})
			return
		}
	}

	log.Printf("Cache purged, freed %s", formatSize(float64(freed)))
	respondWithJSON(w, http.StatusOK, map[string]interface{}{
		"message":    "Cache purged",
		"freedBytes": freed,
		"freed":      formatSize(float64(freed)),
	})
}

// Test the proxy connection
func testProwlarrConnection(w http.ResponseWriter, r *http.Request) {
	// Add CORS headers
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

	// Handle preflight requests
	if r.Method == "OPTIONS" {
		return
	}

	var settings ProwlarrSettings
	if err := json.NewDecoder(r.Body).Decode(&settings); err != nil {
		respondWithJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid request body"})
		return
	}

	prowlarrHost := settings.ProwlarrHost
	prowlarrApiKey := settings.ProwlarrApiKey

	// The UI never sees stored keys, so an empty key means "test with the stored one"
	if prowlarrApiKey == "" {
		settingsMutex.RLock()
		prowlarrApiKey = currentSettings.ProwlarrApiKey
		settingsMutex.RUnlock()
	}

	if prowlarrHost == "" || prowlarrApiKey == "" {
		respondWithJSON(w, http.StatusBadRequest, map[string]string{"error": "Prowlarr host or API key not set"})
		return
	}

	client := createSelectiveProxyClient()
	testURL := fmt.Sprintf("%s/api/v1/system/status", prowlarrHost)

	req, err := http.NewRequest("GET", testURL, nil)
	if err != nil {
		log.Printf("Error creating request: %v", err)
		respondWithJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	req.Header.Set("X-Api-Key", prowlarrApiKey)
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Error making request to Prowlarr: %v", err)
		respondWithJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to connect to Prowlarr: " + err.Error()})
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respondWithJSON(w, resp.StatusCode, map[string]string{"error": fmt.Sprintf("Prowlarr returned status %d", resp.StatusCode)})
		return
	}

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Error reading response: %v", err)
		respondWithJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to read Prowlarr response"})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(responseBody)
}

// Search from Prowlarr
func searchFromProwlarr(w http.ResponseWriter, r *http.Request) {
	// Add CORS headers
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Prowlarr-Host, X-Api-Key")

	// Handle preflight requests
	if r.Method == "OPTIONS" {
		return
	}

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	query := r.URL.Query().Get("q")
	if query == "" {
		respondWithJSON(w, http.StatusBadRequest, map[string]string{"error": "No search query provided"})
		return
	}

	// search movies in prowlarr
	settingsMutex.RLock()
	prowlarrHost := currentSettings.ProwlarrHost
	prowlarrApiKey := currentSettings.ProwlarrApiKey
	settingsMutex.RUnlock()

	if prowlarrHost == "" || prowlarrApiKey == "" {
		http.Error(w, "Prowlarr host or API key not set", http.StatusBadRequest)
		return
	}

	// Use the client that bypasses proxy for Prowlarr
	client := createSelectiveProxyClient()

	// Prowlarr search endpoint - looking for movie torrents
	searchURL := fmt.Sprintf("%s/api/v1/search?query=%s&limit=10", prowlarrHost, url.QueryEscape(query))

	req, err := http.NewRequest("GET", searchURL, nil)
	if err != nil {
		log.Printf("Error creating request: %v", err)
		respondWithJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	req.Header.Set("X-Api-Key", prowlarrApiKey)
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Error making request to Prowlarr: %v", err)
		respondWithJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to connect to Prowlarr: " + err.Error()})
		return
	}
	defer resp.Body.Close()

	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Error reading response: %v", err)
		respondWithJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to read Prowlarr response"})
		return
	}

	if resp.StatusCode != http.StatusOK {
		respondWithJSON(w, resp.StatusCode, map[string]string{"error": fmt.Sprintf("Prowlarr returned status %d: %s", resp.StatusCode, string(body))})
		return
	}

	// Parse the JSON response and process the results
	var results []map[string]interface{}
	if err := json.Unmarshal(body, &results); err != nil {
		log.Printf("Error parsing JSON: %v", err)
		respondWithJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to parse Prowlarr response"})
		return
	}

	// Process the results to make them more usable by the frontend
	var processedResults []map[string]interface{}
	for _, result := range results {
		// Get title and download URL
		title, hasTitle := result["title"].(string)
		downloadUrl, hasDownloadUrl := result["downloadUrl"].(string)

		// Magnet URL might be present in some results
		magnetUrl, hasMagnet := result["magnetUrl"].(string)

		if !hasTitle || title == "" {
			// Skip results without titles
			continue
		}

		// We need at least one of download URL or magnet URL
		if (!hasDownloadUrl || downloadUrl == "") && (!hasMagnet || magnetUrl == "") {
			continue
		}

		// Create a simplified result object with just what we need
		processedResult := map[string]interface{}{
			"title": title,
		}

		// Prefer magnet URLs if available directly
		if hasMagnet && magnetUrl != "" {
			processedResult["magnetUrl"] = magnetUrl
			processedResult["directMagnet"] = true
		} else if hasDownloadUrl && downloadUrl != "" {
			processedResult["downloadUrl"] = downloadUrl
			processedResult["directMagnet"] = false
		}

		// Include optional fields if they exist
		if size, ok := result["size"].(float64); ok {
			processedResult["size"] = formatSize(size)
		}

		if seeders, ok := result["seeders"].(float64); ok {
			processedResult["seeders"] = seeders
		}

		if leechers, ok := result["leechers"].(float64); ok {
			processedResult["leechers"] = leechers
		}

		if indexer, ok := result["indexer"].(string); ok {
			processedResult["indexer"] = indexer
		}

		if publishDate, ok := result["publishDate"].(string); ok {
			processedResult["publishDate"] = publishDate
		}

		if category, ok := result["category"].(string); ok {
			processedResult["category"] = category
		}

		processedResults = append(processedResults, processedResult)
	}

	respondWithJSON(w, http.StatusOK, processedResults)
}

// Test Jackett Connection Handler
func testJackettConnection(w http.ResponseWriter, r *http.Request) {
	// Add CORS headers
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	// Handle preflight requests
	if r.Method == "OPTIONS" {
		return
	}

	var settings JackettSettings
	if err := json.NewDecoder(r.Body).Decode(&settings); err != nil {
		respondWithJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid request body"})
		return
	}

	jackettHost := settings.JackettHost
	jackettApiKey := settings.JackettApiKey

	// The UI never sees stored keys, so an empty key means "test with the stored one"
	if jackettApiKey == "" {
		settingsMutex.RLock()
		jackettApiKey = currentSettings.JackettApiKey
		settingsMutex.RUnlock()
	}

	if jackettHost == "" || jackettApiKey == "" {
		respondWithJSON(w, http.StatusBadRequest, map[string]string{"error": "Jackett host or API key not set"})
		return
	}

	client := createSelectiveProxyClient()
	testURL := fmt.Sprintf("%s/api/v2.0/indexers/all/results?apikey=%s", jackettHost, jackettApiKey)
	req, err := http.NewRequest("GET", testURL, nil)
	if err != nil {
		log.Printf("Error creating request: %v", err)
		respondWithJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Error making request to Jackett: %v", err)
		respondWithJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to connect to Jackett: " + err.Error()})
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respondWithJSON(w, resp.StatusCode, map[string]string{"error": fmt.Sprintf("Jackett returned status %d", resp.StatusCode)})
		return
	}
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Error reading response: %v", err)
		respondWithJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to read Jackett response"})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(responseBody)
}

// Search from Jackett
func searchFromJackett(w http.ResponseWriter, r *http.Request) {
	// Add CORS headers
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

	// Handle preflight requests
	if r.Method == "OPTIONS" {
		return
	}

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	query := r.URL.Query().Get("q")
	if query == "" {
		respondWithJSON(w, http.StatusBadRequest, map[string]string{"error": "No search query provided"})
		return
	}

	// search movies in jackett
	settingsMutex.RLock()
	jackettHost := currentSettings.JackettHost
	jackettApiKey := currentSettings.JackettApiKey
	settingsMutex.RUnlock()

	if jackettHost == "" || jackettApiKey == "" {
		http.Error(w, "Jackett host or API key not set", http.StatusBadRequest)
		return
	}

	// Use the client that bypasses proxy for Jackett
	client := createSelectiveProxyClient()

	// Jackett search endpoint - looking for movie torrents
	searchURL := fmt.Sprintf("%s/api/v2.0/indexers/all/results?Query=%s&apikey=%s", jackettHost, url.QueryEscape(query), jackettApiKey)

	req, err := http.NewRequest("GET", searchURL, nil)
	if err != nil {
		log.Printf("Error creating request: %v", err)
		respondWithJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Error making request to Jackett: %v", err)
		respondWithJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to connect to Jackett: " + err.Error()})
		return
	}
	defer resp.Body.Close()

	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Error reading response: %v", err)
		respondWithJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to read Jackett response"})
		return
	}

	if resp.StatusCode != http.StatusOK {
		respondWithJSON(w, resp.StatusCode, map[string]string{"error": fmt.Sprintf("Jackett returned status %d: %s", resp.StatusCode, string(body))})
		return
	}

	var jacketResponse struct {
		Results []map[string]interface{} `json:"Results"`
	}

	// Parse the JSON response and process the results
	if err := json.Unmarshal(body, &jacketResponse); err != nil {
		log.Printf("Error parsing JSON: %v", err)
		respondWithJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to parse Jackett response"})
		return
	}

	// Process the results to make them more usable by the frontend
	var processedResults []map[string]interface{}
	for _, result := range jacketResponse.Results {
		// Get title and download URL
		title, hasTitle := result["Title"].(string)
		downloadUrl, hasDownloadUrl := result["Link"].(string)

		// Magnet URL might be present in some results
		magnetUrl, hasMagnet := result["MagnetUri"].(string)

		if !hasTitle || title == "" {
			// Skip results without titles
			continue
		}

		// We need at least one of download URL or magnet URL
		if (!hasDownloadUrl || downloadUrl == "") && (!hasMagnet || magnetUrl == "") {
			continue
		}

		// Create a simplified result object with just what we need
		processedResult := map[string]interface{}{
			"title": title,
		}

		// Prefer magnet URLs if available directly
		if hasMagnet && magnetUrl != "" && strings.HasPrefix(magnetUrl, "magnet:") {
			processedResult["magnetUrl"] = magnetUrl
			processedResult["directMagnet"] = true
		} else if hasDownloadUrl && downloadUrl != "" {
			processedResult["downloadUrl"] = downloadUrl
			processedResult["directMagnet"] = false
		}

		// Include optional fields if they exist
		if size, ok := result["Size"].(float64); ok {
			processedResult["size"] = formatSize(size)
		}

		if seeders, ok := result["Seeders"].(float64); ok {
			processedResult["seeders"] = seeders
		}

		if leechers, ok := result["Peers"].(float64); ok {
			processedResult["leechers"] = leechers
		}

		if indexer, ok := result["Tracker"].(string); ok {
			processedResult["indexer"] = indexer
		}

		if publishDate, ok := result["PublishDate"].(string); ok {
			processedResult["publishDate"] = publishDate
		}

		if category, ok := result["category"].(string); ok {
			processedResult["category"] = category
		}

		processedResults = append(processedResults, processedResult)
	}

	respondWithJSON(w, http.StatusOK, processedResults)
}

// Test Proxy Connection Handler
func testProxyConnection(w http.ResponseWriter, r *http.Request) {
	// Add CORS headers
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

	// Handle preflight requests
	if r.Method == "OPTIONS" {
		return
	}

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var settings ProxySettings
	if err := json.NewDecoder(r.Body).Decode(&settings); err != nil {
		respondWithJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid request body"})
		return
	}

	proxyURL := settings.ProxyURL

	if proxyURL == "" {
		respondWithJSON(w, http.StatusBadRequest, map[string]string{"error": "Proxy URL not set"})
		return
	}

	// Parse the proxy URL
	parsedProxyURL, err := url.Parse(proxyURL)
	if err != nil {
		respondWithJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid proxy URL: " + err.Error()})
		return
	}

	// Create a transport that uses the proxy
	transport := &http.Transport{
		Proxy: http.ProxyURL(parsedProxyURL),
	}

	// Create client with custom transport and timeout
	client := &http.Client{
		Transport: transport,
		Timeout:   10 * time.Second, // Adjust timeout as needed
	}

	testURL := "https://httpbin.org/ip"
	req, err := http.NewRequest("GET", testURL, nil)
	if err != nil {
		log.Printf("Error creating request: %v", err)
		respondWithJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Error making request through proxy: %v", err)
		respondWithJSON(w, http.StatusInternalServerError, map[string]string{"error": "Proxy connection failed: " + err.Error()})
		return
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Error reading response: %v", err)
		respondWithJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to read proxy response"})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(responseBody)
}

// Helper function to save settings to file (assumes mutex is already locked)
func saveSettingsToFile() error {
	// Create the directory if it doesn't exist
	if err := os.MkdirAll("config", 0755); err != nil {
		log.Fatalf("Failed to create config directory: %v", err)
	}

	settingsMutex.RLock()
	snapshot := currentSettings
	settingsMutex.RUnlock()

	// 0600: the file contains API keys
	file, err := os.OpenFile("config/settings.json", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer file.Close()
	// the mode above only applies on creation
	if err := file.Chmod(0600); err != nil {
		log.Printf("Warning: could not chmod settings.json: %v", err)
	}

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(snapshot); err != nil {
		return err
	}

	return nil
}

// Proxy Settings Save Handler
func saveProxySettingsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	if r.Method == "OPTIONS" {
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var newSettings ProxySettings
	if err := json.NewDecoder(r.Body).Decode(&newSettings); err != nil {
		respondWithJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid request body"})
		return
	}

	settingsMutex.Lock()
	currentSettings.EnableProxy = newSettings.EnableProxy
	currentSettings.ProxyURL = newSettings.ProxyURL
	settingsMutex.Unlock()

	if err := saveSettingsToFile(); err != nil {
		respondWithJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to save settings: " + err.Error()})
		return
	}
	println("Proxy settings saved successfully")

	setGlobalProxy()

	respondWithJSON(w, http.StatusOK, map[string]string{"message": "Proxy settings saved successfully"})
}

// Prowlarr Settings Save Handler
func saveProwlarrSettingsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	if r.Method == "OPTIONS" {
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var newSettings ProwlarrSettings
	if err := json.NewDecoder(r.Body).Decode(&newSettings); err != nil {
		respondWithJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid request body"})
		return
	}

	settingsMutex.Lock()
	currentSettings.EnableProwlarr = newSettings.EnableProwlarr
	currentSettings.ProwlarrHost = newSettings.ProwlarrHost
	// Empty key means "keep the stored one" (the UI never sees the real key)
	if newSettings.ProwlarrApiKey != "" {
		currentSettings.ProwlarrApiKey = newSettings.ProwlarrApiKey
	}
	currentSettings.SearchTimeoutSeconds = newSettings.SearchTimeoutSeconds
	currentSettings.SkipTLSVerify = newSettings.SkipTLSVerify
	settingsMutex.Unlock()

	if err := saveSettingsToFile(); err != nil {
		respondWithJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to save settings: " + err.Error()})
		return
	}

	respondWithJSON(w, http.StatusOK, map[string]string{"message": "Prowlarr settings saved successfully"})
}

// Jackett Settings Save Handler
func saveJackettSettingsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	if r.Method == "OPTIONS" {
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var newSettings JackettSettings
	if err := json.NewDecoder(r.Body).Decode(&newSettings); err != nil {
		respondWithJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid request body"})
		return
	}

	settingsMutex.Lock()
	currentSettings.EnableJackett = newSettings.EnableJackett
	currentSettings.JackettHost = newSettings.JackettHost
	// Empty key means "keep the stored one" (the UI never sees the real key)
	if newSettings.JackettApiKey != "" {
		currentSettings.JackettApiKey = newSettings.JackettApiKey
	}
	currentSettings.SearchTimeoutSeconds = newSettings.SearchTimeoutSeconds
	currentSettings.SkipTLSVerify = newSettings.SkipTLSVerify
	settingsMutex.Unlock()

	if err := saveSettingsToFile(); err != nil {
		respondWithJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to save settings: " + err.Error()})
		return
	}

	respondWithJSON(w, http.StatusOK, map[string]string{"message": "Jackett settings saved successfully"})
}

// Convert Torrent to Magnet Handler
func convertTorrentToMagnetHandler(w http.ResponseWriter, r *http.Request) {
	// Set CORS headers
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

	if r.Method == "OPTIONS" {
		return
	}

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse multipart form with 10MB memory limit
	const maxUploadSize = 10 << 20 // 10MB
	if err := r.ParseMultipartForm(maxUploadSize); err != nil {
		respondWithJSON(w, http.StatusBadRequest, map[string]string{"error": "Failed to parse form: " + err.Error()})
		return
	}

	// Get the torrent file from the form data
	file, header, err := r.FormFile("torrent")
	if err != nil {
		respondWithJSON(w, http.StatusBadRequest, map[string]string{"error": "Missing torrent file"})
		return
	}
	defer file.Close()

	// Check file size
	if header.Size > maxUploadSize {
		respondWithJSON(w, http.StatusBadRequest, map[string]string{"error": "File too large"})
		return
	}

	// Read the torrent file content
	fileBytes, err := io.ReadAll(file)
	if err != nil {
		respondWithJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to read file"})
		return
	}

	// Parse torrent file
	mi, err := metainfo.Load(bytes.NewReader(fileBytes))
	if err != nil {
		respondWithJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid torrent file: " + err.Error()})
		return
	}

	// Get info hash
	infoHash := mi.HashInfoBytes().String()

	// Build magnet URL components
	magnet := fmt.Sprintf("magnet:?xt=urn:btih:%s", infoHash)

	// Add display name
	info, err := mi.UnmarshalInfo()
	if err == nil {
		magnet += fmt.Sprintf("&dn=%s", url.QueryEscape(info.Name))
	}

	// Add trackers
	for _, tier := range mi.AnnounceList {
		for _, tracker := range tier {
			magnet += fmt.Sprintf("&tr=%s", url.QueryEscape(tracker))
		}
	}

	respondWithJSON(w, http.StatusOK, map[string]string{
		"magnet": magnet,
	})
}
