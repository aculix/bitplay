package main

import (
	"bytes"
	"context"
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
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
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
}

type ProxySettings struct {
	EnableProxy bool   `json:"enableProxy"`
	ProxyURL    string `json:"proxyUrl"`
}

type ProwlarrSettings struct {
	EnableProwlarr bool   `json:"enableProwlarr"`
	ProwlarrHost   string `json:"prowlarrHost"`
	ProwlarrApiKey string `json:"prowlarrApiKey"`
}

type JackettSettings struct {
	EnableJackett bool   `json:"enableJackett"`
	JackettHost   string `json:"jackettHost"`
	JackettApiKey string `json:"jackettApiKey"`
}

var (
	sessions  sync.Map
	usedPorts sync.Map
	portMutex sync.Mutex
)

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

var (
	proxyTransport = &http.Transport{
		// copy your existing timeouts & DialContext logic here...
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 20 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		IdleConnTimeout:       30 * time.Second,
		MaxIdleConnsPerHost:   10,
	}
	proxyClient = &http.Client{
		Transport: proxyTransport,
		Timeout:   30 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return errors.New("too many redirects")
			}
			for k, vv := range via[0].Header {
				if _, ok := req.Header[k]; !ok {
					req.Header[k] = vv
				}
			}
			return nil
		},
	}
)

func createSelectiveProxyClient() *http.Client {
	settingsMutex.RLock()
	defer settingsMutex.RUnlock()

	if !currentSettings.EnableProxy {
		return &http.Client{Timeout: 30 * time.Second}
	}
	// Reconfigure proxyTransport’s DialContext if URL changed:
	dialer, _ := createProxyDialer(currentSettings.ProxyURL)
	proxyTransport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		return dialer.Dial(network, addr)
	}
	// Drop any old idle conns after reconfiguration:
	proxyTransport.CloseIdleConnections()

	return proxyClient
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
			defer settingsMutex.RUnlock()
			respondWithJSON(w, http.StatusOK, currentSettings)
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

	// Set up client file serving
	http.Handle("/", http.FileServer(http.Dir("./client")))
	http.HandleFunc("/client/", func(w http.ResponseWriter, r *http.Request) {
		http.StripPrefix("/client/", http.FileServer(http.Dir("./client"))).ServeHTTP(w, r)
	})
	http.HandleFunc("/favicon.ico", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "./client/favicon.ico")
	})

	go cleanupSessions()

	port := 3347

	addr := fmt.Sprintf(":%d", port)
	log.Printf("Attempting to start server on %s", addr)

	// Create channel to signal if server started successfully
	serverStarted := make(chan bool, 1)

	// Create a server with graceful shutdown
	server := &http.Server{
		Addr:    addr,
		Handler: nil, // Use the default ServeMux
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
		fmt.Printf("   http://localhost:%d\n", port)
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
	}

	sessionID := t.InfoHash().HexString()
	log.Printf("Creating new session with ID: %s", sessionID)
	sessions.Store(sessionID, &TorrentSession{
		Client:   client,
		Torrent:  t,
		Port:     port,
		LastUsed: time.Now(),
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
				w.Header().Set("Access-Control-Allow-Origin", "*") // Allow cross-origin requests

				// Read the SRT file with size limit
				reader := file.NewReader()
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
				w.Header().Set("Access-Control-Allow-Origin", "*") // Allow cross-origin requests
			}
		case ".vtt":
			w.Header().Set("Content-Type", "text/vtt")
			w.Header().Set("Access-Control-Allow-Origin", "*") // Allow cross-origin requests
		case ".sub":
			w.Header().Set("Content-Type", "text/plain")
			w.Header().Set("Access-Control-Allow-Origin", "*") // Allow cross-origin requests
		default:
			w.Header().Set("Content-Type", "application/octet-stream")
		}

		// Add CORS headers for all content
		// Stream the file
		reader := file.NewReader()
		// ServeContent will close the reader when done but we need to
		// ensure it gets closed if there's a panic or other error
		defer func() {
			if closer, ok := reader.(io.Closer); ok {
				closer.Close()
				println("Closed reader***************************************")
			}
		}()
		println("Serving content*****************************************")
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
	srtContent := string(srtBytes)

	// Add VTT header
	vttContent := "WEBVTT\n\n"

	// Convert SRT content to VTT format
	// Simple conversion - replace timestamps format
	lines := strings.Split(srtContent, "\n")

	for i := 0; i < len(lines); i++ {
		line := lines[i]

		// Skip subtitle numbers
		if _, err := strconv.Atoi(strings.TrimSpace(line)); err == nil {
			continue
		}

		// Convert timestamp lines
		if strings.Contains(line, " --> ") {
			// SRT: 00:00:20,000 --> 00:00:24,400
			// VTT: 00:00:20.000 --> 00:00:24.400
			line = strings.Replace(line, ",", ".", -1)
			vttContent += line + "\n"
		} else {
			vttContent += line + "\n"
		}
	}

	return []byte(vttContent)
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

			if time.Since(session.LastUsed) > 15*time.Minute {
				releasePort(session.Port)
				session.Torrent.Drop()
				session.Client.Close()
				sessions.Delete(key)
				log.Printf("Removed unused session: %s", key)
			}
			return true
		})
		runtime.GC()
	}
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

	file, err := os.Create("config/settings.json")
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(currentSettings); err != nil {
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

	settingsMutex.RLock()
	currentSettings.EnableProxy = newSettings.EnableProxy
	currentSettings.ProxyURL = newSettings.ProxyURL
	defer settingsMutex.RUnlock()

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

	settingsMutex.RLock()
	currentSettings.EnableProwlarr = newSettings.EnableProwlarr
	currentSettings.ProwlarrHost = newSettings.ProwlarrHost
	currentSettings.ProwlarrApiKey = newSettings.ProwlarrApiKey
	defer settingsMutex.RUnlock()

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

	settingsMutex.RLock()
	currentSettings.EnableJackett = newSettings.EnableJackett
	currentSettings.JackettHost = newSettings.JackettHost
	currentSettings.JackettApiKey = newSettings.JackettApiKey
	defer settingsMutex.RUnlock()

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
