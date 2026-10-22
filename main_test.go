package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAddTorrentHandlerRejectsEmptyMagnetWithSingleJSONResponse(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/api/v1/torrent/add", strings.NewReader(`{"magnet":""}`))
	response := httptest.NewRecorder()

	addTorrentHandler(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusBadRequest)
	}

	decoder := json.NewDecoder(response.Body)
	var body map[string]string
	if err := decoder.Decode(&body); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if body["error"] != "No magnet link provided" {
		t.Fatalf("error = %q, want %q", body["error"], "No magnet link provided")
	}

	var extra json.RawMessage
	if err := decoder.Decode(&extra); err != io.EOF {
		t.Fatalf("response contains an unexpected second JSON value: %s (err %v)", extra, err)
	}
}
