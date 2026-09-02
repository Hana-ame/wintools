package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// MediaItem represents a single media item from the Twitter API response
type MediaItem struct {
	URL         string `json:"url"`
	Type        string `json:"type"` // "photo", "video", etc.
	Thumbnail   string `json:"thumbnail,omitempty"`
	Description string `json:"description,omitempty"`
}

// AccountInfo represents user account information
type AccountInfo struct {
	Nick          string `json:"nick"`
	ProfileImage  string `json:"profile_image"`
}

// APIResponse represents the structure of the Twitter API response (new format)
type APIResponse struct {
	Username    string      `json:"username,omitempty"`    // Old format
	Media       []MediaItem `json:"media,omitempty"`       // Old format
	AccountInfo AccountInfo `json:"account_info,omitempty"` // New format
	Timeline    []MediaItem `json:"timeline,omitempty"`     // New format
	Error       string      `json:"error,omitempty"`
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// Get username from command line or prompt
	var username string
	if len(os.Args) > 1 {
		username = os.Args[1]
	} else {
		fmt.Print("Enter Twitter username: ")
		fmt.Scanln(&username)
	}

	username = strings.TrimPrefix(username, "@")
	username = strings.TrimSpace(username)

	if username == "" {
		log.Fatal("Username cannot be empty")
	}

	log.Printf("Fetching media list for user: %s", username)

	// Fetch media list from API
	apiURL := fmt.Sprintf("https://x.moonchan.xyz/api/twitter/%s.json.gz", username)
	mediaList, err := fetchMediaList(apiURL)
	if err != nil {
		log.Fatalf("Failed to fetch media list: %v", err)
	}

	if len(mediaList) == 0 {
		log.Println("No media found for this user")
		return
	}

	log.Printf("Found %d media items", len(mediaList))

	// Create download directory
	downloadDir := filepath.Join(".", username)
	if err := os.MkdirAll(downloadDir, 0755); err != nil {
		log.Fatalf("Failed to create download directory: %v", err)
	}

	// Download all media files
	successCount := 0
	failCount := 0

	for i, media := range mediaList {
		log.Printf("[%d/%d] Downloading: %s", i+1, len(mediaList), media.URL)

		filename := generateFilename(media.URL, media.Type, i)
		outputPath := filepath.Join(downloadDir, filename)

		if err := downloadMedia(media.URL, outputPath); err != nil {
			log.Printf("  Failed: %v", err)
			failCount++
		} else {
			log.Printf("  Saved: %s", outputPath)
			successCount++
		}

		// Small delay to avoid overwhelming the server
		time.Sleep(100 * time.Millisecond)
	}

	log.Printf("\nDownload complete!")
	log.Printf("Success: %d, Failed: %d", successCount, failCount)
	log.Printf("Files saved to: %s", downloadDir)
}

func fetchMediaList(apiURL string) ([]MediaItem, error) {
	client := &http.Client{
		Timeout: 60 * time.Second,
	}

	resp, err := client.Get(apiURL)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 500))
		return nil, fmt.Errorf("API returned status %d: %s", resp.StatusCode, string(body))
	}

	// Read response body first to check if it's gzip or plain JSON
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	// Try to decompress as gzip first
	var reader io.Reader
	gzReader, err := gzip.NewReader(bytes.NewReader(body))
	if err == nil {
		// Successfully created gzip reader, use it
		reader = gzReader
		defer gzReader.Close()
	} else {
		// Not gzip, use raw body
		reader = bytes.NewReader(body)
	}

	// Parse JSON response
	var apiResp APIResponse
	if err := json.NewDecoder(reader).Decode(&apiResp); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	if apiResp.Error != "" {
		return nil, fmt.Errorf("API error: %s", apiResp.Error)
	}

	// Support both old and new API formats
	if len(apiResp.Media) > 0 {
		return apiResp.Media, nil
	}
	if len(apiResp.Timeline) > 0 {
		return apiResp.Timeline, nil
	}

	return []MediaItem{}, nil
}

func downloadMedia(mediaURL, outputPath string) error {
	// Parse the original URL
	parsed, err := url.Parse(mediaURL)
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}

	var req *http.Request
	var client *http.Client

	// Determine how to fetch based on the source
	switch {
	case strings.Contains(parsed.Host, "pbs.twimg.com") || strings.Contains(parsed.Host, "video.twimg.com") || strings.Contains(parsed.Host, "video-cf.twimg.com"):
		// Twitter CDN - requires ECH proxy for access from restricted networks
		// Option 1: Use twimg.l.moonchan.xyz HTTP proxy (if ech-proxy is running)
		// Option 2: Direct access (may be blocked in some regions)
		
		log.Printf("  Twitter CDN detected: %s", parsed.Host)
		log.Printf("  Note: For full access, run ech-proxy locally or use a VPN")
		
		// Try direct access first
		req, err = http.NewRequest("GET", mediaURL, nil)
		if err != nil {
			return fmt.Errorf("failed to create request: %w", err)
		}
		
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
		req.Header.Set("Referer", "https://x.com")
		
		client = &http.Client{
			Timeout: 120 * time.Second,
		}
		
	case strings.Contains(parsed.Host, "upload.moonchan.xyz"):
		// Moonchan upload server - direct download (no ECH needed)
		log.Printf("  Direct download from moonchan upload server")
		
		req, err = http.NewRequest("GET", mediaURL, nil)
		if err != nil {
			return fmt.Errorf("failed to create request: %w", err)
		}
		
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
		
		client = &http.Client{
			Timeout: 120 * time.Second,
		}
		
	default:
		// Unknown source - try direct download
		log.Printf("  Direct download from: %s", parsed.Host)
		
		req, err = http.NewRequest("GET", mediaURL, nil)
		if err != nil {
			return fmt.Errorf("failed to create request: %w", err)
		}
		
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
		
		client = &http.Client{
			Timeout: 120 * time.Second,
		}
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("download failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download returned status %d", resp.StatusCode)
	}

	// Create output file
	outFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer outFile.Close()

	// Copy response body to file with progress tracking
	written, err := io.Copy(outFile, resp.Body)
	if err != nil {
		return fmt.Errorf("failed to save file: %w", err)
	}

	log.Printf("  Downloaded %d bytes (%.2f MB)", written, float64(written)/(1024*1024))
	return nil
}

func generateFilename(mediaURL, mediaType string, index int) string {
	// Extract filename from URL
	parsed, err := url.Parse(mediaURL)
	if err != nil {
		return fmt.Sprintf("media_%d.bin", index)
	}

	baseName := filepath.Base(parsed.Path)
	if baseName == "" || baseName == "." {
		baseName = fmt.Sprintf("media_%d", index)
	}

	// Clean up the filename - remove query parameters that might have leaked
	baseName = strings.Split(baseName, "?")[0]

	// Add extension based on type if not present
	ext := filepath.Ext(baseName)
	if ext == "" {
		switch mediaType {
		case "photo":
			baseName += ".jpg"
		case "video":
			baseName += ".mp4"
		default:
			baseName += ".bin"
		}
	}

	return baseName
}
