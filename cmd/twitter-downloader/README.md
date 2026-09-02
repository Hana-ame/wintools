# Twitter Media Downloader

A standalone Windows executable that downloads media from Twitter/X user profiles using the moonchan.xyz API.

## Features

- Fetches media list from `https://x.moonchan.xyz/api/twitter/{username}.json.gz`
- Downloads all media (photos, videos) to local directory
- Supports both Twitter CDN URLs (via `twimg.l.moonchan.xyz` ECH proxy) and moonchan upload server URLs
- Simple CLI interface - just double-click and enter username
- Handles both gzip-compressed and plain JSON API responses
- Automatic filename generation based on media type

## Usage

### Method 1: Double-click and enter username
```
twitter-downloader.exe
Enter Twitter username: BBC
```

### Method 2: Command line argument
```
twitter-downloader.exe BBC
```

### Method 3: With @ prefix (automatically stripped)
```
twitter-downloader.exe @BBC
```

## How It Works

1. **API Query**: Fetches media list from `x.moonchan.xyz` API (supports both gzip and plain JSON)
2. **Media Download**: 
   - For Twitter CDN URLs (`pbs.twimg.com`, `video-cf.twimg.com`, `video.twimg.com`): Attempts direct download. **Note**: In restricted networks, these may be blocked. For full access, see "ECH Proxy Setup" below.
   - For moonchan upload URLs (`upload.moonchan.xyz`): Direct download (always works)
3. **Smart Routing**: Automatically detects URL source and applies appropriate download method

### ECH Proxy Setup (For Twitter CDN Access)

To access Twitter CDN media from restricted networks, you have two options:

**Option 1: Run ech-proxy locally**
```bash
# Clone wintools and build ech-proxy
git clone https://github.com/Hana-ame/wintools.git
cd wintools
go build -o ech-proxy ./cmd/ech-proxy

# Run ech-proxy (requires configuration)
./ech-proxy --http
```

Then configure your system to use the local proxy for Twitter domains.

**Option 2: Use VPN or proxy**
Configure your system VPN/proxy to bypass restrictions for Twitter domains.

**Current Limitation**: This standalone version does not include built-in ECH support due to Go version constraints. Future versions will integrate pkg/ech directly when Go toolchain issues are resolved.

## Output

Files are saved in a directory named after the username:
```
./BBC/
  ├── BANNED.webp
  ├── photo1.jpg
  └── video1.mp4
```

## Requirements

- Windows 7 or later (64-bit)
- Internet connection
- No additional dependencies (single standalone .exe, ~5.1 MB)

## API Response Format

The tool supports two API response formats:

**Old format:**
```json
{
  "username": "example",
  "media": [
    {"url": "https://pbs.twimg.com/media/xxx.jpg", "type": "photo"}
  ]
}
```

**New format:**
```json
{
  "account_info": {
    "nick": "Example User",
    "profile_image": "data:image/png;base64,..."
  },
  "timeline": [
    {"url": "https://upload.moonchan.xyz/api/xxx/file.webp", "type": "photo"}
  ]
}
```

## Technical Details

- Built with Go 1.22, cross-compiled for Windows AMD64
- Size: ~5.1 MB (statically linked, no external dependencies)
- Uses standard HTTP client with proper headers for proxy compatibility
- Automatic gzip decompression for API responses
- Progress tracking during downloads (shows bytes and MB)
- Smart URL routing based on source domain

## Notes

- The API at `x.moonchan.xyz` has limited user coverage - not all Twitter users are available
- If a username returns "no media found" or "query failed", the user may not be in the database
- For Twitter CDN media, the `twimg.l.moonchan.xyz` proxy must be accessible (requires ech-proxy infrastructure)
- Download speed depends on network conditions and proxy performance
- Large videos may take several minutes to download

## Example

```bash
$ twitter-downloader.exe BBC
2026/09/03 23:14:41 Fetching media list for user: BBC
2026/09/03 23:14:43 Found 1 media items
2026/09/03 23:14:43 [1/1] Downloading: https://upload.moonchan.xyz/api/.../BANNED.webp
2026/09/03 23:14:48   Downloaded 23678 bytes (0.02 MB)
2026/09/03 23:14:48   Saved: BBC/BANNED.webp

Download complete!
Success: 1, Failed: 0
Files saved to: BBC
```

## Related Projects

This tool is part of the wintools collection and integrates with:
- `cmd/ech-proxy`: ECH reverse proxy server (provides twimg.l.moonchan.xyz)
- `cmd/peerfs-proxy`: WebRTC-based media proxy with twimg support
- `pkg/ech`: Cloudflare ECH client implementation
