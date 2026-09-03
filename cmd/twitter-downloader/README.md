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
   - For Twitter CDN URLs (`pbs.twimg.com`, `video-cf.twimg.com`, `video.twimg.com`): Uses **ECH (Encrypted Client Hello)** domain fronting to access `video-cf.twimg.com` directly, bypassing network restrictions
   - For moonchan upload URLs (`upload.moonchan.xyz`): Direct download
3. **ECH Technology**: The tool integrates `pkg/ech` which:
   - Encrypts the real destination (`video-cf.twimg.com`) in TLS handshake
   - Uses Cloudflare's ECH infrastructure for domain fronting
   - Presents a different SNI to middleboxes while reaching the actual Twitter CDN
   - Works exactly like `ech-proxy`'s twimg configuration

### No Additional Setup Required

The twitter-downloader has **built-in ECH support** - no need to run ech-proxy separately or configure VPN. Just download and run!

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
