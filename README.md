# Monochrome Playlist Downloader

Standalone CLI project for downloading tracks from:

- Spotify-style CSV exports
- Monochrome playlist links
- Monochrome public playlist links

It keeps the working resolver behavior from the fork:

- tries configured instances conservatively, one at a time
- rejects obvious preview-only results before accepting them
- prefers full-track `/track` responses when available
- falls back to DASH manifest handling when needed
- writes metadata, lyrics, collection artifacts, and optional ZIP output

## Requirements

- Node.js 20+
- `ffmpeg` and `ffprobe` available on `PATH`

## Usage

From this folder:

```powershell
npm run download -- --input "C:\Users\v\Downloads\liked.csv" --output "C:\Users\v\Music"
```

Or directly:

```powershell
node .\index.mjs --input "C:\Users\v\Downloads\liked.csv" --output "C:\Users\v\Music"
```

## Options

```text
--input <value>          CSV path or Monochrome playlist link
--output <dir>           Output directory root. Default: ./downloads
--api-url <url>          Override the primary API base URL
--pocketbase-url <url>   Override PocketBase URL for public playlists
--quality <token>        Default: HI_RES_LOSSLESS
--no-lyrics              Skip .lrc lyric downloads
--no-zip                 Skip ZIP archive creation
--help                   Show help
```

## Notes

- Cache is written to `.cache/monochrome-playlist-downloader-cache.json`
- Output artifacts are written into the playlist folder, including `_run-state.json`
- Logs are colorized and compacted when running in a TTY
