#!/usr/bin/env node

import { promises as fs } from 'node:fs';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const ANSI = {
    reset: '\x1b[0m',
    bold: '\x1b[1m',
    dim: '\x1b[2m',
    red: '\x1b[31m',
    green: '\x1b[32m',
    yellow: '\x1b[33m',
    blue: '\x1b[34m',
    magenta: '\x1b[35m',
    cyan: '\x1b[36m',
    gray: '\x1b[90m',
};

const DEFAULT_API_INSTANCES = [
    'https://tidal-api.binimum.org',
    'https://api.monochrome.tf',
    'https://monochrome-api.samidy.com',
    'https://hifi.geeked.wtf',
];

const DEFAULT_POCKETBASE_URL = 'https://data.samidy.xyz';
const DEFAULT_OUTPUT_ROOT = path.resolve(process.cwd(), 'downloads');
const DEFAULT_QUALITY = 'HI_RES_LOSSLESS';
const DEFAULT_FOLDER_TEMPLATE = '{albumTitle} - {albumArtist}';
const DEFAULT_FILENAME_TEMPLATE = '{trackNumber} - {artist} - {title}';
const CACHE_PATH = path.resolve(process.cwd(), '.cache', 'monochrome-playlist-downloader-cache.json');
const TIDAL_BROWSER_CLIENT_ID = 'txNoH4kkV41MfH25';
const TIDAL_BROWSER_CLIENT_SECRET = 'dQjy0MinCEvxi1O4UmxvxWnDjt4cgHBPw8ll6nYBk98=';

installPrettyLogging();

async function main() {
    const args = parseArgs(process.argv.slice(2));

    if (args.help || !args.input) {
        printHelp();
        process.exit(args.help ? 0 : 1);
    }

    const input = path.resolve(args.input);
    const sourceValue = (await exists(input)) ? input : args.input;
    const outputRoot = path.resolve(args.output || DEFAULT_OUTPUT_ROOT);
    const apiBase = args.apiUrl || process.env.MONOCHROME_API || null;
    const pocketbaseUrl = args.pocketbaseUrl || process.env.MONOCHROME_POCKETBASE || DEFAULT_POCKETBASE_URL;
    const quality = args.quality || DEFAULT_QUALITY;
    const includeLyrics = !args['no-lyrics'];
    const createZip = !args['no-zip'];
    const cache = await PersistentCache.load(CACHE_PATH);
    let shuttingDown = false;
    let currentRunState = null;
    process.on('SIGINT', () => {
        if (shuttingDown) {
            process.exit(130);
            return;
        }
        shuttingDown = true;
        console.log('\nSIGINT received, flushing cache...');
        Promise.all([
            cache.flush().catch(() => {}),
            currentRunState?.flush().catch(() => {}),
        ])
            .catch(() => {})
            .finally(() => process.exit(130));
    });

    const client = new MonochromeClient({
        apiBase,
        apiInstances: DEFAULT_API_INSTANCES,
        pocketbaseUrl,
        quality,
        cache,
    });

    console.log(`Cache file: ${CACHE_PATH}`);
    console.log(
        `Cache stats: search=${Object.keys(cache.data.searchTrack || {}).length}, metadata=${Object.keys(cache.data.trackMetadata || {}).length}, playlists=${Object.keys(cache.data.playlists || {}).length}, publicPlaylists=${Object.keys(cache.data.publicPlaylists || {}).length}, covers=${Object.keys(cache.data.covers || {}).length}`
    );

    console.log(`Resolving source: ${sourceValue}`);
    const source = await resolveSource(sourceValue, client);
    const safeSourceName = sanitizeForFilename(source.title);
    const assemblyRoot = path.join(outputRoot, safeSourceName);

    await fs.mkdir(assemblyRoot, { recursive: true });

    console.log(`Source type: ${source.type}`);
    console.log(`Tracks discovered: ${source.tracks.length}`);
    if (source.missing?.length) {
        console.log(`Unmatched CSV rows: ${source.missing.length}`);
    }

    const downloaded = [];
    const failures = [];
    const albumCoverWrites = new Set();
    currentRunState = new RunState(assemblyRoot, source, downloaded, failures);
    await currentRunState.flush();

    for (let index = 0; index < source.tracks.length; index += 1) {
        const track = source.tracks[index];
        const displayTitle = `${getTrackArtists(track)} - ${getTrackTitle(track)}`;
        const trackLabel = `[${index + 1}/${source.tracks.length}] ${displayTitle}`;
        console.log(trackLabel);

        try {
            const hydratedTrack = await client.getTrackMetadata(track.id).catch(() => track);
            const resolvedTrack = mergeTrackMetadata(track, hydratedTrack);
            const folderName = formatTemplate(DEFAULT_FOLDER_TEMPLATE, {
                albumTitle: resolvedTrack.album?.title,
                albumArtist: resolvedTrack.album?.artist?.name || resolvedTrack.artist?.name,
            });
            const fileBase = formatTemplate(DEFAULT_FILENAME_TEMPLATE, {
                trackNumber: resolvedTrack.trackNumber,
                artist: resolvedTrack.artist?.name || resolvedTrack.artists?.[0]?.name,
                title: getTrackTitle(resolvedTrack),
            });

            const relativeAudioPath = path.posix.join(folderName, `${fileBase}.flac`);
            const absoluteAudioPath = path.join(assemblyRoot, ...relativeAudioPath.split('/'));
            await fs.mkdir(path.dirname(absoluteAudioPath), { recursive: true });

            const audioResult = await client.downloadTrackToFile(resolvedTrack.id, absoluteAudioPath);
            const finalRelativeAudioPath = path.posix.join(folderName, `${fileBase}.${audioResult.extension}`);
            let finalAbsoluteAudioPath = absoluteAudioPath;

            if (audioResult.extension !== 'flac') {
                finalAbsoluteAudioPath = absoluteAudioPath.replace(/\.flac$/i, `.${audioResult.extension}`);
                await fs.rename(absoluteAudioPath, finalAbsoluteAudioPath);
                console.warn(
                    `  Saved ${resolvedTrack.id} as .${audioResult.extension} because the upstream stream was not FLAC.`
                );
            }

            let embeddedLyrics = null;
            if (includeLyrics) {
                embeddedLyrics = await fetchLyrics(resolvedTrack);
                if (embeddedLyrics) {
                    const lrcPath = finalAbsoluteAudioPath.replace(/\.[^.]+$/u, '.lrc');
                    await fs.writeFile(lrcPath, embeddedLyrics, 'utf8');
                }
            }

            let coverBuffer = null;
            const coverId = resolvedTrack.album?.cover;
            if (coverId) {
                const coverFolder = path.dirname(finalAbsoluteAudioPath);
                const coverTarget = path.join(coverFolder, 'cover.jpg');
                coverBuffer = await client.fetchCover(coverId);
                if (coverBuffer && !albumCoverWrites.has(coverTarget)) {
                    await fs.writeFile(coverTarget, coverBuffer);
                    albumCoverWrites.add(coverTarget);
                }
            }

            await embedMetadataWithFfmpeg({
                audioPath: finalAbsoluteAudioPath,
                track: resolvedTrack,
                lyrics: embeddedLyrics,
                coverBuffer,
            });

            downloaded.push({
                ...resolvedTrack,
                filePath: finalRelativeAudioPath,
            });
            await currentRunState.flush();
        } catch (error) {
            failures.push({
                id: track.id,
                title: getTrackTitle(track),
                artist: getTrackArtists(track),
                error: error instanceof Error ? error.message : String(error),
            });
            console.warn(`  Failed: ${failures.at(-1).error}`);
            await currentRunState.flush();
        }
    }

    await writeCollectionArtifacts(assemblyRoot, source, downloaded, failures);
    await cache.flush();
    await currentRunState.flush();

    if (createZip) {
        const zipPath = `${assemblyRoot}.zip`;
        await zipFolder(assemblyRoot, zipPath);
        console.log(`ZIP archive: ${zipPath}`);
    }

    console.log(`Completed: ${downloaded.length} succeeded, ${failures.length} failed.`);
    if (failures.length) {
        process.exitCode = 2;
    }
}

function installPrettyLogging() {
    const useColor = Boolean(process.stdout.isTTY) && !process.env.NO_COLOR;
    const original = {
        log: console.log.bind(console),
        warn: console.warn.bind(console),
        error: console.error.bind(console),
    };

    const paint = (text, ...styles) => {
        if (!useColor) {
            return text;
        }
        return `${styles.join('')}${text}${ANSI.reset}`;
    };

    const shortenUrl = (value) => {
        try {
            const url = new URL(String(value));
            return `${url.host}${url.pathname}${url.search ? `?${url.searchParams.toString()}` : ''}`;
        } catch {
            return String(value);
        }
    };

    const formatLine = (line) => {
        if (typeof line !== 'string' || !line.trim()) {
            return line;
        }

        const requestMatch = line.match(/^\[request:(api|tidal)\]\s+(.*)$/u);
        if (requestMatch) {
            const [, kind, detail] = requestMatch;
            const label = paint(kind.toUpperCase().padEnd(5), ANSI.bold, kind === 'tidal' ? ANSI.magenta : ANSI.blue);

            if (/^ok$/iu.test(detail.trim())) {
                return `${label} ${paint('ok', ANSI.green)}`;
            }

            if (/^\d{3}\s/u.test(detail.trim())) {
                const color = detail.startsWith('2') ? ANSI.green : detail.startsWith('4') ? ANSI.yellow : ANSI.red;
                return `${label} ${paint(detail.trim(), color)}`;
            }

            return `${label} ${paint(shortenUrl(detail.trim()), ANSI.gray)}`;
        }

        if (/^\[\d+\/\d+\]/u.test(line)) {
            return paint(line, ANSI.bold, ANSI.cyan);
        }

        if (/^\[resolve \d+\/\d+\]/u.test(line)) {
            return paint(line, ANSI.bold, ANSI.blue);
        }

        if (line.startsWith('  -> ')) {
            const body = line.slice(5);
            if (/rejected|preview|failed|unavailable|skipping/iu.test(body)) {
                return `  ${paint('->', ANSI.yellow)} ${paint(body, ANSI.yellow)}`;
            }
            if (/matched|accepted|resolved|source|selection|duration|cache hit|cover fetch|cover cache hit/iu.test(body)) {
                return `  ${paint('->', ANSI.cyan)} ${body}`;
            }
            return `  ${paint('->', ANSI.gray)} ${paint(body, ANSI.dim)}`;
        }

        if (/^Failed:/iu.test(line.trim()) || /^  Failed:/iu.test(line)) {
            return paint(line, ANSI.red);
        }

        if (/^Completed:/iu.test(line.trim())) {
            return paint(line, ANSI.bold, ANSI.green);
        }

        if (/^Cache file:|^Cache stats:|^Resolving source:|^Source type:|^Tracks discovered:|^Unmatched CSV rows:|^ZIP archive:/iu.test(line.trim())) {
            return paint(line, ANSI.dim);
        }

        return line;
    };

    const wrap = (method) => (...args) => {
        if (args.length === 1 && typeof args[0] === 'string') {
            original[method](formatLine(args[0]));
            return;
        }

        if (args.length > 1 && typeof args[0] === 'string') {
            original[method](formatLine(args[0]), ...args.slice(1));
            return;
        }

        original[method](...args);
    };

    console.log = wrap('log');
    console.warn = wrap('warn');
    console.error = wrap('error');
}

function parseArgs(argv) {
    const args = {};

    for (let i = 0; i < argv.length; i += 1) {
        const token = argv[i];

        if (!token.startsWith('--')) {
            if (!args.input) {
                args.input = token;
            }
            continue;
        }

        const key = token.slice(2);
        const next = argv[i + 1];
        if (!next || next.startsWith('--')) {
            args[key] = true;
            continue;
        }

        args[key] = next;
        i += 1;
    }

    return args;
}

class PersistentCache {
    constructor(filePath, data) {
        this.filePath = filePath;
        this.data = data;
        this.saveTimer = null;
        this.pendingSave = null;
    }

    static async load(filePath) {
        const defaults = {
            searchTrack: {},
            trackMetadata: {},
            playlists: {},
            publicPlaylists: {},
            covers: {},
        };

        try {
            const raw = await fs.readFile(filePath, 'utf8');
            const parsed = JSON.parse(raw);
            return new PersistentCache(filePath, { ...defaults, ...parsed });
        } catch {
            await fs.mkdir(path.dirname(filePath), { recursive: true }).catch(() => {});
            return new PersistentCache(filePath, defaults);
        }
    }

    get(bucket, key) {
        return this.data?.[bucket]?.[key] ?? null;
    }

    set(bucket, key, value) {
        if (!this.data[bucket]) {
            this.data[bucket] = {};
        }
        this.data[bucket][key] = value;
        this.scheduleSave();
    }

    async save() {
        if (this.saveTimer) {
            clearTimeout(this.saveTimer);
            this.saveTimer = null;
        }
        await fs.mkdir(path.dirname(this.filePath), { recursive: true });
        await fs.writeFile(this.filePath, JSON.stringify(this.data, null, 2), 'utf8');
    }

    scheduleSave() {
        if (this.saveTimer) {
            clearTimeout(this.saveTimer);
        }

        this.saveTimer = setTimeout(() => {
            this.pendingSave = this.save().catch(() => {});
        }, 250);
    }

    async flush() {
        if (this.saveTimer) {
            clearTimeout(this.saveTimer);
            this.saveTimer = null;
        }
        if (this.pendingSave) {
            await this.pendingSave.catch(() => {});
            this.pendingSave = null;
        }
        await this.save();
    }
}

class RunState {
    constructor(rootDir, source, downloaded, failures) {
        this.rootDir = rootDir;
        this.source = source;
        this.downloaded = downloaded;
        this.failures = failures;
        this.statePath = path.join(rootDir, '_run-state.json');
    }

    async flush() {
        await fs.mkdir(this.rootDir, { recursive: true });
        await writeCollectionArtifacts(this.rootDir, this.source, this.downloaded, this.failures);
        await fs.writeFile(
            this.statePath,
            JSON.stringify(
                {
                    updated: new Date().toISOString(),
                    sourceType: this.source.type,
                    sourceTitle: this.source.title,
                    downloadedCount: this.downloaded.length,
                    failureCount: this.failures.length,
                    downloadedIds: this.downloaded.map((track) => track.id),
                    failures: this.failures,
                },
                null,
                2
            ),
            'utf8'
        );
    }
}

function printHelp() {
    console.log(`
Monochrome playlist downloader

Usage:
  node index.mjs --input <csv-or-link> [--output <dir>]

Accepted input:
  - Spotify playlist/library CSV exports with track and artist columns
  - Monochrome playlist links: /playlist/{id}
  - Monochrome public user playlist links: /userplaylist/{id}

Options:
  --input <value>          CSV path or Monochrome playlist link
  --output <dir>           Output directory root. Default: ./downloads
  --api-url <url>          Override Monochrome/HiFi API base URL
  --pocketbase-url <url>   Override PocketBase URL for public user playlists
  --quality <token>        Default: HI_RES_LOSSLESS
  --no-lyrics              Skip .lrc lyric downloads
  --no-zip                 Skip ZIP archive creation
  --help                   Show this help
`.trim());
}

async function resolveSource(input, client) {
    if (await exists(input)) {
        if (path.extname(input).toLowerCase() !== '.csv') {
            throw new Error('Only CSV files are supported for file input.');
        }

        console.log(`Reading CSV: ${input}`);
        const csvText = await fs.readFile(input, 'utf8');
        const csvResult = await parseCsvSource(csvText, client);
        return {
            type: 'csv',
            title: path.basename(input, path.extname(input)),
            tracks: csvResult.tracks,
            missing: csvResult.missing,
            metadata: {
                title: path.basename(input, path.extname(input)),
                source: input,
            },
        };
    }

    const playlistMatch = input.match(/\/playlist\/([^/?#]+)/i);
    if (playlistMatch) {
        const playlistId = playlistMatch[1];
        const playlist = await client.getPlaylist(playlistId);
        return {
            type: 'playlist',
            title: playlist.playlist.title,
            tracks: playlist.tracks,
            missing: [],
            metadata: playlist.playlist,
        };
    }

    const userPlaylistMatch = input.match(/\/userplaylist\/([^/?#]+)/i);
    if (userPlaylistMatch) {
        const playlistId = userPlaylistMatch[1];
        const playlist = await client.getPublicPlaylist(playlistId);
        return {
            type: 'user-playlist',
            title: playlist.title,
            tracks: playlist.tracks,
            missing: [],
            metadata: playlist,
        };
    }

    throw new Error('Unsupported input. Use a CSV file or a Monochrome playlist link.');
}

async function parseCsvSource(csvText, client) {
    const rows = parseCsv(csvText);
    if (rows.length <= 1) {
        return { tracks: [], missing: [] };
    }

    const headerMap = mapCsvHeaders(rows[0]);
    const tracks = [];
    const missing = [];
    console.log(`CSV rows to resolve: ${Math.max(0, rows.length - 1)}`);

    for (let index = 1; index < rows.length; index += 1) {
        const row = rows[index];
        if (!row.some((cell) => cell.trim())) {
            continue;
        }

        const trackName = getCsvValue(row, headerMap, 'track');
        const artistName = getCsvValue(row, headerMap, 'artist');
        const albumName = getCsvValue(row, headerMap, 'album');
        const isrc = getCsvValue(row, headerMap, 'isrc');
        console.log(
            `[resolve ${index}/${rows.length - 1}] track="${trackName}" artist="${artistName}" album="${albumName}" isrc="${isrc}"`
        );

        if (!trackName || !artistName) {
            console.log('  -> skipped: missing required track or artist column');
            missing.push({ title: trackName, artist: artistName, album: albumName, reason: 'Missing track or artist' });
            continue;
        }

        const found = await client.searchTrack(trackName, artistName, albumName, isrc);
        if (found) {
            console.log(`  -> matched: id=${found.id} title="${getTrackTitle(found)}" artist="${getTrackArtists(found)}"`);
            tracks.push(found);
        } else {
            console.log('  -> no match');
            missing.push({ title: trackName, artist: artistName, album: albumName, isrc, reason: 'No match' });
        }
    }

    return { tracks, missing };
}

function parseCsv(text) {
    const lines = text.replace(/^\uFEFF/u, '').split(/\r?\n/u);
    return lines.map((line) => {
        const cells = [];
        let current = '';
        let inQuotes = false;

        for (let i = 0; i < line.length; i += 1) {
            const char = line[i];
            const next = line[i + 1];

            if (char === '"' && inQuotes && next === '"') {
                current += '"';
                i += 1;
                continue;
            }

            if (char === '"') {
                inQuotes = !inQuotes;
                continue;
            }

            if (char === ',' && !inQuotes) {
                cells.push(current.trim());
                current = '';
                continue;
            }

            current += char;
        }

        cells.push(current.trim());
        return cells;
    });
}

function mapCsvHeaders(headers) {
    const aliases = {
        track: ['track name', 'title', 'song', 'name', 'track', 'track title'],
        artist: ['artist name(s)', 'artist name', 'artist', 'artists', 'creator'],
        album: ['album', 'album name'],
        isrc: ['isrc', 'isrc code'],
    };

    const normalized = headers.map((header) =>
        header
            .toLowerCase()
            .trim()
            .replace(/[_\s]+/gu, ' ')
    );

    const result = {};
    for (const [key, values] of Object.entries(aliases)) {
        result[key] = normalized.findIndex((header) => values.includes(header));
    }
    return result;
}

function getCsvValue(row, headerMap, key) {
    const index = headerMap[key];
    return index >= 0 ? (row[index] || '').trim() : '';
}

function sanitizeForFilename(value) {
    return (value || 'Unknown')
        .replace(/[\\/:*?"<>|]/gu, '_')
        .replace(/\s+/gu, ' ')
        .trim();
}

function formatTemplate(template, values) {
    const tokens = {
        trackNumber: values.trackNumber ? String(values.trackNumber).padStart(2, '0') : '00',
        artist: sanitizeForFilename(values.artist || 'Unknown Artist'),
        title: sanitizeForFilename(values.title || 'Unknown Title'),
        albumTitle: sanitizeForFilename(values.albumTitle || 'Unknown Album'),
        albumArtist: sanitizeForFilename(values.albumArtist || 'Unknown Artist'),
    };

    return template.replace(/\{([^{}]+)\}/gu, (match, key) => tokens[key] ?? match);
}

function getTrackTitle(track) {
    return track?.version ? `${track.title} (${track.version})` : track?.title || 'Unknown Title';
}

function getTrackArtists(track) {
    if (Array.isArray(track?.artists) && track.artists.length > 0) {
        return track.artists.map((artist) => artist?.name || 'Unknown Artist').join(', ');
    }
    return track?.artist?.name || 'Unknown Artist';
}

function mergeTrackMetadata(primary, fallback) {
    return {
        ...fallback,
        ...primary,
        album: {
            ...(fallback?.album || {}),
            ...(primary?.album || {}),
        },
        artist: primary?.artist || fallback?.artist,
        artists: primary?.artists || fallback?.artists,
    };
}

function normalizeSearchResponse(data, key) {
    const section = findSearchSection(data, key, new Set());
    const items = section?.items ?? [];
    return { items };
}

function findSearchSection(source, key, visited) {
    if (!source || typeof source !== 'object') {
        return null;
    }

    if (Array.isArray(source)) {
        for (const entry of source) {
            const found = findSearchSection(entry, key, visited);
            if (found) {
                return found;
            }
        }
        return null;
    }

    if (visited.has(source)) {
        return null;
    }
    visited.add(source);

    if (Array.isArray(source.items)) {
        return source;
    }

    if (key in source) {
        const found = findSearchSection(source[key], key, visited);
        if (found) {
            return found;
        }
    }

    for (const value of Object.values(source)) {
        const found = findSearchSection(value, key, visited);
        if (found) {
            return found;
        }
    }

    return null;
}

function scoreTrackMatch(item, trackName, artistName, albumName, isrc) {
    let score = 0;
    const normalizedTrack = normalizeLoose(trackName);
    const normalizedArtist = normalizeLoose(artistName);
    const normalizedAlbum = normalizeLoose(albumName);
    const itemTrack = normalizeLoose(getTrackTitle(item));
    const itemArtist = normalizeLoose(getTrackArtists(item));
    const itemAlbum = normalizeLoose(item.album?.title || '');

    if (isrc && item.isrc && String(item.isrc).toUpperCase() === String(isrc).toUpperCase()) {
        score += 1000;
    }
    if (itemTrack === normalizedTrack) {
        score += 100;
    } else if (itemTrack.includes(normalizedTrack) || normalizedTrack.includes(itemTrack)) {
        score += 50;
    }
    if (normalizedArtist && itemArtist.includes(normalizedArtist)) {
        score += 35;
    }
    if (normalizedAlbum && itemAlbum && (itemAlbum === normalizedAlbum || itemAlbum.includes(normalizedAlbum))) {
        score += 15;
    }
    return score;
}

function normalizeLoose(value) {
    return (value || '')
        .toLowerCase()
        .replace(/[^\p{L}\p{N}]+/gu, '')
        .trim();
}

function detectExtension(buffer, contentType = '') {
    const normalizedContentType = String(contentType || '').toLowerCase();

    if (normalizedContentType.includes('flac')) {
        return 'flac';
    }
    if (buffer.length >= 4 && buffer[0] === 0x66 && buffer[1] === 0x4c && buffer[2] === 0x61 && buffer[3] === 0x43) {
        return 'flac';
    }
    if (buffer.length >= 8 && buffer[4] === 0x66 && buffer[5] === 0x74 && buffer[6] === 0x79 && buffer[7] === 0x70) {
        return normalizedContentType.includes('video') ? 'mp4' : 'm4a';
    }
    if (buffer.length >= 3 && buffer[0] === 0x49 && buffer[1] === 0x44 && buffer[2] === 0x33) {
        return 'mp3';
    }
    if (normalizedContentType.includes('mpeg')) {
        return 'mp3';
    }
    if (normalizedContentType.includes('mp4')) {
        return 'm4a';
    }
    return 'flac';
}

async function embedMetadataWithFfmpeg({ audioPath, track, lyrics, coverBuffer }) {
    const parsedPath = path.parse(audioPath);
    const isMp4Family = ['.m4a', '.mp4'].includes(parsedPath.ext.toLowerCase());
    const tempOutput = path.join(parsedPath.dir, `${parsedPath.name}.tagged${isMp4Family ? '.mp4' : parsedPath.ext}`);
    const tempDir = await fs.mkdtemp(path.join(parsedPath.dir, '.meta-'));
    const cleanupPaths = [tempOutput];

    try {
        const metadataArgs = buildMetadataArgs(track, lyrics);
        const args = ['-y', '-i', audioPath];
        const shouldAttachCover = Boolean(coverBuffer) && !isMp4Family;

        if (shouldAttachCover) {
            const coverExt = detectImageExtension(coverBuffer);
            const coverPath = path.join(tempDir, `cover.${coverExt}`);
            await fs.writeFile(coverPath, coverBuffer);
            cleanupPaths.push(coverPath);
            args.push('-i', coverPath);
        }

        args.push('-map', '0:a');

        if (shouldAttachCover) {
            args.push('-map', '1');
        }

        args.push('-map_metadata', '-1', '-c:a', 'copy');

        if (isMp4Family) {
            args.push('-f', 'mp4');
        }

        if (shouldAttachCover) {
            args.push('-c:v', 'copy', '-disposition:v:0', 'attached_pic');
        }

        args.push(...metadataArgs, tempOutput);

        await runCommand('ffmpeg', args);
        await fs.rename(tempOutput, audioPath);
    } finally {
        for (const file of cleanupPaths.slice(1)) {
            await fs.rm(file, { force: true }).catch(() => {});
        }
        await fs.rm(tempOutput, { force: true }).catch(() => {});
        await fs.rm(tempDir, { recursive: true, force: true }).catch(() => {});
    }
}

function buildMetadataArgs(track, lyrics) {
    const metadata = new Map();
    const albumArtist = track?.album?.artist?.name || track?.artist?.name || getTrackArtists(track);

    metadata.set('title', getTrackTitle(track));
    metadata.set('artist', getTrackArtists(track));
    metadata.set('album', track?.album?.title || '');
    metadata.set('album_artist', albumArtist || '');
    metadata.set('track', formatNumberPair(track?.trackNumber, track?.album?.numberOfTracksOnDisc || track?.album?.numberOfTracks));
    metadata.set('disc', formatNumberPair(track?.volumeNumber || track?.discNumber, track?.album?.totalDiscs));
    metadata.set('date', normalizeReleaseDate(track?.album?.releaseDate || track?.streamStartDate));
    metadata.set('isrc', track?.isrc || '');
    metadata.set('copyright', track?.copyright || '');
    metadata.set('lyrics', lyrics || '');
    metadata.set('comment', buildComment(track));

    const args = [];
    for (const [key, value] of metadata.entries()) {
        if (value) {
            args.push('-metadata', `${key}=${sanitizeMetadataValue(value)}`);
        }
    }

    return args;
}

function formatNumberPair(number, total) {
    if (!number) {
        return '';
    }
    return total ? `${number}/${total}` : String(number);
}

function normalizeReleaseDate(value) {
    if (!value) {
        return '';
    }
    return String(value).split('T')[0];
}

function buildComment(track) {
    const payload = {
        tidalTrackId: track?.id || null,
        tidalAlbumId: track?.album?.id || null,
        audioQuality: track?.audioQuality || null,
        explicit: Boolean(track?.explicit),
    };
    return JSON.stringify(payload);
}

function sanitizeMetadataValue(value) {
    return String(value).replace(/\0/gu, '').trim();
}

function detectImageExtension(buffer) {
    if (buffer.length >= 8 && buffer[0] === 0x89 && buffer[1] === 0x50 && buffer[2] === 0x4e && buffer[3] === 0x47) {
        return 'png';
    }
    return 'jpg';
}

async function fetchLyrics(track) {
    const title = track?.title;
    const artist = getTrackArtists(track);
    const album = track?.album?.title;
    const duration = track?.duration ? Math.round(track.duration) : null;

    if (!title || !artist) {
        return null;
    }

    const params = new URLSearchParams({
        track_name: title,
        artist_name: artist,
    });
    if (album) {
        params.set('album_name', album);
    }
    if (duration) {
        params.set('duration', String(duration));
    }

    const response = await fetch(`https://lrclib.net/api/get?${params.toString()}`);
    if (!response.ok) {
        return null;
    }

    const json = await response.json();
    if (!json?.syncedLyrics) {
        return null;
    }

    return [
        `[ti:${getTrackTitle(track)}]`,
        `[ar:${artist}]`,
        `[al:${track?.album?.title || 'Unknown Album'}]`,
        `[by:LRCLIB]`,
        '',
        json.syncedLyrics,
    ].join('\n');
}

async function writeCollectionArtifacts(rootDir, source, tracks, failures) {
    const sourceName = sanitizeForFilename(source.title);
    const m3u = generateM3U(source.metadata, tracks);
    const m3u8 = generateM3U8(source.metadata, tracks);
    const cue = generateCUE(source.metadata, tracks);
    const nfo = generateNFO(source.metadata, tracks, source.type.includes('playlist') ? 'playlist' : 'playlist');
    const json = generateJSON(source.metadata, tracks, failures);

    await Promise.all([
        fs.writeFile(path.join(rootDir, `${sourceName}.m3u`), m3u, 'utf8'),
        fs.writeFile(path.join(rootDir, `${sourceName}.m3u8`), m3u8, 'utf8'),
        fs.writeFile(path.join(rootDir, `${sourceName}.cue`), cue, 'utf8'),
        fs.writeFile(path.join(rootDir, `${sourceName}.nfo`), nfo, 'utf8'),
        fs.writeFile(path.join(rootDir, `${sourceName}.json`), json, 'utf8'),
    ]);
}

function generateM3U(metadata, tracks) {
    let content = '#EXTM3U\n';
    content += `#PLAYLIST:${sanitizeForFilename(metadata?.title || 'Playlist')}\n`;
    content += `#DATE:${new Date().toISOString().slice(0, 10)}\n\n`;

    for (const track of tracks) {
        content += `#EXTINF:${Math.round(track.duration || 0)},${getTrackArtists(track)} - ${getTrackTitle(track)}\n`;
        content += `${track.filePath}\n\n`;
    }
    return content;
}

function generateM3U8(metadata, tracks) {
    let content = '#EXTM3U\n';
    content += '#EXT-X-VERSION:3\n';
    content += '#EXT-X-PLAYLIST-TYPE:VOD\n';
    content += `#PLAYLIST:${sanitizeForFilename(metadata?.title || 'Playlist')}\n`;
    content += `#DATE:${new Date().toISOString().slice(0, 10)}\n\n`;

    for (const track of tracks) {
        content += `#EXTINF:${Math.round(track.duration || 0)}.000,${getTrackArtists(track)} - ${getTrackTitle(track)}\n`;
        content += `${track.filePath}\n\n`;
    }
    content += '#EXT-X-ENDLIST\n';
    return content;
}

function generateCUE(metadata, tracks) {
    let content = `PERFORMER "${escapeCue(metadata?.artist?.name || metadata?.artist || 'Various Artists')}"\n`;
    content += `TITLE "${escapeCue(metadata?.title || 'Playlist')}"\n`;

    tracks.forEach((track, index) => {
        const trackNumber = String(track.trackNumber || index + 1).padStart(2, '0');
        const extension = path.extname(track.filePath).slice(1).toUpperCase() || 'FLAC';
        content += `FILE "${track.filePath}" ${extension}\n`;
        content += `  TRACK ${trackNumber} AUDIO\n`;
        content += `    TITLE "${escapeCue(getTrackTitle(track))}"\n`;
        content += `    PERFORMER "${escapeCue(getTrackArtists(track))}"\n`;
        content += '    INDEX 01 00:00:00\n';
    });

    return content;
}

function generateNFO(metadata, tracks, type) {
    const date = new Date().toISOString();
    let xml = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n';
    xml += type === 'album' ? '<album>\n' : '<musicplaylist>\n';
    xml += `  <title>${escapeXml(metadata?.title || 'Playlist')}</title>\n`;
    xml += `  <artist>${escapeXml(metadata?.artist?.name || metadata?.artist || 'Various Artists')}</artist>\n`;
    xml += `  <dateadded>${date}</dateadded>\n`;

    tracks.forEach((track, index) => {
        xml += '  <track>\n';
        xml += `    <position>${index + 1}</position>\n`;
        xml += `    <title>${escapeXml(getTrackTitle(track))}</title>\n`;
        xml += `    <artist>${escapeXml(getTrackArtists(track))}</artist>\n`;
        xml += `    <album>${escapeXml(track.album?.title || '')}</album>\n`;
        xml += `    <duration>${Math.round(track.duration || 0)}</duration>\n`;
        xml += `    <musicbrainztrackid>${track.id || ''}</musicbrainztrackid>\n`;
        xml += '  </track>\n';
    });

    xml += type === 'album' ? '</album>\n' : '</musicplaylist>\n';
    return xml;
}

function generateJSON(metadata, tracks, failures) {
    return JSON.stringify(
        {
            format: 'monochrome-playlist',
            version: '1.0',
            generated: new Date().toISOString(),
            playlist: {
                title: metadata?.title || 'Playlist',
                artist: metadata?.artist?.name || metadata?.artist || 'Various Artists',
                id: metadata?.id || metadata?.uuid || null,
            },
            tracks: tracks.map((track, index) => ({
                position: index + 1,
                id: track.id,
                title: getTrackTitle(track),
                artist: getTrackArtists(track),
                album: track.album?.title || null,
                albumArtist: track.album?.artist?.name || null,
                trackNumber: track.trackNumber || null,
                duration: Math.round(track.duration || 0),
                isrc: track.isrc || null,
                filePath: track.filePath,
            })),
            failures,
        },
        null,
        2
    );
}

function escapeXml(value) {
    return String(value || '')
        .replace(/&/gu, '&amp;')
        .replace(/</gu, '&lt;')
        .replace(/>/gu, '&gt;')
        .replace(/"/gu, '&quot;')
        .replace(/'/gu, '&#39;');
}

function escapeCue(value) {
    return String(value || '').replace(/"/gu, "'");
}

async function zipFolder(sourceDir, destinationZip) {
    if (process.platform !== 'win32') {
        throw new Error('ZIP creation currently uses PowerShell Compress-Archive and is only implemented for Windows.');
    }

    await fs.rm(destinationZip, { force: true }).catch(() => {});

    const sourcePath = path.resolve(sourceDir);
    const destinationPath = path.resolve(destinationZip);

    await new Promise((resolve, reject) => {
        const command = [
            '-NoProfile',
            '-Command',
            `Compress-Archive -Path '${sourcePath.replace(/'/gu, "''")}\\*' -DestinationPath '${destinationPath.replace(/'/gu, "''")}' -Force`,
        ];

        const child = spawn('powershell', command, { stdio: 'inherit' });
        child.on('exit', (code) => {
            if (code === 0) {
                resolve();
                return;
            }
            reject(new Error(`Compress-Archive failed with exit code ${code}`));
        });
        child.on('error', reject);
    });
}

async function runCommand(command, args) {
    await new Promise((resolve, reject) => {
        const child = spawn(command, args, { stdio: ['ignore', 'ignore', 'pipe'] });
        let stderr = '';

        child.stderr.on('data', (chunk) => {
            stderr += chunk.toString();
        });

        child.on('error', reject);
        child.on('exit', (code) => {
            if (code === 0) {
                resolve();
                return;
            }
            reject(new Error(`${command} failed with exit code ${code}: ${stderr.trim()}`));
        });
    });
}

async function probeMedia(input) {
    return await new Promise((resolve, reject) => {
        const child = spawn(
            'ffprobe',
            ['-v', 'error', '-show_streams', '-show_format', '-of', 'json', input],
            { stdio: ['ignore', 'pipe', 'pipe'] }
        );
        let stdout = '';
        let stderr = '';

        child.stdout.on('data', (chunk) => {
            stdout += chunk.toString();
        });
        child.stderr.on('data', (chunk) => {
            stderr += chunk.toString();
        });

        child.on('error', reject);
        child.on('exit', (code) => {
            if (code === 0) {
                try {
                    resolve(JSON.parse(stdout));
                } catch (error) {
                    reject(error);
                }
                return;
            }
            reject(new Error(`ffprobe failed with exit code ${code}: ${stderr.trim()}`));
        });
    });
}

async function probeMediaBuffer(buffer, contentType = 'audio/flac') {
    const extension = detectExtension(buffer, contentType);
    const tempDir = await fs.mkdtemp(path.join(process.cwd(), '.probe-'));
    const tempPath = path.join(tempDir, `probe.${extension}`);

    try {
        await fs.writeFile(tempPath, buffer);
        return await probeMedia(tempPath);
    } finally {
        await fs.rm(tempDir, { recursive: true, force: true }).catch(() => {});
    }
}

async function exists(target) {
    try {
        await fs.access(target);
        return true;
    } catch {
        return false;
    }
}

class MonochromeClient {
    constructor({ apiBase, apiInstances, pocketbaseUrl, quality, cache }) {
        this.apiBase = apiBase;
        this.apiInstances = apiInstances;
        this.pocketbaseUrl = pocketbaseUrl;
        this.quality = quality;
        this.cache = cache;
        this.tidalToken = null;
        this.tidalTokenExpiry = 0;
    }

    async request(relativePath, { type = 'api', raw = false } = {}) {
        const instances = this.apiBase ? [this.apiBase] : this.apiInstances;
        let lastError = null;

        for (const base of instances) {
            const url = `${base.replace(/\/$/u, '')}${relativePath}`;
            try {
                console.log(`[request:${type}] ${url}`);
                const response = await fetch(url);
                if (!response.ok) {
                    console.log(`[request:${type}] ${response.status} ${response.statusText}`);
                    lastError = new Error(`${response.status} ${response.statusText} for ${url}`);
                    continue;
                }
                console.log(`[request:${type}] ok`);
                return raw ? response : await response.json();
            } catch (error) {
                console.log(`[request:${type}] failed: ${error instanceof Error ? error.message : String(error)}`);
                lastError = error;
            }
        }

        throw lastError || new Error(`All ${type} instances failed for ${relativePath}`);
    }

    async searchTrack(trackName, artistName, albumName, isrc) {
        const cacheKey = JSON.stringify({
            trackName: trackName || '',
            artistName: artistName || '',
            albumName: albumName || '',
            isrc: isrc || '',
        });
        const cached = this.cache.get('searchTrack', cacheKey);
        if (cached) {
            console.log(`  -> cache hit: searchTrack`);
            return cached;
        }

        if (isrc) {
            console.log(`  -> searching by ISRC: ${isrc}`);
            const byIsrc = await this.request(`/search/?s=${encodeURIComponent(`isrc:${isrc}`)}`);
            const matches = normalizeSearchResponse(byIsrc.data || byIsrc, 'tracks').items;
            const exact = matches.find((item) => item.isrc && String(item.isrc).toUpperCase() === String(isrc).toUpperCase());
            if (exact) {
                this.cache.set('searchTrack', cacheKey, exact);
                return exact;
            }
        }

        const query = `"${trackName}" ${artistName}`.trim();
        console.log(`  -> searching by query: ${query}`);
        const result = await this.request(`/search/?s=${encodeURIComponent(query)}`);
        const items = normalizeSearchResponse(result.data || result, 'tracks').items;
        if (!items.length) {
            console.log('  -> API returned 0 candidates');
            return null;
        }

        console.log(`  -> API candidates: ${items.length}`);

        const ranked = [...items].sort(
            (left, right) =>
                scoreTrackMatch(right, trackName, artistName, albumName, isrc) -
                scoreTrackMatch(left, trackName, artistName, albumName, isrc)
        );
        const winner = ranked[0] || null;
        if (winner) {
            console.log(
                `  -> selected candidate: id=${winner.id} title="${getTrackTitle(winner)}" artist="${getTrackArtists(winner)}"`
            );
            this.cache.set('searchTrack', cacheKey, winner);
        }
        return winner;
    }

    async getPlaylist(id) {
        const cached = this.cache.get('playlists', String(id));
        if (cached) {
            console.log(`Playlist cache hit: ${id}`);
            return cached;
        }

        const payload = await this.request(`/playlist/?id=${encodeURIComponent(id)}`);
        const data = payload.data || payload;

        let playlist = data.playlist || null;
        let items = data.items || null;

        if (!playlist || !items) {
            const entries = Array.isArray(data) ? data : [data];
            for (const entry of entries) {
                if (!playlist && entry && typeof entry === 'object' && ('uuid' in entry || 'numberOfTracks' in entry)) {
                    playlist = entry;
                }
                if (!items && Array.isArray(entry?.items)) {
                    items = entry.items;
                }
            }
        }

        if (!playlist) {
            throw new Error(`Playlist ${id} not found`);
        }

        const tracks = (items || []).map((entry) => entry.item || entry);
        const result = { playlist, tracks };
        this.cache.set('playlists', String(id), result);
        return result;
    }

    async getPublicPlaylist(uuid) {
        const cached = this.cache.get('publicPlaylists', String(uuid));
        if (cached) {
            console.log(`Public playlist cache hit: ${uuid}`);
            return cached;
        }

        const filter = encodeURIComponent(`uuid="${uuid}"`);
        const url = `${this.pocketbaseUrl.replace(/\/$/u, '')}/api/collections/public_playlists/records?filter=${filter}&perPage=1`;
        console.log(`[request:pocketbase] ${url}`);
        const response = await fetch(url);
        if (!response.ok) {
            throw new Error(`Failed to fetch PocketBase playlist ${uuid}: ${response.status}`);
        }

        const payload = await response.json();
        const record = payload.items?.[0];
        if (!record) {
            throw new Error(`Public playlist ${uuid} not found`);
        }

        const data = safeJson(record.data, {});
        const tracks = safeJson(record.tracks, []);
        const title = record.title || record.name || record.playlist_name || data.title || data.name || uuid;

        const result = {
            ...record,
            id: uuid,
            uuid,
            title,
            name: title,
            artist: 'Community Playlist',
            tracks,
        };
        this.cache.set('publicPlaylists', String(uuid), result);
        return result;
    }

    async getTrackMetadata(id) {
        const cached = this.cache.get('trackMetadata', String(id));
        if (cached) {
            console.log(`  -> metadata cache hit: ${id}`);
            return cached;
        }

        console.log(`  -> metadata fetch: ${id}`);
        const payload = await this.request(`/info/?id=${encodeURIComponent(id)}`);
        const data = payload.data || payload;
        const list = Array.isArray(data) ? data : [data];
        const match = list.find((item) => item?.id == id || item?.item?.id == id);
        if (!match) {
            throw new Error(`Track metadata not found for ${id}`);
        }
        const result = match.item || match;
        this.cache.set('trackMetadata', String(id), result);
        return result;
    }

    async fetchCover(coverId) {
        const cached = this.cache.get('covers', String(coverId));
        if (cached) {
            console.log(`  -> cover cache hit: ${coverId}`);
            return Buffer.from(cached, 'base64');
        }

        const url = this.getCoverUrl(coverId, '1280');
        console.log(`  -> cover fetch: ${url}`);
        const response = await fetch(url);
        if (!response.ok) {
            return null;
        }
        const buffer = Buffer.from(await response.arrayBuffer());
        this.cache.set('covers', String(coverId), buffer.toString('base64'));
        return buffer;
    }

    getCoverUrl(id, size = '1280') {
        if (!id) {
            return null;
        }
        if (String(id).startsWith('http')) {
            return String(id);
        }
        const formatted = String(id).replace(/-/gu, '/');
        return `https://resources.tidal.com/images/${formatted}/${size}x${size}.jpg`;
    }

    async downloadTrackToFile(trackId, outputPath) {
        console.log(`  -> resolving stream: ${trackId}`);
        const stream = await this.resolveTrackStream(trackId);
        const buffer = await streamToBuffer(stream);
        const extension = detectExtension(buffer, stream.contentType);
        console.log(`  -> stream resolved: extension=.${extension} contentType=${stream.contentType} bytes=${buffer.length}`);
        await fs.writeFile(outputPath, buffer);
        return { extension };
    }

    async resolveTrackStream(trackId) {
        const byTrack = await this.tryTrackEndpointAcrossInstances(trackId).catch(() => null);
        if (byTrack) {
            return byTrack;
        }

        const byManifest = await this.tryTrackManifestEndpointAcrossInstances(trackId).catch(() => null);
        if (byManifest) {
            return byManifest;
        }

        const directManifest = await this.tryDirectTidalTrackManifest(trackId).catch(() => null);
        if (directManifest) {
            console.log('  -> stream source: direct TIDAL trackManifests');
            return await this.downloadFromResolvedUri(directManifest);
        }

        throw new Error('Could not resolve stream from /track or /trackManifests');
    }

    getApiBases() {
        return this.apiBase ? [this.apiBase] : this.apiInstances;
    }

    async tryTrackEndpointAcrossInstances(trackId) {
        const bases = this.getApiBases();
        let lastError = null;

        for (const base of bases) {
            console.log(`  -> stream source: /track endpoint (${base})`);
            try {
                const lookupPayload = await this.requestAgainstBase(
                    base,
                    `/track/?id=${encodeURIComponent(trackId)}&quality=${encodeURIComponent(this.quality)}`
                );
                const normalized = this.normalizeTrackLookup(lookupPayload);

                if (normalized.originalTrackUrl) {
                    console.log('  -> stream source: OriginalTrackUrl');
                    const stream = await this.downloadFromResolvedUri(normalized.originalTrackUrl);
                    if (await this.isAcceptableResolvedStream(stream)) {
                        return stream;
                    }
                    console.log('  -> stream rejected, trying next instance');
                    continue;
                }

                if (normalized.manifest) {
                    const manifestResult = this.extractFromManifest(normalized.manifest);
                    console.log('  -> stream source: decoded /track manifest');
                    const stream = await this.downloadFromResolvedUri(manifestResult);
                    if (await this.isAcceptableResolvedStream(stream)) {
                        return stream;
                    }
                    console.log('  -> stream rejected, trying next instance');
                    continue;
                }

                throw new Error('No usable stream in /track response');
            } catch (error) {
                lastError = error;
                console.log(`  -> /track failed on ${base}: ${error instanceof Error ? error.message : String(error)}`);
            }
        }

        if (lastError) {
            console.log(
                `  -> /track path unavailable, falling back to trackManifests: ${lastError instanceof Error ? lastError.message : String(lastError)}`
            );
        }
        return null;
    }

    async fetchDirectTidalToken(force = false) {
        if (!force && this.tidalToken && Date.now() < this.tidalTokenExpiry) {
            return this.tidalToken;
        }

        const params = new URLSearchParams({
            client_id: TIDAL_BROWSER_CLIENT_ID,
            client_secret: TIDAL_BROWSER_CLIENT_SECRET,
            grant_type: 'client_credentials',
        });

        const response = await fetch('https://auth.tidal.com/v1/oauth2/token', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/x-www-form-urlencoded',
                Authorization: `Basic ${Buffer.from(`${TIDAL_BROWSER_CLIENT_ID}:${TIDAL_BROWSER_CLIENT_SECRET}`).toString('base64')}`,
            },
            body: params,
        });

        if (!response.ok) {
            throw new Error(`Failed to obtain TIDAL app token: ${response.status} ${response.statusText}`);
        }

        const payload = await response.json();
        const token = payload?.access_token;
        const expiresIn = Number(payload?.expires_in || 3600);

        if (!token) {
            throw new Error('TIDAL app token response did not include access_token');
        }

        this.tidalToken = token;
        this.tidalTokenExpiry = Date.now() + Math.max(0, expiresIn - 60) * 1000;
        return token;
    }

    async tryDirectTidalTrackManifest(trackId) {
        const params = new URLSearchParams();
        params.append('adaptive', 'true');
        params.append('manifestType', 'MPEG_DASH');
        params.append('uriScheme', 'HTTPS');
        params.append('usage', 'PLAYBACK');
        params.append('formats', 'HEAACV1');
        params.append('formats', 'AACLC');
        params.append('formats', 'FLAC_HIRES');
        params.append('formats', 'FLAC');

        const url = `https://openapi.tidal.com/v2/trackManifests/${encodeURIComponent(trackId)}?${params.toString()}`;
        let token = await this.fetchDirectTidalToken(false);

        for (let attempt = 0; attempt < 2; attempt += 1) {
            console.log(`[request:tidal] ${url}`);
            const response = await fetch(url, {
                headers: {
                    authorization: `Bearer ${token}`,
                },
            });

            if (response.status === 401 && attempt === 0) {
                token = await this.fetchDirectTidalToken(true);
                continue;
            }

            if (!response.ok) {
                throw new Error(`Direct TIDAL trackManifests failed: ${response.status} ${response.statusText}`);
            }

            const payload = await response.json();
            const attributes = payload?.data?.attributes || payload?.data?.data?.attributes || null;
            const uri = attributes?.uri || null;
            const trackPresentation = attributes?.trackPresentation || 'unknown';
            const previewReason = attributes?.previewReason || null;

            console.log(
                `  -> direct TIDAL manifest: presentation=${trackPresentation}${previewReason ? ` previewReason=${previewReason}` : ''} uri=${uri ? 'present' : 'missing'}`
            );

            if (String(trackPresentation).toUpperCase() === 'PREVIEW') {
                console.log('  -> skipping direct TIDAL manifest because it is preview-only');
                return null;
            }

            if (!uri) {
                throw new Error('Direct TIDAL trackManifests response did not include uri');
            }

            return uri;
        }

        return null;
    }

    async tryTrackManifestEndpoint(trackId) {
        const params = new URLSearchParams();
        params.append('formats', 'FLAC_HIRES');
        params.append('formats', 'FLAC');
        params.append('adaptive', 'true');
        params.append('manifestType', 'MPEG_DASH');
        params.append('uriScheme', 'HTTPS');
        params.append('usage', 'PLAYBACK');

        const payload = await this.request(`/trackManifests/?id=${encodeURIComponent(trackId)}&${params.toString()}`);
        const uri = payload?.data?.data?.attributes?.uri || null;
        console.log(`  -> trackManifests uri: ${uri ? 'present' : 'missing'}`);
        return uri;
    }

    async tryTrackManifestEndpointAcrossInstances(trackId) {
        const bases = this.getApiBases();
        let lastError = null;

        for (const base of bases) {
            try {
                const params = new URLSearchParams();
                params.append('formats', 'FLAC_HIRES');
                params.append('formats', 'FLAC');
                params.append('adaptive', 'true');
                params.append('manifestType', 'MPEG_DASH');
                params.append('uriScheme', 'HTTPS');
                params.append('usage', 'PLAYBACK');

                const payload = await this.requestAgainstBase(
                    base,
                    `/trackManifests/?id=${encodeURIComponent(trackId)}&${params.toString()}`
                );
                const attributes = payload?.data?.data?.attributes || null;
                const uri = attributes?.uri || null;
                const trackPresentation = attributes?.trackPresentation || 'unknown';
                const previewReason = attributes?.previewReason || null;
                console.log(
                    `  -> trackManifests (${base}): presentation=${trackPresentation}${previewReason ? ` previewReason=${previewReason}` : ''} uri=${uri ? 'present' : 'missing'}`
                );

                if (String(trackPresentation).toUpperCase() === 'PREVIEW') {
                    console.log('  -> trackManifests candidate rejected, trying next instance');
                    continue;
                }

                if (!uri) {
                    continue;
                }

                console.log('  -> stream source: trackManifests endpoint fallback');
                return await this.downloadFromResolvedUri(uri);
            } catch (error) {
                lastError = error;
                console.log(
                    `  -> trackManifests failed on ${base}: ${error instanceof Error ? error.message : String(error)}`
                );
            }
        }

        if (lastError) {
            console.log(
                `  -> trackManifests path unavailable: ${lastError instanceof Error ? lastError.message : String(lastError)}`
            );
        }
        return null;
    }

    async requestAgainstBase(base, relativePath) {
        const url = `${String(base).replace(/\/$/u, '')}${relativePath}`;
        console.log(`[request:api] ${url}`);
        const response = await fetch(url);
        if (!response.ok) {
            console.log(`[request:api] ${response.status} ${response.statusText}`);
            throw new Error(`${response.status} ${response.statusText} for ${url}`);
        }
        console.log('[request:api] ok');
        return await response.json();
    }

    async isAcceptableResolvedStream(stream) {
        const buffer = await streamToBuffer(stream);
        const contentType = String(stream?.contentType || '').toLowerCase();

        if (contentType.includes('flac') && buffer.length >= 8 * 1024 * 1024) {
            return true;
        }

        const probe = await probeMediaBuffer(buffer, contentType).catch(() => null);
        const duration = Number(probe?.format?.duration || 0);
        if (duration >= 60) {
            return true;
        }

        return false;
    }

    normalizeTrackLookup(response) {
        const unwrapped = response?.data ?? response;
        const entries = Array.isArray(unwrapped) ? unwrapped : [unwrapped, response].filter(Boolean);
        let info = null;
        let originalTrackUrl = null;

        for (const entry of entries) {
            if (!entry || typeof entry !== 'object') {
                continue;
            }
            if (!info && entry.manifest) {
                info = entry;
            }
            if (!originalTrackUrl && typeof entry.OriginalTrackUrl === 'string') {
                originalTrackUrl = entry.OriginalTrackUrl;
            }
            if (!originalTrackUrl && typeof entry.originalTrackUrl === 'string') {
                originalTrackUrl = entry.originalTrackUrl;
            }
            if (!originalTrackUrl && typeof entry.url === 'string' && !entry.manifest) {
                originalTrackUrl = entry.url;
            }
        }

        if (!info && response?.info?.manifest) {
            info = response.info;
        }

        if (!info) {
            throw new Error('Malformed track lookup payload');
        }

        return {
            manifest: info.manifest,
            originalTrackUrl,
        };
    }

    extractFromManifest(manifest) {
        if (!manifest) {
            throw new Error('Missing track manifest');
        }

        let decoded = manifest;
        if (typeof manifest === 'string') {
            try {
                decoded = Buffer.from(manifest, 'base64').toString('utf8');
            } catch {
                decoded = manifest;
            }
        }

        if (typeof decoded === 'object' && Array.isArray(decoded.urls) && decoded.urls.length > 0) {
            return decoded.urls[0];
        }

        if (typeof decoded === 'string' && decoded.includes('<MPD')) {
            return { kind: 'mpd-text', text: decoded };
        }

        if (typeof decoded === 'string') {
            try {
                const parsed = JSON.parse(decoded);
                if (Array.isArray(parsed.urls) && parsed.urls.length > 0) {
                    return parsed.urls[0];
                }
            } catch {
                const match = decoded.match(/https?:\/\/[^\s"'<>]+/u);
                if (match) {
                    return match[0];
                }
            }
        }

        throw new Error('Could not resolve a stream URL from the track manifest');
    }

    async downloadFromResolvedUri(uri) {
        if (typeof uri === 'object' && uri.kind === 'mpd-text') {
            console.log('  -> resolved uri kind: inline DASH manifest');
            return await downloadDashManifestText(uri.text);
        }

        if (typeof uri !== 'string') {
            throw new Error('Unsupported stream URI');
        }

        if (uri.includes('.m3u8')) {
            console.log(`  -> resolved uri kind: HLS (${uri})`);
            return await downloadHls(uri);
        }

        if (uri.includes('.mpd')) {
            console.log(`  -> resolved uri kind: DASH URL (${uri})`);
            try {
                const manifestResponse = await fetch(uri);
                if (!manifestResponse.ok) {
                    throw new Error(`Failed to fetch DASH manifest: ${manifestResponse.status}`);
                }
                const manifestText = await manifestResponse.text();
                return await downloadDashManifestText(manifestText, uri);
            } catch (error) {
                console.log(`  -> manual DASH download failed, retrying via ffmpeg: ${error instanceof Error ? error.message : String(error)}`);
                return await downloadDashViaFfmpeg(uri);
            }
        }

        console.log(`  -> resolved uri kind: direct file (${uri})`);
        const response = await fetch(uri);
        if (!response.ok) {
            throw new Error(`Failed to fetch stream: ${response.status}`);
        }
        return {
            buffer: Buffer.from(await response.arrayBuffer()),
            contentType: response.headers.get('content-type') || 'audio/flac',
        };
    }
}

async function downloadHls(masterUrl) {
    const masterResponse = await fetch(masterUrl);
    if (!masterResponse.ok) {
        throw new Error(`Failed to fetch HLS manifest: ${masterResponse.status}`);
    }

    const masterText = await masterResponse.text();
    const variantUrl = resolveBestHlsVariant(masterUrl, masterText);
    const playlistResponse = await fetch(variantUrl);
    if (!playlistResponse.ok) {
        throw new Error(`Failed to fetch HLS playlist: ${playlistResponse.status}`);
    }

    const playlistText = await playlistResponse.text();
    const segmentUrls = playlistText
        .split(/\r?\n/u)
        .map((line) => line.trim())
        .filter((line) => line && !line.startsWith('#'))
        .map((line) => new URL(line, variantUrl).href);

    const chunks = [];
    for (const segmentUrl of segmentUrls) {
        const segmentResponse = await fetch(segmentUrl);
        if (!segmentResponse.ok) {
            throw new Error(`Failed to fetch HLS segment: ${segmentResponse.status}`);
        }
        chunks.push(Buffer.from(await segmentResponse.arrayBuffer()));
    }

    return {
        buffer: Buffer.concat(chunks),
        contentType: 'audio/flac',
    };
}

function resolveBestHlsVariant(masterUrl, masterText) {
    if (!masterText.includes('#EXT-X-STREAM-INF')) {
        return masterUrl;
    }

    const lines = masterText.split(/\r?\n/u);
    const variants = [];
    let currentBandwidth = 0;

    for (const line of lines) {
        const trimmed = line.trim();
        if (trimmed.startsWith('#EXT-X-STREAM-INF:')) {
            const match = trimmed.match(/BANDWIDTH=(\d+)/u);
            currentBandwidth = match ? Number(match[1]) : 0;
            continue;
        }
        if (trimmed && !trimmed.startsWith('#')) {
            variants.push({
                bandwidth: currentBandwidth,
                url: new URL(trimmed, masterUrl).href,
            });
            currentBandwidth = 0;
        }
    }

    variants.sort((left, right) => right.bandwidth - left.bandwidth);
    return variants[0]?.url || masterUrl;
}

async function downloadDashViaFfmpeg(manifestUrl) {
    const tempDir = await fs.mkdtemp(path.join(process.cwd(), '.dash-'));
    const probe = await probeMedia(manifestUrl).catch(() => null);
    const bestAudioStream = selectBestAudioStream(probe?.streams || []);
    const outputExt = inferDashOutputExtension(bestAudioStream);
    const outputPath = path.join(tempDir, `dash-output.${outputExt}`);
    const mapTarget = bestAudioStream?.index != null ? `0:${bestAudioStream.index}` : '0:a:0';

    try {
        if (bestAudioStream) {
            const variantBitrate = Number(bestAudioStream.tags?.variant_bitrate || 0);
            const bitRate = Number(bestAudioStream.bit_rate || 0);
            console.log(
                `  -> DASH stream via ffmpeg: codec=${bestAudioStream.codec_name || 'unknown'} sampleRate=${bestAudioStream.sample_rate || 'unknown'} bits=${bestAudioStream.bits_per_raw_sample || bestAudioStream.bits_per_sample || 'unknown'} bitrate=${variantBitrate || bitRate || 'unknown'}`
            );
        }

        const args = ['-y', '-i', manifestUrl, '-map', mapTarget, '-c', 'copy'];

        if (outputExt === 'flac') {
            args.push('-f', 'flac');
        } else if (outputExt === 'm4a' || outputExt === 'mp4') {
            args.push('-f', 'mp4');
        }

        args.push(outputPath);

        await runCommand('ffmpeg', args);
        const buffer = await fs.readFile(outputPath);
        return {
            buffer,
            contentType: outputExt === 'flac' ? 'audio/flac' : 'audio/mp4',
        };
    } finally {
        await fs.rm(tempDir, { recursive: true, force: true }).catch(() => {});
    }
}

function selectBestAudioStream(streams) {
    const audioStreams = streams.filter((stream) => stream.codec_type === 'audio');
    if (!audioStreams.length) {
        return null;
    }

    const score = (stream) => {
        const variantBitrate = Number(stream.tags?.variant_bitrate || 0);
        const bitRate = Number(stream.bit_rate || 0);
        const bitsPerRawSample = Number(stream.bits_per_raw_sample || 0);
        const sampleRate = Number(stream.sample_rate || 0);
        return variantBitrate || bitRate || bitsPerRawSample * sampleRate || 0;
    };

    return [...audioStreams].sort((left, right) => score(right) - score(left))[0];
}

function inferDashOutputExtension(stream) {
    const codec = String(stream?.codec_name || '').toLowerCase();
    if (codec === 'flac') {
        return 'flac';
    }
    if (codec === 'aac' || codec === 'alac') {
        return 'm4a';
    }
    return 'm4a';
}

async function downloadDashManifestText(manifestText, manifestUrl = null) {
    const manifest = parseDashManifest(manifestText, manifestUrl);
    if (isPreviewDashManifest(manifest)) {
        throw new Error(
            `Preview DASH manifest detected: duration=${formatSeconds(manifest.durationSeconds)} baseUrl=${manifest.baseUrl || 'unknown'}`
        );
    }
    const urls = generateDashSegmentUrls(manifest);

    if (!urls.length) {
        throw new Error('No DASH segment URLs were generated');
    }

    console.log(
        `  -> DASH selection: repId=${manifest.repId || 'unknown'} bandwidth=${manifest.bandwidth || 'unknown'} mimeType=${manifest.mimeType || 'unknown'} codecs=${manifest.codecs || 'unknown'} sampleRate=${manifest.audioSamplingRate || 'unknown'} segments=${Math.max(0, urls.length - 1)} duration=${formatSeconds(manifest.durationSeconds)}${manifest.durationSeconds > 0 && manifest.durationSeconds < 60 ? ' previewLikely=yes' : ''}`
    );

    const chunks = [];
    for (let index = 0; index < urls.length; index += 1) {
        const url = urls[index];
        const response = await fetch(url);

        if (!response.ok) {
            await new Promise((resolve) => setTimeout(resolve, 500));
            const retry = await fetch(url);
            if (!retry.ok) {
                throw new Error(`Failed to fetch DASH segment ${index + 1}/${urls.length}: ${retry.status}`);
            }
            chunks.push(Buffer.from(await retry.arrayBuffer()));
            continue;
        }

        chunks.push(Buffer.from(await response.arrayBuffer()));
    }

    return {
        buffer: Buffer.concat(chunks),
        contentType: inferDashMimeType(manifest),
    };
}

function parseDashManifest(manifestText, manifestUrl = null) {
    const xml = decodeXmlEntities(String(manifestText || ''));
    if (!xml.includes('<MPD')) {
        throw new Error('Invalid DASH manifest');
    }

    const mpdBaseUrl = extractFirstTagText(xml, 'BaseURL') || '';
    const periodBody = extractFirstTagInner(xml, 'Period') || xml;
    const adaptationSets = extractTagBlocks(periodBody, 'AdaptationSet').map((block) => ({
        attrs: parseXmlAttributes(block.attrs),
        body: block.body,
    }));

    if (!adaptationSets.length) {
        throw new Error('No AdaptationSet found');
    }

    adaptationSets.sort((left, right) => getMaxRepresentationBandwidth(right.body) - getMaxRepresentationBandwidth(left.body));

    let audioSet =
        adaptationSets.find((set) => String(set.attrs.mimeType || '').toLowerCase().startsWith('audio')) || adaptationSets[0];

    const representations = extractTagBlocks(audioSet.body, 'Representation')
        .map((block) => ({
            attrs: parseXmlAttributes(block.attrs),
            body: block.body,
        }))
        .sort((left, right) => Number(right.attrs.bandwidth || 0) - Number(left.attrs.bandwidth || 0));

    if (!representations.length) {
        throw new Error('No Representation found');
    }

    const representation = representations[0];
    const segmentTemplateBlock =
        extractFirstTagBlock(representation.body, 'SegmentTemplate') || extractFirstTagBlock(audioSet.body, 'SegmentTemplate');

    if (!segmentTemplateBlock) {
        throw new Error('No SegmentTemplate found');
    }

    const segmentTemplateAttrs = parseXmlAttributes(segmentTemplateBlock.attrs);
    const baseUrl =
        extractFirstTagText(representation.body, 'BaseURL') ||
        extractFirstTagText(audioSet.body, 'BaseURL') ||
        extractFirstTagText(periodBody, 'BaseURL') ||
        mpdBaseUrl ||
        deriveDashBaseUrl(manifestUrl);

    const timelineBlock = extractFirstTagBlock(segmentTemplateBlock.body, 'SegmentTimeline');
    const startNumber = Number(segmentTemplateAttrs.startNumber || 1);
    const segments = [];

    if (timelineBlock) {
        let currentTime = 0;
        let currentNumber = startNumber;
        const segmentEntries = extractSelfClosingOrOpenTags(timelineBlock.body, 'S');

        for (const entry of segmentEntries) {
            const attrs = parseXmlAttributes(entry.attrs);
            const duration = Number(attrs.d || 0);
            const repeat = Number(attrs.r || 0);

            if (attrs.t != null) {
                currentTime = Number(attrs.t);
            }

            segments.push({ number: currentNumber, time: currentTime });
            currentTime += duration;
            currentNumber += 1;

            for (let index = 0; index < repeat; index += 1) {
                segments.push({ number: currentNumber, time: currentTime });
                currentTime += duration;
                currentNumber += 1;
            }
        }
    }

    return {
        baseUrl,
        initialization: segmentTemplateAttrs.initialization || null,
        media: segmentTemplateAttrs.media || null,
        segments,
        repId: representation.attrs.id || null,
        mimeType: audioSet.attrs.mimeType || representation.attrs.mimeType || null,
        bandwidth: Number(representation.attrs.bandwidth || 0),
        codecs: representation.attrs.codecs || audioSet.attrs.codecs || null,
        audioSamplingRate: representation.attrs.audioSamplingRate || null,
        durationSeconds: computeDashDurationSeconds(segments, segmentTemplateBlock.body, segmentTemplateAttrs.timescale),
    };
}

function generateDashSegmentUrls(manifest) {
    const urls = [];
    const { baseUrl, initialization, media, segments, repId } = manifest;

    const resolveTemplate = (template, number, time) =>
        template
            .replace(/\$RepresentationID\$/gu, repId || '')
            .replace(/\$Number(?:%0([0-9]+)d)?\$/gu, (_match, width) =>
                width ? String(number).padStart(Number(width), '0') : String(number)
            )
            .replace(/\$Time(?:%0([0-9]+)d)?\$/gu, (_match, width) =>
                width ? String(time).padStart(Number(width), '0') : String(time)
            );

    if (initialization) {
        urls.push(resolveDashUrl(baseUrl, resolveTemplate(initialization, 0, 0)));
    }

    if (media && segments.length) {
        for (const segment of segments) {
            urls.push(resolveDashUrl(baseUrl, resolveTemplate(media, segment.number, segment.time)));
        }
    }

    return urls;
}

function inferDashMimeType(manifest) {
    const mimeType = String(manifest?.mimeType || '').toLowerCase();
    const codecs = String(manifest?.codecs || '').toLowerCase();

    if (mimeType.includes('flac') || codecs.includes('flac')) {
        return 'audio/flac';
    }
    if (mimeType.includes('mp4') || codecs.includes('mp4a') || codecs.includes('aac') || codecs.includes('alac')) {
        return 'audio/mp4';
    }
    return 'audio/flac';
}

function resolveDashUrl(base, part) {
    if (!base) {
        return part;
    }
    if (/^https?:\/\//iu.test(part)) {
        return part;
    }
    return new URL(part, base.endsWith('/') ? base : `${base}/`).href;
}

function deriveDashBaseUrl(manifestUrl) {
    if (!manifestUrl) {
        return '';
    }
    try {
        return new URL('.', manifestUrl).href;
    } catch {
        return '';
    }
}

function decodeXmlEntities(value) {
    return String(value)
        .replace(/&amp;/gu, '&')
        .replace(/&quot;/gu, '"')
        .replace(/&apos;/gu, "'")
        .replace(/&lt;/gu, '<')
        .replace(/&gt;/gu, '>');
}

function parseXmlAttributes(attrs) {
    const result = {};
    const pattern = /([A-Za-z_:][A-Za-z0-9_.:-]*)="([^"]*)"/gu;
    let match = pattern.exec(attrs);

    while (match) {
        result[match[1]] = decodeXmlEntities(match[2]);
        match = pattern.exec(attrs);
    }

    return result;
}

function extractTagBlocks(xml, tagName) {
    const pattern = new RegExp(`<${tagName}\\b([^>]*)>([\\s\\S]*?)</${tagName}>`, 'gu');
    const blocks = [];
    let match = pattern.exec(xml);

    while (match) {
        blocks.push({ attrs: match[1] || '', body: match[2] || '' });
        match = pattern.exec(xml);
    }

    return blocks;
}

function extractFirstTagBlock(xml, tagName) {
    return extractTagBlocks(xml, tagName)[0] || null;
}

function extractFirstTagInner(xml, tagName) {
    return extractFirstTagBlock(xml, tagName)?.body || null;
}

function extractFirstTagText(xml, tagName) {
    const block = extractFirstTagBlock(xml, tagName);
    return block ? decodeXmlEntities(block.body.trim()) : null;
}

function extractSelfClosingOrOpenTags(xml, tagName) {
    const pattern = new RegExp(`<${tagName}\\b([^>/]*?)(?:/?>)`, 'gu');
    const tags = [];
    let match = pattern.exec(xml);

    while (match) {
        tags.push({ attrs: match[1] || '' });
        match = pattern.exec(xml);
    }

    return tags;
}

function getMaxRepresentationBandwidth(xml) {
    const representations = extractTagBlocks(xml, 'Representation');
    if (!representations.length) {
        return 0;
    }
    return Math.max(...representations.map((block) => Number(parseXmlAttributes(block.attrs).bandwidth || 0)));
}

function computeDashDurationSeconds(segments, segmentTimelineXml, timescaleValue) {
    const timescale = Number(timescaleValue || 1);
    if (!segments.length || !timescale) {
        return 0;
    }

    const segmentEntries = extractSelfClosingOrOpenTags(segmentTimelineXml, 'S').map((entry) => parseXmlAttributes(entry.attrs));
    let totalUnits = 0;

    for (const entry of segmentEntries) {
        const duration = Number(entry.d || 0);
        const repeat = Number(entry.r || 0);
        totalUnits += duration * (repeat + 1);
    }

    return totalUnits / timescale;
}

function formatSeconds(seconds) {
    if (!seconds || !Number.isFinite(seconds)) {
        return 'unknown';
    }
    return `${seconds.toFixed(2)}s`;
}

function isPreviewDashManifest(manifest) {
    const repId = String(manifest?.repId || '').toLowerCase();
    const likelyPreviewDuration = Number(manifest?.durationSeconds || 0) > 0 && Number(manifest.durationSeconds) <= 35;
    const likelyPreviewRep = repId.includes('preview');

    return likelyPreviewRep || likelyPreviewDuration;
}

function safeJson(value, fallback) {
    if (!value) {
        return fallback;
    }
    if (typeof value !== 'string') {
        return value;
    }
    try {
        return JSON.parse(value);
    } catch {
        return fallback;
    }
}

async function streamToBuffer(result) {
    if (Buffer.isBuffer(result?.buffer)) {
        return result.buffer;
    }
    throw new Error('Expected a buffer result from stream download');
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
    main().catch((error) => {
        console.error(error instanceof Error ? error.message : error);
        process.exit(1);
    });
}
