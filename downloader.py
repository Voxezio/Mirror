# BotQuerify / downloader.py — High-Performance Multi-Stream Downloader & Remuxer
import os
import re
import sys
import asyncio
import logging
import shutil
import urllib.parse
from pathlib import Path
from typing import Optional, Dict, List, Callable, Tuple
import time
import aiofiles
import httpx

logger = logging.getLogger("botquerify.downloader")

FFMPEG_BIN = os.getenv("FFMPEG_PATH") or shutil.which("ffmpeg") or "/usr/bin/ffmpeg"
FFPROBE_BIN = os.getenv("FFPROBE_PATH") or shutil.which("ffprobe") or "/usr/bin/ffprobe"

class DownloadProgress:
    def __init__(self, total_bytes: int = 0, total_segments: int = 0, title: str = ""):
        self.total_bytes = total_bytes
        self.downloaded_bytes = 0
        self.total_segments = total_segments
        self.downloaded_segments = 0
        self.title = title
        self.mode = "init"
        self._last_callback = 0.0

    def update_bytes(self, chunk_len: int):
        self.downloaded_bytes += chunk_len

    def update_segment(self, bytes_len: int = 0):
        self.downloaded_segments += 1
        if bytes_len:
            self.downloaded_bytes += bytes_len

    def get_progress_pct(self) -> float:
        if self.mode == "hls" and self.total_segments > 0:
            return (self.downloaded_segments / self.total_segments) * 100.0
        if self.total_bytes > 0:
            return min(100.0, (self.downloaded_bytes / self.total_bytes) * 100.0)
        return 0.0

    def format_status(self) -> str:
        pct = self.get_progress_pct()
        if self.mode == "hls":
            mb = self.downloaded_bytes / (1024 * 1024)
            return f"HLS [{self.downloaded_segments}/{self.total_segments} chunks] {pct:.1f}% ({mb:.1f} MB)"
        else:
            cur_mb = self.downloaded_bytes / (1024 * 1024)
            tot_mb = self.total_bytes / (1024 * 1024) if self.total_bytes else 0
            if tot_mb > 0:
                return f"Range [{cur_mb:.1f}/{tot_mb:.1f} MB] {pct:.1f}%"
            return f"Stream [{cur_mb:.1f} MB]"


async def get_video_duration(file_path: Path) -> float:
    """Get video duration in seconds via ffprobe."""
    cmd = [
        FFPROBE_BIN,
        "-v", "error",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        str(file_path),
    ]
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await proc.communicate()
        if proc.returncode == 0:
            return float(stdout.decode().strip())
    except Exception as e:
        logger.warning(f"Failed to probe duration for {file_path.name}: {e}")
    return 1400.0


async def compress_to_target_size(input_path: Path, output_path: Path, target_mb: float = 48.5) -> bool:
    """Compress video with FFmpeg to fit strictly under target_mb (bypassing Telegram 50MB Bot API limit)."""
    if not Path(FFMPEG_BIN).exists():
        logger.error(f"FFmpeg binary not found at {FFMPEG_BIN}")
        return False

    duration = await get_video_duration(input_path)
    if duration <= 0:
        duration = 1400.0

    target_bits = target_mb * 8 * 1024 * 1024
    total_bitrate = int(target_bits / duration)
    audio_bitrate = min(96_000, max(48_000, int(total_bitrate * 0.15)))
    video_bitrate = total_bitrate - audio_bitrate

    if video_bitrate < 160_000:
        video_bitrate = 160_000

    logger.info(
        f"Compressing {input_path.name} ({input_path.stat().st_size / (1024 * 1024):.1f} MB, {duration:.0f}s) "
        f"to target {target_mb} MB: video_bitrate={video_bitrate // 1000}k, audio_bitrate={audio_bitrate // 1000}k"
    )

    cmd = [
        FFMPEG_BIN,
        "-y",
        "-i", str(input_path),
        "-c:v", "libx264",
        "-preset", "faster",
        "-b:v", f"{video_bitrate}",
        "-maxrate", f"{int(video_bitrate * 1.25)}",
        "-bufsize", f"{int(video_bitrate * 2)}",
        "-vf", "scale=-2:'min(720,ih)'",
        "-c:a", "aac",
        "-b:a", f"{audio_bitrate}",
        "-movflags", "+faststart",
        str(output_path),
    ]

    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await proc.communicate()

    if proc.returncode != 0:
        logger.error(f"FFmpeg compression failed: {stderr.decode(errors='replace')[-300:]}")
        return False

    if output_path.exists():
        out_mb = output_path.stat().st_size / (1024 * 1024)
        logger.info(f"Compressed successfully: {output_path.name} ({out_mb:.2f} MB)")
        return True
    return False


async def remux_video(input_path: Path, output_path: Path) -> bool:
    """
    Remux video using FFmpeg without re-encoding (-c copy)
    and move metadata to the front (-movflags +faststart) for instant Telegram streaming.
    """
    if not Path(FFMPEG_BIN).exists():
        logger.warning(f"FFmpeg binary not found at {FFMPEG_BIN}. Skipping remux.")
        if input_path != output_path:
            shutil.copyfile(input_path, output_path)
        return True

    cmd = [
        FFMPEG_BIN,
        "-y",
        "-i", str(input_path),
        "-c", "copy",
        "-movflags", "+faststart",
        str(output_path),
    ]

    logger.info(f"Remuxing video with FFmpeg: {' '.join(cmd)}")
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()

    if proc.returncode != 0:
        err_msg = stderr.decode(errors="replace")[-500:]
        logger.warning(f"FFmpeg remux returned error (code {proc.returncode}): {err_msg}")
        # If remux failed, copy raw file if different
        if input_path != output_path:
            shutil.copyfile(input_path, output_path)
        return False

    return True


async def download_video(
    url: str,
    headers: Optional[Dict[str, str]] = None,
    dest_path: Optional[Path] = None,
    num_streams: int = 6,
    progress_callback: Optional[Callable[[DownloadProgress], None]] = None,
    proxy: Optional[str] = None,
) -> Tuple[Path, int]:
    """
    High-performance downloader:
    - If HLS (.m3u8), downloads segments in 6 concurrent streams and assembles via FFmpeg.
    - If HTTP file, splits into 6 concurrent Range chunks and assembles.
    - Always applies FFmpeg -movflags +faststart for native Telegram streaming.
    """
    if not dest_path:
        dest_path = Path("/tmp/botquerify_videos") / f"download_{os.getpid()}_{id(url)}.mp4"

    dest_path.parent.mkdir(parents=True, exist_ok=True)
    temp_dir = dest_path.parent / f"tmp_{dest_path.stem}"
    temp_dir.mkdir(parents=True, exist_ok=True)

    progress = DownloadProgress()
    req_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        **(headers or {}),
    }

    is_hls = ".m3u8" in url.lower() or "application/x-mpegurl" in req_headers.get("Accept", "").lower()

    try:
        if is_hls:
            progress.mode = "hls"
            raw_path = await _download_hls(
                url, req_headers, temp_dir, num_streams, progress, progress_callback, proxy
            )
        else:
            progress.mode = "range"
            raw_path = await _download_http_multichunk(
                url, req_headers, temp_dir, num_streams, progress, progress_callback, proxy
            )

        # Faststart remux
        logger.info(f"Applying FFmpeg faststart to {raw_path.name} -> {dest_path.name}...")
        await remux_video(raw_path, dest_path)

        file_size = dest_path.stat().st_size
        logger.info(f"Finished processing: {dest_path.name} ({file_size / (1024 * 1024):.2f} MB)")
        return dest_path, file_size

    finally:
        # Cleanup temporary chunks directory
        if temp_dir.exists():
            try:
                shutil.rmtree(temp_dir, ignore_errors=True)
            except Exception as e:
                logger.debug(f"Failed to clean temp dir {temp_dir}: {e}")


async def _download_http_multichunk(
    url: str,
    headers: Dict[str, str],
    temp_dir: Path,
    num_streams: int,
    progress: DownloadProgress,
    progress_callback: Optional[Callable],
    proxy: Optional[str],
) -> Path:
    """Download standard HTTP video file using multi-stream Range chunks."""
    logger.info(f"Probing stream URL for Range support: {url}")
    total_length = None
    supports_ranges = False

    async with httpx.AsyncClient(proxy=proxy, timeout=30.0, follow_redirects=True) as client:
        # 1. Probe with Range request
        probe_headers = {**headers, "Range": "bytes=0-1"}
        try:
            resp = await client.get(url, headers=probe_headers)
            if resp.status_code == 206:
                supports_ranges = True
                content_range = resp.headers.get("content-range", "")
                # e.g. "bytes 0-1/104857600"
                m = re.search(r"/(\d+)$", content_range)
                if m:
                    total_length = int(m.group(1))
            elif resp.status_code == 200:
                content_len = resp.headers.get("content-length")
                if content_len:
                    total_length = int(content_len)
        except Exception as e:
            logger.warning(f"Probe failed: {e}. Will attempt streaming GET.")

    raw_path = temp_dir / "assembled_raw.mp4"

    # Fallback to single stream if range not supported or length unknown
    if not supports_ranges or not total_length or total_length < 2 * 1024 * 1024:
        logger.info("Range chunking not supported or file too small; downloading single stream...")
        async with httpx.AsyncClient(proxy=proxy, timeout=180.0, follow_redirects=True) as client:
            async with client.stream("GET", url, headers=headers) as resp:
                if resp.status_code not in (200, 206):
                    raise RuntimeError(f"HTTP GET returned {resp.status_code}")
                if not total_length:
                    cl = resp.headers.get("content-length")
                    if cl:
                        total_length = int(cl)
                progress.total_bytes = total_length or 0
                with open(raw_path, "wb") as f:
                    async for chunk in resp.aiter_bytes(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)
                            await progress.update_bytes(len(chunk))
                            if progress_callback:
                                progress_callback(progress)
        return raw_path

    # Multi-stream Range download
    progress.total_bytes = total_length
    effective_streams = min(max(num_streams, 16), 32)
    chunk_size = total_length // effective_streams
    logger.info(
        f"Downloading with {effective_streams} concurrent Range streams. Total size: {total_length / (1024 * 1024):.2f} MB"
    )

    ranges: List[Tuple[int, int, Path]] = []
    for i in range(effective_streams):
        start = i * chunk_size
        end = total_length - 1 if i == effective_streams - 1 else (start + chunk_size - 1)
        part_file = temp_dir / f"part_{i:02d}.tmp"
        ranges.append((start, end, part_file))

    limits = httpx.Limits(max_connections=effective_streams + 4, max_keepalive_connections=effective_streams + 4)

    async def _fetch_range(start_byte: int, end_byte: int, out_file: Path, stream_idx: int):
        range_headers = {**headers, "Range": f"bytes={start_byte}-{end_byte}"}
        retries = 3
        for attempt in range(retries):
            try:
                async with httpx.AsyncClient(proxy=proxy, timeout=120.0, limits=limits, follow_redirects=True) as c:
                    async with c.stream("GET", url, headers=range_headers) as r:
                        if r.status_code not in (200, 206):
                            raise RuntimeError(f"Stream {stream_idx} returned {r.status_code}")
                        async with aiofiles.open(out_file, "wb") as pf:
                            async for b in r.aiter_bytes(chunk_size=1024 * 1024):
                                if b:
                                    await pf.write(b)
                                    progress.update_bytes(len(b))
                                    if progress_callback:
                                        now = time.time()
                                        if now - progress._last_callback > 0.8:
                                            progress._last_callback = now
                                            progress_callback(progress)
                return
            except Exception as ex:
                if attempt == retries - 1:
                    logger.error(f"Stream {stream_idx} failed after {retries} attempts: {ex}")
                    raise
                await asyncio.sleep(1.0 * (attempt + 1))

    tasks = [_fetch_range(s, e, f, idx) for idx, (s, e, f) in enumerate(ranges)]
    await asyncio.gather(*tasks)

    # Assemble all parts sequentially
    logger.info("Assembling Range chunks into single video file...")
    with open(raw_path, "wb") as outfile:
        for _, _, part_file in ranges:
            with open(part_file, "rb") as infile:
                shutil.copyfileobj(infile, outfile, length=16 * 1024 * 1024)
            try:
                part_file.unlink()
            except Exception:
                pass

    return raw_path


async def _download_hls(
    url: str,
    headers: Dict[str, str],
    temp_dir: Path,
    num_streams: int,
    progress: DownloadProgress,
    progress_callback: Optional[Callable],
    proxy: Optional[str],
) -> Path:
    """Download HLS playlist (.m3u8) by fetching segments with concurrent workers."""
    logger.info(f"Fetching HLS playlist: {url}")

    async with httpx.AsyncClient(proxy=proxy, timeout=30.0, follow_redirects=True) as client:
        resp = await client.get(url, headers=headers)
        if resp.status_code != 200:
            raise RuntimeError(f"Failed to fetch HLS playlist: HTTP {resp.status_code}")
        playlist_text = resp.text

    # Check if Master playlist
    if "#EXT-X-STREAM-INF" in playlist_text:
        logger.info("Master HLS playlist detected. Finding highest quality variant...")
        media_url = _find_best_variant_url(url, playlist_text)
        logger.info(f"Selected media playlist: {media_url}")
        async with httpx.AsyncClient(proxy=proxy, timeout=30.0, follow_redirects=True) as client:
            m_resp = await client.get(media_url, headers=headers)
            if m_resp.status_code != 200:
                raise RuntimeError(f"Failed to fetch media playlist: HTTP {m_resp.status_code}")
            playlist_text = m_resp.text
            url = media_url

    # Check for encryption (AES-128)
    has_encryption = "#EXT-X-KEY" in playlist_text
    if has_encryption:
        logger.info("Encrypted HLS stream detected (AES-128). Delegating directly to FFmpeg...")
        return await _download_hls_via_ffmpeg(url, headers, temp_dir, proxy)

    # Parse segment URLs
    segment_urls = []
    for line in playlist_text.splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            seg_abs = urllib.parse.urljoin(url, line)
            segment_urls.append(seg_abs)

    if not segment_urls:
        logger.warning("No segments parsed from HLS playlist. Falling back to FFmpeg...")
        return await _download_hls_via_ffmpeg(url, headers, temp_dir, proxy)

    hls_workers = min(max(num_streams, 16), 32)
    progress.total_segments = len(segment_urls)
    logger.info(f"HLS playlist contains {len(segment_urls)} segments. Downloading with {hls_workers} parallel workers...")

    semaphore = asyncio.Semaphore(hls_workers)
    seg_dir = temp_dir / "segments"
    seg_dir.mkdir(parents=True, exist_ok=True)

    limits = httpx.Limits(max_connections=hls_workers + 6, max_keepalive_connections=hls_workers + 6)
    async with httpx.AsyncClient(proxy=proxy, timeout=60.0, limits=limits, follow_redirects=True) as sc:
        async def _fetch_segment(seg_url: str, seg_idx: int):
            async with semaphore:
                seg_path = seg_dir / f"seg_{seg_idx:05d}.ts"
                retries = 3
                for attempt in range(retries):
                    try:
                        sr = await sc.get(seg_url, headers=headers)
                        if sr.status_code == 200:
                            async with aiofiles.open(seg_path, "wb") as f:
                                await f.write(sr.content)
                            progress.update_segment(len(sr.content))
                            if progress_callback:
                                now = time.time()
                                if now - progress._last_callback > 0.8:
                                    progress._last_callback = now
                                    progress_callback(progress)
                            return
                    except Exception as ex:
                        if attempt == retries - 1:
                            logger.error(f"Segment {seg_idx} failed: {ex}")
                            raise
                        await asyncio.sleep(0.5 * (attempt + 1))

        await asyncio.gather(*[_fetch_segment(s_url, i) for i, s_url in enumerate(segment_urls)])

    # Create concat list for FFmpeg
    concat_list_file = temp_dir / "concat_list.txt"
    with open(concat_list_file, "w") as cl:
        for i in range(len(segment_urls)):
            seg_file = seg_dir / f"seg_{i:05d}.ts"
            cl.write(f"file '{seg_file.resolve()}'\n")

    raw_path = temp_dir / "assembled_hls.mp4"
    cmd = [
        FFMPEG_BIN,
        "-y",
        "-f", "concat",
        "-safe", "0",
        "-i", str(concat_list_file),
        "-c", "copy",
        str(raw_path),
    ]

    logger.info("Concatenating HLS segments with FFmpeg...")
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await proc.communicate()
    if proc.returncode != 0:
        logger.warning(f"FFmpeg concat warning: {stderr.decode(errors='replace')[-300:]}")

    if not raw_path.exists() or raw_path.stat().st_size == 0:
        logger.warning("Concat failed or empty. Falling back to direct FFmpeg stream dump...")
        return await _download_hls_via_ffmpeg(url, headers, temp_dir, proxy)

    return raw_path


async def _download_hls_via_ffmpeg(url: str, headers: Dict[str, str], temp_dir: Path, proxy: Optional[str]) -> Path:
    """Fallback FFmpeg HLS stream capture."""
    raw_path = temp_dir / "ffmpeg_hls.mp4"
    header_str = "".join([f"{k}: {v}\r\n" for k, v in headers.items()])

    cmd = [FFMPEG_BIN, "-y"]
    if proxy:
        cmd.extend(["-http_proxy", proxy])
    if header_str:
        cmd.extend(["-headers", header_str])
    cmd.extend([
        "-i", url,
        "-c", "copy",
        "-bsf:a", "aac_adtstoasc",
        str(raw_path),
    ])

    logger.info(f"Capturing HLS stream directly via FFmpeg: {url}")
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await proc.communicate()
    if proc.returncode != 0:
        logger.error(f"FFmpeg direct HLS dump failed: {stderr.decode(errors='replace')[-400:]}")
        raise RuntimeError("FFmpeg HLS stream capture failed")

    return raw_path


def _find_best_variant_url(master_url: str, playlist_text: str) -> str:
    """Parse master playlist and return the URL of highest resolution/bandwidth stream."""
    best_bandwidth = -1
    best_url = None
    current_bandwidth = 0

    lines = playlist_text.splitlines()
    for i, line in enumerate(lines):
        line = line.strip()
        if line.startswith("#EXT-X-STREAM-INF:"):
            m = re.search(r"BANDWIDTH=(\d+)", line)
            current_bandwidth = int(m.group(1)) if m else 0
        elif line and not line.startswith("#"):
            if current_bandwidth > best_bandwidth:
                best_bandwidth = current_bandwidth
                best_url = urllib.parse.urljoin(master_url, line)
            current_bandwidth = 0

    return best_url or master_url
