#!/usr/bin/env python3
"""
parallel_extract_json_from_carved.py

What it does:
 - Walks an input directory for files (carved .gz/.bin/whatever).
 - For each file (processed in parallel workers):
     * Detects likely compression type (gzip, zlib, raw-deflate) using header heuristics.
     * Uses a tolerant zlib.DecompressObj to stream-decompress until exhaustion (works with truncated files).
     * Streams decompressed bytes through a JSON extractor that tries NDJSON first, then a brace-aware parser.
     * Sends recovered JSON lines to the main process via a multiprocessing.Queue.
 - Main process:
     * Consumes JSON lines and writes them into rotating merged NDJSON files (max size configurable, default 50 MB).
     * Tracks file-level metadata (json count, decompressed bytes, errors) and writes index.csv.
     * Shows progress and ETA.

Notes:
 - This tries to be defensive (many real-world carved files are truncated or partially overwritten).
 - It uses only the Python standard library.
"""

import os
import sys
import argparse
import multiprocessing as mp
import time
import zlib
import json
import csv
from pathlib import Path
from queue import Empty as QueueEmpty

# --- Tunables ---
DECOMP_CHUNK = 64 * 1024
MAX_DECOMPRESSED_BYTES = 500 * 1024 * 1024  # per-file safety limit (500MB) - adjust if you expect huge payloads
DEFAULT_MAX_NDJSON_BYTES = 50 * 1024 * 1024  # rotate merged ndjson every 50 MB by default

# --- Worker function helpers ---


def detect_compression_type(path, peek=4):
    """
    Heuristic detection:
     - gzip: first two bytes 0x1f 0x8b
     - zlib: first byte often 0x78 (0x78 0x9C, 0x78 0x01, 0x78 0xDA)
     - fallback: raw deflate
    Returns: 'gzip', 'zlib', or 'deflate'
    """
    try:
        with open(path, "rb") as f:
            hdr = f.read(peek)
    except Exception:
        return "deflate"
    if len(hdr) >= 2 and hdr[0:2] == b"\x1f\x8b":
        return "gzip"
    if len(hdr) >= 1 and hdr[0] == 0x78:
        # common zlib header values start with 0x78
        return "zlib"
    # fallback
    return "deflate"


def tolerant_decompress_stream(path, ctype):
    """
    Generator: yields decompressed byte chunks from `path`.
    ctype in {'gzip', 'zlib', 'deflate'} chooses the wbits for zlib.decompressobj.
    This is tolerant to truncated streams; flush at the end is attempted.
    """
    if ctype == "gzip":
        wbits = 16 + zlib.MAX_WBITS
    elif ctype == "zlib":
        wbits = zlib.MAX_WBITS
    else:
        # raw deflate
        wbits = -zlib.MAX_WBITS

    d = zlib.decompressobj(wbits)
    total_out = 0

    with open(path, "rb") as f:
        while True:
            data = f.read(DECOMP_CHUNK)
            if not data:
                break
            try:
                out = d.decompress(data)
            except Exception:
                # decompression failed for this chunk; yield what we have and stop
                # (this is the tolerant behavior for corrupted inputs)
                break
            if out:
                total_out += len(out)
                if total_out > MAX_DECOMPRESSED_BYTES:
                    # safety cutoff
                    yield out
                    break
                yield out
    # flush any remaining
    try:
        tail = d.flush()
        if tail:
            yield tail
    except Exception:
        pass


def stream_json_extractor(byte_chunks):
    """
    Accepts an iterator/generator yielding bytes (decompressed).
    Yields complete JSON strings (one per item), encoded as Python str (utf-8).
    Strategy:
      - Try NDJSON (line-based) first; if it fails, fall back to brace-aware extraction.
      - Works across chunk boundaries.
    """
    buf = ""
    # First attempt NDJSON line extraction
    it = iter(byte_chunks)
    try:
        while True:
            b = next(it)
            s = b.decode("utf-8", errors="replace")
            buf += s
            while "\n" in buf:
                line, buf = buf.split("\n", 1)
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    # normalized JSON as a compact line
                    yield json.dumps(obj, ensure_ascii=False)
                    continue
                except Exception:
                    # not NDJSON line -> fallback
                    buf = line + "\n" + buf
                    raise StopIteration  # break out to fallback parsing
    except StopIteration:
        # proceed to gather the rest into the buffer and fall back to bracket parser
        pass
    except StopIteration:
        pass
    except Exception:
        # if NDJSON pass fails for any unexpected reason, continue to fallback
        pass

    # collect remaining chunks
    for b in it:
        try:
            buf += b.decode("utf-8", errors="replace")
        except Exception:
            buf += b.decode("latin1", errors="replace")

    # Brace-aware parser (works for concatenated JSON objects/arrays)
    i = 0
    n = len(buf)
    while i < n:
        # find next opening brace or bracket
        while i < n and buf[i] not in "{[":
            i += 1
        if i >= n:
            break
        start = i
        depth = 0
        in_string = False
        escape = False
        j = i
        while j < n:
            c = buf[j]
            if in_string:
                if escape:
                    escape = False
                elif c == "\\":
                    escape = True
                elif c == '"':
                    in_string = False
            else:
                if c == '"':
                    in_string = True
                elif c == "{" or c == "[":
                    depth += 1
                elif c == "}" or c == "]":
                    depth -= 1
                    if depth == 0:
                        candidate = buf[start : j + 1]
                        try:
                            obj = json.loads(candidate)
                            yield json.dumps(obj, ensure_ascii=False)
                            i = j + 1
                            break
                        except Exception:
                            # not a valid JSON candidate; move start forward
                            i = start + 1
                            break
            j += 1
        else:
            # ran out of buffer before closing; stop
            break


def process_single_file(path, out_q):
    """
    Worker function to:
     - detect compression type
     - stream-decompress tolerantly
     - extract JSON objects and send them to out_q
    out_q messages:
     - ('json', json_line)
     - ('done', filename, json_count, decompressed_bytes)
     - ('error', filename, errmsg)
    """
    fname = os.path.basename(path)
    json_count = 0
    decompressed_bytes = 0
    try:
        ctype = detect_compression_type(path)
        # attempt decompression using that type; if it yields nothing, optionally try fallbacks
        # The generator yields decompressed byte chunks
        gen = tolerant_decompress_stream(path, ctype)

        # If gen yields nothing, we can attempt other types as fallback (zlib/gzip/deflate)
        # We'll try up to one fallback type if the first yields zero bytes
        first_yielded = False
        # We'll buffer the yielded chunks into a small local generator to allow fallback attempts
        chunks_buffer = []
        for chunk in gen:
            chunks_buffer.append(chunk)
            first_yielded = True
            break

        # If first yielded something, continue from generator (we have the original gen, but we consumed one chunk)
        if first_yielded:
            # Create an iterator that yields the chunk we consumed plus the rest from gen
            def whole_stream():
                yield from chunks_buffer
                for c in gen:
                    yield c

            dec_stream = whole_stream()
        else:
            # Try fallbacks if initial type produced nothing
            tried = {ctype}
            fallback_types = ["gzip", "zlib", "deflate"]
            found = False
            for t in fallback_types:
                if t in tried:
                    continue
                gen2 = tolerant_decompress_stream(path, t)
                # check for any output
                chs = []
                for c in gen2:
                    chs.append(c)
                    found = True
                    break
                if found:
                    def whole_stream():
                        yield from chs
                        for c in gen2:
                            yield c
                    dec_stream = whole_stream()
                    break
                tried.add(t)
            if not found:
                # nothing decompressed at all
                out_q.put(("done", fname, 0, 0))
                return

        # Now pass dec_stream to json extractor
        for js in stream_json_extractor(dec_stream):
            # js is a string (not bytes); send as ('json', js)
            out_q.put(("json", js))
            json_count += 1
            decompressed_bytes += len(js.encode("utf-8"))
        out_q.put(("done", fname, json_count, decompressed_bytes))
    except Exception as e:
        out_q.put(("error", fname, str(e)))
        out_q.put(("done", fname, json_count, decompressed_bytes))


# --- Main orchestration / writer loop ---

def writer_loop(out_q, out_dir, max_ndjson_bytes, total_files):
    """
    Consume queue messages and:
     - write ('json', line) into rotating merged ndjson files
     - collect stats on 'done' messages and return them when finished
    Returns dict stats mapping filenames -> metadata
    """
    os.makedirs(out_dir, exist_ok=True)
    merged_index = 0
    merged_path = os.path.join(out_dir, f"merged_{merged_index:04d}.ndjson")
    merged_f = open(merged_path, "a", encoding="utf-8")
    merged_size = os.path.getsize(merged_path) if os.path.exists(merged_path) else 0

    files_done = 0
    start_time = time.time()

    stats = {}  # filename -> {json_count, bytes, error}

    # progress print helper
    def print_progress():
        elapsed = time.time() - start_time
        files_done_local = files_done
        if files_done_local > 0:
            rate = files_done_local / elapsed
            remaining = max(total_files - files_done_local, 0)
            eta = remaining / rate if rate > 0 else float("inf")
            eta_s = time.strftime("%H:%M:%S", time.gmtime(eta)) if eta != float("inf") else "unknown"
        else:
            eta_s = "unknown"
        print(
            f"\rProcessed files: {files_done_local}/{total_files} | "
            f"Merged files: merged_{merged_index:04d}.ndjson | "
            f"Elapsed: {int(elapsed)}s | ETA: {eta_s}     ",
            end="",
            flush=True,
        )

    # Continue until we see total_files 'done' events and queue drained
    done_file_names = set()

    while True:
        try:
            msg = out_q.get(timeout=1.0)
        except QueueEmpty:
            # If all files done and queue is empty, break
            if files_done >= total_files:
                # drain queue quickly (non-blocking)
                try:
                    while True:
                        msg = out_q.get_nowait()
                        # process
                        if not msg:
                            continue
                        typ = msg[0]
                        if typ == "json":
                            jline = msg[1]
                            data = (jline + "\n").encode("utf-8")
                            # rotate if needed
                            if merged_size + len(data) > max_ndjson_bytes:
                                merged_f.close()
                                merged_index += 1
                                merged_path = os.path.join(out_dir, f"merged_{merged_index:04d}.ndjson")
                                merged_f = open(merged_path, "a", encoding="utf-8")
                                merged_size = 0
                            merged_f.write(jline + "\n")
                            merged_size += len(data)
                        elif typ == "done":
                            _, fname, cnt, bytes_ = msg
                            stats[fname] = {"json_count": cnt, "bytes": bytes_, "error": None}
                            files_done += 1
                            done_file_names.add(fname)
                            print_progress()
                        elif typ == "error":
                            _, fname, err = msg
                            if fname not in stats:
                                stats[fname] = {"json_count": 0, "bytes": 0, "error": err}
                            else:
                                stats[fname]["error"] = err
                            print_progress()
                except QueueEmpty:
                    pass
                break
            else:
                # still waiting for workers; print progress occasionally
                print_progress()
                continue

        if not msg:
            continue
        typ = msg[0]
        if typ == "json":
            jline = msg[1]
            data = (jline + "\n").encode("utf-8")
            # rotate if needed
            if merged_size + len(data) > max_ndjson_bytes:
                merged_f.close()
                merged_index += 1
                merged_path = os.path.join(out_dir, f"merged_{merged_index:04d}.ndjson")
                merged_f = open(merged_path, "a", encoding="utf-8")
                merged_size = 0
            merged_f.write(jline + "\n")
            merged_size += len(data)
        elif typ == "done":
            _, fname, cnt, bytes_ = msg
            if fname not in stats:
                stats[fname] = {"json_count": cnt, "bytes": bytes_, "error": None}
            else:
                stats[fname].update({"json_count": cnt, "bytes": bytes_})
            files_done += 1
            print_progress()
        elif typ == "error":
            _, fname, err = msg
            if fname not in stats:
                stats[fname] = {"json_count": 0, "bytes": 0, "error": err}
            else:
                stats[fname]["error"] = err
            print_progress()

    # close merged file
    try:
        merged_f.close()
    except Exception:
        pass

    print("\nAll done. Writing index.csv ...")
    # write index.csv
    idx_path = os.path.join(out_dir, "index.csv")
    with open(idx_path, "w", newline="", encoding="utf-8") as idxf:
        w = csv.writer(idxf)
        w.writerow(["source_file", "json_count", "decompressed_bytes", "error"])
        for fname, meta in sorted(stats.items()):
            w.writerow([fname, meta.get("json_count", 0), meta.get("bytes", 0), meta.get("error", "")])

    return stats


def gather_input_files(input_dir):
    p = Path(input_dir)
    if not p.exists():
        raise FileNotFoundError(input_dir)
    files = []
    if p.is_dir():
        for child in sorted(p.iterdir()):
            if child.is_file():
                # include all files; we rely on detection to pick compression type
                files.append(str(child))
    else:
        # single file path
        files = [str(p)]
    return files


def main():
    parser = argparse.ArgumentParser(description="Parallel JSON extraction from carved compressed files (tolerant).")
    parser.add_argument("--input-dir", "-i", required=True, help="Directory with carved files (or single file).")
    parser.add_argument("--out-dir", "-o", required=True, help="Output directory for merged NDJSON and index.csv")
    parser.add_argument("--workers", "-w", type=int, default=max(1, mp.cpu_count() - 1), help="Number of parallel workers")
    parser.add_argument("--max-ndjson-mb", type=int, default=50, help="Max MB per merged NDJSON file before rotating")
    args = parser.parse_args()

    input_dir = args.input_dir
    out_dir = args.out_dir
    workers = args.workers
    max_ndjson_bytes = args.max_ndjson_mb * 1024 * 1024

    files = gather_input_files(input_dir)
    total_files = len(files)
    if total_files == 0:
        print("No files found in input.")
        sys.exit(1)

    print(f"Found {total_files} files. Spawning {workers} workers...")
    ctx = mp.get_context("spawn")
    out_q = ctx.Queue(maxsize=10000)

    # Start worker pool and dispatch tasks
    pool = ctx.Pool(processes=workers)

    # Launch tasks
    for pth in files:
        pool.apply_async(process_single_file, args=(pth, out_q))

    pool.close()

    # Main process consumes messages and writes merged NDJSON
    stats = writer_loop(out_q, out_dir, max_ndjson_bytes, total_files)

    # Join workers
    pool.join()

    print("Finished. Summary:")
    total_json = sum(meta.get("json_count", 0) for meta in stats.values())
    total_bytes = sum(meta.get("bytes", 0) for meta in stats.values())
    errors = {f: m["error"] for f, m in stats.items() if m.get("error")}
    print(f"  Files processed: {total_files}")
    print(f"  JSON objects recovered: {total_json}")
    print(f"  Total decompressed bytes (approx): {total_bytes}")
    print(f"  Files with errors: {len(errors)}")
    if errors:
        print("  Example errors (up to 10):")
        for i, (f, e) in enumerate(errors.items()):
            print(f"    {f}: {e}")
            if i >= 9:
                break


if __name__ == "__main__":
    main()
