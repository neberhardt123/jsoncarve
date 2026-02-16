#!/usr/bin/env python3

import sys
import os
import gzip
import json
import csv

DECOMP_CHUNK = 64 * 1024
MAX_DECOMPRESSED_BYTES = 200 * 1024 * 1024  # 200MB safety limit per gzip

def stream_decompress_from_offset(f, offset):
    f.seek(offset)
    gz = gzip.GzipFile(fileobj=f)

    total = 0
    while True:
        chunk = gz.read(DECOMP_CHUNK)
        if not chunk:
            break
        total += len(chunk)
        if total > MAX_DECOMPRESSED_BYTES:
            break
        yield chunk

def extract_json_objects(chunk_iter):
    """
    Attempts NDJSON first, then falls back to brace-matching JSON extraction.
    """

    buffer = ""

    # First pass: try NDJSON
    for b in chunk_iter:
        buffer += b.decode("utf-8", errors="replace")

        while "\n" in buffer:
            line, buffer = buffer.split("\n", 1)
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                yield json.dumps(obj, ensure_ascii=False)
            except Exception:
                # Not NDJSON; fall back to brace parser
                buffer = line + "\n" + buffer
                break
        else:
            continue
        break

    # Collect remaining stream for brace parsing
    for b in chunk_iter:
        buffer += b.decode("utf-8", errors="replace")

    i = 0
    n = len(buffer)

    while i < n:
        if buffer[i] not in "{[":
            i += 1
            continue

        start = i
        depth = 0
        in_string = False
        escape = False

        j = i
        while j < n:
            c = buffer[j]

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
                elif c in "{[":
                    depth += 1
                elif c in "}]":
                    depth -= 1
                    if depth == 0:
                        candidate = buffer[start:j+1]
                        try:
                            obj = json.loads(candidate)
                            yield json.dumps(obj, ensure_ascii=False)
                            i = j + 1
                            break
                        except Exception:
                            i = start + 1
                            break
            j += 1
        else:
            break

def main():
    if len(sys.argv) != 3:
        print("Usage: python3 carve_extract_json.py disk.img outdir")
        sys.exit(1)

    image = sys.argv[1]
    outdir = sys.argv[2]

    offsets_file = "gzip_offsets.txt"

    os.makedirs(outdir, exist_ok=True)

    index_path = os.path.join(outdir, "index.csv")
    index = open(index_path, "w", newline="", encoding="utf-8")
    writer = csv.writer(index)
    writer.writerow(["offset", "json_file", "bytes"])

    with open(offsets_file) as f:
        offsets = [int(x.strip()) for x in f if x.strip()]

    with open(image, "rb") as fh:
        for offset in offsets:
            print(f"[+] Trying offset {offset}")

            try:
                chunks = stream_decompress_from_offset(fh, offset)
                count = 0

                for idx, js in enumerate(extract_json_objects(chunks)):
                    fname = f"json_{offset}_{idx}.json"
                    path = os.path.join(outdir, fname)

                    with open(path, "w", encoding="utf-8") as out:
                        out.write(js + "\n")

                    size = os.path.getsize(path)
                    writer.writerow([offset, fname, size])
                    index.flush()
                    count += 1

                if count == 0:
                    print(f"    [-] No JSON found")

                else:
                    print(f"    [+] Extracted {count} JSON objects")

            except Exception as e:
                print(f"    [!] Failed: {e}")

    index.close()
    print("Done.")

if __name__ == "__main__":
    main()