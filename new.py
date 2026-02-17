#!/usr/bin/env python3

import os
import sys
import argparse
import re
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from itertools import islice

# Try fast JSON
try:
    import orjson as jsonlib
    def loads(x): return jsonlib.loads(x)
    def dumps(x): return jsonlib.dumps(x).decode("utf-8")
except ImportError:
    import json as jsonlib
    def loads(x): return jsonlib.loads(x)
    def dumps(x): return jsonlib.dumps(x, ensure_ascii=False, separators=(",", ":"))

# --- Escape Decoding ---

octal_escape_re = re.compile(r'\\([0-7]{3})')

def decode_escapes(s: str) -> str:
    # Convert octal like \134 → \
    def replace_octal(match):
        return chr(int(match.group(1), 8))

    s = octal_escape_re.sub(replace_octal, s)

    try:
        s = bytes(s, "utf-8").decode("unicode_escape")
    except Exception:
        pass

    return s


# --- JSON Extraction Logic ---

def extract_messages_from_blob(json_blob: str):
    results = []

    try:
        json_blob = decode_escapes(json_blob)
        data = loads(json_blob)
    except Exception:
        return results

    packets = data.get("packets")
    if not isinstance(packets, list):
        return results

    for packet in packets:
        message = packet.get("message")
        if not isinstance(message, list):
            continue

        for event in message:
            # If event is string, try decoding inner JSON
            if isinstance(event, str):
                decoded = decode_escapes(event)
                try:
                    event = loads(decoded)
                except Exception:
                    continue  # skip non-json strings

            if isinstance(event, (dict, list)):
                results.append(event)

    return results


# --- Worker Function ---

def process_chunk(lines):
    output = []

    for line in lines:
        if not line or line.startswith("#"):
            continue

        # bulk_extractor format:
        # offset<TAB>json_blob<TAB>md5
        parts = line.rstrip("\n").split("\t")

        if len(parts) < 3:
            continue

        json_blob = parts[1]

        events = extract_messages_from_blob(json_blob)
        for event in events:
            try:
                output.append(dumps(event))
            except Exception:
                continue

    return output


# --- Chunk Generator ---

def chunked_iterator(file_obj, chunk_size):
    while True:
        chunk = list(islice(file_obj, chunk_size))
        if not chunk:
            break
        yield chunk


# --- Main ---

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--workers", type=int, default=multiprocessing.cpu_count())
    parser.add_argument("--chunk-size", type=int, default=50000)
    parser.add_argument("--max-mb", type=int, default=50)

    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    max_bytes = args.max_mb * 1024 * 1024
    file_index = 0
    current_size = 0

    def new_output_file(index):
        return open(
            os.path.join(args.output_dir, f"output_{index:03d}.ndjson"),
            "w",
            encoding="utf-8",
            buffering=1024 * 1024
        )

    out_f = new_output_file(file_index)

    with open(args.input, "r", encoding="utf-8", errors="ignore") as f, \
         ProcessPoolExecutor(max_workers=args.workers) as executor:

        futures = []
        for chunk in chunked_iterator(f, args.chunk_size):
            futures.append(executor.submit(process_chunk, chunk))

        for future in futures:
            try:
                results = future.result()
            except Exception:
                continue

            for line in results:
                encoded_line = line + "\n"
                encoded_bytes = encoded_line.encode("utf-8")

                if current_size + len(encoded_bytes) > max_bytes:
                    out_f.close()
                    file_index += 1
                    current_size = 0
                    out_f = new_output_file(file_index)

                out_f.write(encoded_line)
                current_size += len(encoded_bytes)

    out_f.close()

    print("Extraction complete.")


if __name__ == "__main__":
    main()
