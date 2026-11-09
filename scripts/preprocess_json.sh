#!/bin/bash
# Preprocess JSON files: compact multi-line JSON into single-line format
# using `jq -c .` to enable efficient line-by-line streaming in Dask Bag.

set -euo pipefail

INPUT_DIR="${1:-/home/noemi/pdf_json}"
OUTPUT_DIR="${2:-/home/noemi/1001g}"
MAX_FILES="${3:-1000}"

mkdir -p "$OUTPUT_DIR"

echo "Input directory:  $INPUT_DIR"
echo "Output directory: $OUTPUT_DIR"
echo "Max files:        $MAX_FILES"

i=0
for f in "$INPUT_DIR"/*.json; do
    [ -e "$f" ] || { echo "No JSON files found in $INPUT_DIR"; exit 1; }
    
    (( i >= MAX_FILES )) && break
    (( ++i ))
    
    echo "Processing ($i/$MAX_FILES): $(basename "$f")..."
    compact_file="${OUTPUT_DIR}/$(basename "${f%.json}g.json")"
    jq -c . "$f" > "$compact_file"
done

echo "Done! Processed $i files into $OUTPUT_DIR."
