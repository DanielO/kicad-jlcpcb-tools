#!/bin/sh

set -e

BASEURL=https://yaqwsx.github.io/jlcparts/data/cache
wget -q ${BASEURL}.zip
for seq in $(seq 1 9); do
    wget -q ${BASEURL}.z0$seq || true
done
rm -f cache.sqlite3
7z x cache.zip

python3 gencsv.py cache.sqlite3 parts.csv.xz
rm -f cache.z??
