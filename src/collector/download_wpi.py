"""
Download official World Port Index (WPI - Pub 150) from NGA Maritime Safety Information.
Saves to data/samples/wpi/WPI.csv for dim_port ingestion.
"""

import sys
import urllib.request
from pathlib import Path

WPI_URL = "https://msi.nga.mil/api/publications/world-port-index?output=csv"
OUTPUT_FILE = Path("data/samples/wpi/WPI.csv")


def download_wpi():
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    print(f"Downloading World Port Index from: {WPI_URL}")
    req = urllib.request.Request(
        WPI_URL,
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp, open(OUTPUT_FILE, "wb") as out:
            data = resp.read()
            out.write(data)
        size_kb = OUTPUT_FILE.stat().st_size / 1024
        print(f"Successfully saved WPI dataset to: {OUTPUT_FILE} ({size_kb:.1f} KB)")
    except Exception as e:
        print(f"Download failed: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    download_wpi()
