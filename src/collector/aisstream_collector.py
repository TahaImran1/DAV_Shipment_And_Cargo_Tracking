"""
AISStream WebSocket collector.
Runs OUTSIDE Databricks on a local machine or server.
Collects position + static AIS messages for a fixed bounding box,
buffers them, and writes JSON Lines files in batches.

Region: Houston / Galveston Bay
Bounding box: [[29.351, -94.702], [29.201, -94.536]]  (AISStream order: [[max_lat, min_lon], [min_lat, max_lon]])
"""

import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import websockets
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("AISSTREAM_API_KEY")
if not API_KEY:
    print("ERROR: AISSTREAM_API_KEY not set in .env", file=sys.stderr)
    sys.exit(1)

# Galveston Entrance, Port of Galveston & Texas City Industrial Channel
# Includes: Galveston sea buoy, Bolivar Roads anchorage, Port of Galveston, Texas City chemical docks, and Lower Galveston Bay up to Bayport approach
# Target data volume: ~3 to 5 MB per day
# Format required by AISStream: [[max_lat, min_lon], [min_lat, max_lon]]
BOUNDING_BOX = [
    [
        float(os.getenv("AIS_MAX_LAT", "29.600")),
        float(os.getenv("AIS_MIN_LON", "-94.950")),
    ],
    [
        float(os.getenv("AIS_MIN_LAT", "29.200")),
        float(os.getenv("AIS_MAX_LON", "-94.500")),
    ],
]

MESSAGE_TYPES = [
    "PositionReport",
    "StandardClassBPositionReport",
    "ExtendedClassBPositionReport",
    "ShipStaticData",
    "StaticDataReport",
]

OUTPUT_DIR = Path("data/samples/incremental_load")
BATCH_SIZE = int(os.getenv("AIS_BATCH_SIZE", "100"))  # records per file
FLUSH_INTERVAL_SEC = int(os.getenv("AIS_FLUSH_INTERVAL_SEC", "60"))  # flush every 60s if batch not full


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def utc_today_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d")


def write_batch(records: list) -> None:
    if not records:
        return
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    fname = OUTPUT_DIR / f"ais_daily_{utc_today_str()}.json"
    with open(fname, "a", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, default=str) + "\n")
    total_bytes = fname.stat().st_size
    size_str = f"{total_bytes / (1024 * 1024):.2f} MB" if total_bytes >= 1024 * 1024 else f"{total_bytes / 1024:.1f} KB"
    print(f"[{utc_now_iso()}] Appended {len(records)} records -> {fname.name} (Total size: {size_str})")


async def collect():
    buffer = []
    lock = asyncio.Lock()
    last_msg_time = asyncio.get_event_loop().time()
    last_flush = asyncio.get_event_loop().time()

    subscription = {
        "APIKey": API_KEY,
        "BoundingBoxes": [BOUNDING_BOX],
        "FilterMessageTypes": MESSAGE_TYPES,
    }

    async def background_flusher():
        nonlocal last_flush
        try:
            while True:
                await asyncio.sleep(5)
                now = asyncio.get_event_loop().time()
                async with lock:
                    if buffer and (now - last_flush) >= FLUSH_INTERVAL_SEC:
                        write_batch(list(buffer))
                        buffer.clear()
                        last_flush = now
                # Heartbeat if quiet for > 30s
                if (now - last_msg_time) >= 30 and (int(now) % 30 < 6):
                    idle_sec = int(now - last_msg_time)
                    print(f"[{utc_now_iso()}] Listening... (no new vessels in bounding box for {idle_sec}s)")
        except asyncio.CancelledError:
            pass

    flusher_task = asyncio.create_task(background_flusher())

    try:
        while True:
            try:
                async with websockets.connect(
                    "wss://stream.aisstream.io/v0/stream",
                    ping_interval=20,
                    ping_timeout=20,
                    max_size=2**23,
                ) as ws:
                    await ws.send(json.dumps(subscription))
                    print(f"[{utc_now_iso()}] Connected. Subscribed to {MESSAGE_TYPES}")
                    print(f"[{utc_now_iso()}] Bounding box: {BOUNDING_BOX}")
                    print(f"[{utc_now_iso()}] Batch size: {BATCH_SIZE} | Flush interval: {FLUSH_INTERVAL_SEC}s")

                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError:
                            continue

                        mtype = msg.get("MessageType")
                        if mtype == "SubscriptionConfirmation":
                            print(f"[{utc_now_iso()}] AISStream subscription confirmed. Waiting for messages in area...")
                            continue

                        meta = msg.get("MetaData", {})
                        ship_name = meta.get("ShipName", "").strip() or "Unknown"
                        mmsi = meta.get("MMSI", "Unknown")

                        msg["_collected_at"] = utc_now_iso()
                        last_msg_time = asyncio.get_event_loop().time()

                        async with lock:
                            buffer.append(msg)
                            count = len(buffer)
                            print(
                                f"[{utc_now_iso()}] [{count}/{BATCH_SIZE}] "
                                f"{mtype} from '{ship_name}' (MMSI: {mmsi})"
                            )
                            if count >= BATCH_SIZE:
                                write_batch(list(buffer))
                                buffer.clear()
                                last_flush = asyncio.get_event_loop().time()

            except (websockets.ConnectionClosed, OSError) as e:
                print(f"[{utc_now_iso()}] Connection error: {e}. Reconnecting in 5s...", file=sys.stderr)
                async with lock:
                    if buffer:
                        write_batch(list(buffer))
                        buffer.clear()
                        last_flush = asyncio.get_event_loop().time()
                await asyncio.sleep(5)
    except asyncio.CancelledError:
        pass
    finally:
        flusher_task.cancel()
        async with lock:
            if buffer:
                print(f"[{utc_now_iso()}] Flushing remaining {len(buffer)} records before exit...")
                write_batch(list(buffer))
                buffer.clear()
        print(f"[{utc_now_iso()}] Collector stopped cleanly.")


if __name__ == "__main__":
    try:
        asyncio.run(collect())
    except KeyboardInterrupt:
        print(f"\n[{utc_now_iso()}] Stopped by user (Ctrl+C).")