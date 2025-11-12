#!/usr/bin/env python3
"""
iot_sender_sun_activity.py
- Single script, 8 devices (threads)
- Uses regions.json (contains lat/lon, activity_end, baselines)
- Uses astral to compute sunrise/sunset per region daily
- Temperature begins to fall after sunset
- Pollution levels begin to drop after activity_end (per-region)
- Daytime drift: +/-7% per hour (more dynamic)
- Nighttime drift (post-activity): -5% per hour (smoother decrease)
- Payload fields are strings: "29.29 °C"
- NEW: Automatically saves each payload to daily JSONL log file (Logs/YYYY-MM-DD.jsonl)
"""

import os
import json
import time
import threading
import random
from datetime import datetime, date, timedelta
from astral import LocationInfo
from astral.sun import sun

try:
    from azure.iot.device import IoTHubDeviceClient
except Exception:
    IoTHubDeviceClient = None
    print("Warning: 'azure-iot-device' not installed. Running local-only.")

# ==============================
# CONFIGURATION
# ==============================
DEVICES_FILE = "devices.json"
REGIONS_FILE = "regions.json"
SEND_INTERVAL = 90                # seconds between sends per device
STAGGER_OFFSETS = [0,10,20,30,40,50,60,70]
ACTIVITY_START_HOUR = 6
ACTIVITY_START_MIN = 30          # 6:30 AM

UNITS = {
    "temperature": "°C",
    "humidity": "%",
    "co2": "ppm",
    "no2": "µg/m³",
    "pm25": "µg/m³",
    "pm10": "µg/m³"
}

LOG_MODE = True
LOG_DIR = "Logs"  # folder for daily logs

# Ensure log folder exists at startup
if LOG_MODE and not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

# ==============================
# UTILITY FUNCTIONS
# ==============================
def log_payload(payload):
    """Append one payload JSON to daily log file (Logs/YYYY-MM-DD.jsonl)."""
    if LOG_MODE:
        try:
            date_str = datetime.now().strftime("%Y-%m-%d")
            log_file = os.path.join(LOG_DIR, f"{date_str}.jsonl")
            with open(log_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(payload, ensure_ascii=False) + "\n")
        except Exception as e:
            print(f"[LOG] Failed to write payload: {e}")

def clamp(v, lo, hi):
    return max(lo, min(hi, v))

def get_sun_times_for(date_obj, lat, lon, region_name=""):
    loc = LocationInfo(name=region_name, region=region_name, timezone="Africa/Cairo", latitude=lat, longitude=lon)
    s = sun(loc.observer, date=date_obj)
    return {"sunrise": s["sunrise"], "sunset": s["sunset"]}

# ==============================
# LOAD DEVICES & REGIONS
# ==============================
with open(DEVICES_FILE, "r", encoding="utf-8") as f:
    devices = json.load(f)

with open(REGIONS_FILE, "r", encoding="utf-8") as f:
    regions = json.load(f)

device_entries = []
for idx, d in enumerate(devices):
    deviceId = d.get("deviceId")
    conn = d.get("connectionString")
    if not deviceId or not conn:
        raise ValueError(f"Invalid entry in {DEVICES_FILE} at index {idx}: {d}")
    if deviceId not in regions:
        raise ValueError(f"DeviceId '{deviceId}' not found in {REGIONS_FILE}.")
    device_entries.append({"deviceId": deviceId, "connectionString": conn, "index": idx})

runtime = {}
for entry in device_entries:
    did = entry["deviceId"]
    r = regions[did]
    base = {k: float(r[k]) for k in ("temperature","humidity","co2","no2","pm25","pm10")}
    runtime[did] = {
        "base": base.copy(),
        "state": base.copy(),
        "lat": float(r["lat"]),
        "lon": float(r["lon"]),
        "activity_end": int(r.get("activity_end", 22))
    }

last_drift_hour = {did: None for did in runtime.keys()}

# ==============================
# DRIFT LOGIC
# ==============================
def apply_hourly_drift_for_device(did, is_daytime_flag):
    b = runtime[did]["base"]
    if is_daytime_flag:
        pct = 0.07
        for k in b.keys():
            change = b[k] * pct
            b[k] = clamp(b[k] + random.uniform(-change, change), -1000, 10000)
    else:
        pct = 0.05
        for k in b.keys():
            change = b[k] * pct
            b[k] = clamp(b[k] - random.uniform(0, change), -1000, 10000)

# ==============================
# DEVICE LOOP
# ==============================
def device_loop(device_entry, offset_seconds):
    deviceId = device_entry["deviceId"]
    conn_str = device_entry["connectionString"]

    client = None
    if IoTHubDeviceClient:
        try:
            client = IoTHubDeviceClient.create_from_connection_string(conn_str)
            print(f"[{deviceId}] Connected to IoT Hub.")
        except Exception as e:
            print(f"[{deviceId}] Warning: cannot connect to IoT Hub: {e}")
    else:
        print(f"[{deviceId}] Running local-only (azure-iot-device not installed).")

    time.sleep(offset_seconds)

    while True:
        now = datetime.now()
        today = date.today()
        d = runtime[deviceId]
        lat, lon = d["lat"], d["lon"]

        sun_times = get_sun_times_for(today, lat, lon, d.get("region",""))
        sunset_local_naive = sun_times["sunset"].astimezone().replace(tzinfo=None)
        sunrise_local_naive = sun_times["sunrise"].astimezone().replace(tzinfo=None)

        activity_start = now.replace(hour=ACTIVITY_START_HOUR, minute=ACTIVITY_START_MIN, second=0, microsecond=0)
        ae = d["activity_end"]
        activity_end_dt = now.replace(hour=ae, minute=0, second=0, microsecond=0)
        if ae == 0:
            activity_end_dt += timedelta(days=1)

        is_after_sunset = now >= sunset_local_naive
        is_before_sunrise = now < sunrise_local_naive
        is_after_activity_end = now >= activity_end_dt
        is_effective_day = (now >= activity_start and now < activity_end_dt)

        current_hour = now.hour
        if last_drift_hour[deviceId] != current_hour:
            apply_hourly_drift_for_device(deviceId, is_effective_day)
            last_drift_hour[deviceId] = current_hour
            print(f"[{deviceId}] Hourly drift applied (day={is_effective_day}) at {now.strftime('%Y-%m-%d %H:%M:%S')}")

        base = d["base"]
        state = d["state"]

        if is_after_sunset or is_before_sunrise:
            temp_adj = random.uniform(-3.5, -0.5)
            hum_adj = random.uniform(0.5, 4.0)
        else:
            temp_adj = random.uniform(0.5, 2.5)
            hum_adj = random.uniform(-3.0, -0.5)

        if is_after_activity_end or is_before_sunrise:
            pollutant_multiplier = random.uniform(0.75, 0.9)
            no2_adj = random.uniform(-6.0, -1.0)
        else:
            pollutant_multiplier = random.uniform(0.98, 1.08)
            no2_adj = random.uniform(2.0, 10.0) if is_effective_day else random.uniform(-1.0, 2.0)

        temp = clamp(base["temperature"] + temp_adj + random.gauss(0, 0.5), -50, 70)
        hum = clamp(base["humidity"] + hum_adj + random.gauss(0, 1.5), 0, 100)
        co2 = clamp(base["co2"] * pollutant_multiplier + random.gauss(0, 5.0), 150, 5000)
        no2 = clamp((base["no2"] + no2_adj) * pollutant_multiplier + random.gauss(0, 1.5), 0, 2000)
        pm25 = clamp(base["pm25"] * pollutant_multiplier + random.gauss(0, 1.5), 0, 2000)
        pm10 = clamp(base["pm10"] * pollutant_multiplier + random.gauss(0, 3.0), 0, 5000)

        state.update({
            "temperature": round(temp, 2),
            "humidity": round(hum, 2),
            "co2": round(co2, 2),
            "no2": round(no2, 2),
            "pm25": round(pm25, 2),
            "pm10": round(pm10, 2)
        })

        payload = {
            "deviceId": deviceId,
            "region": regions[deviceId].get("region_name", deviceId),
            "timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
            "temperature": f"{state['temperature']} {UNITS['temperature']}",
            "humidity": f"{state['humidity']} {UNITS['humidity']}",
            "co2": f"{state['co2']} {UNITS['co2']}",
            "no2": f"{state['no2']} {UNITS['no2']}",
            "pm25": f"{state['pm25']} {UNITS['pm25']}",
            "pm10": f"{state['pm10']} {UNITS['pm10']}",
            "period": "night" if (is_after_sunset or is_before_sunrise) else "day",
            "sunset": sunset_local_naive.strftime("%Y-%m-%d %H:%M:%S"),
            "sunrise": sunrise_local_naive.strftime("%Y-%m-%d %H:%M:%S"),
            "activity_end_hour": d["activity_end"]
        }

        text = json.dumps(payload, ensure_ascii=False)

        try:
            if client:
                client.send_message(text)
                print(f"[{deviceId}] Sent to IoT Hub: {payload}")
            else:
                print(f"[{deviceId}] (local) Sent: {payload}")
            log_payload(payload)  # ✅ Save to daily log file
        except Exception as e:
            print(f"[{deviceId}] Send failed: {e}")

        time.sleep(SEND_INTERVAL)

# ==============================
# START THREADS
# ==============================
threads = []
for i, entry in enumerate(device_entries):
    offset = STAGGER_OFFSETS[i % len(STAGGER_OFFSETS)]
    t = threading.Thread(target=device_loop, args=(entry, offset), daemon=True)
    t.start()
    threads.append(t)
    print(f"Started thread for {entry['deviceId']} with offset {offset}s")

# ==============================
# MAIN LOOP
# ==============================
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("Stopping simulator...")
