#!/usr/bin/env python3
"""
Fetch data from Tuya devices and append to a daily CSV file named:
TROFICDORDYYYYMMDD.csv

This script expects environment variables:
- TUYA_ACCESS_ID
- TUYA_ACCESS_KEY
- STORAGE_URL (optional, to POST the JSON readings)
"""

import hashlib
import hmac
import json
import time
import requests
from datetime import datetime, date
import os
import sys

# Get credentials from environment variables
ACCESS_ID = os.environ.get("TUYA_ACCESS_ID")
ACCESS_KEY = os.environ.get("TUYA_ACCESS_KEY")
ENDPOINT = "https://openapi.tuyaeu.com"

# All three device IDs
DEVICE_IDS = [
    "bf7ecfc010164c849dwpqp",
    "bf58e5674b50680432umav",
    "bfefd09a22241b31b10cpo"
]

# For storing data - optional webhook URL for data storage
STORAGE_URL = os.environ.get("STORAGE_URL", "")

class SimpleTuyaAPI:
    def __init__(self, endpoint, access_id, access_key):
        self.endpoint = endpoint
        self.access_id = access_id
        self.access_key = access_key
        self.token = None

    def _sign_request(self, method, path, params=None, body=None):
        """Generate signature for Tuya API request"""
        t = str(int(time.time() * 1000))

        str_to_sign = method + "\n"

        if body:
            content = json.dumps(body) if isinstance(body, dict) else body
            str_to_sign += hashlib.sha256(content.encode()).hexdigest() + "\n"
        else:
            str_to_sign += hashlib.sha256(b"").hexdigest() + "\n"

        str_to_sign += "\n"
        str_to_sign += path

        sign_str = self.access_id + (self.token or "") + t + str_to_sign
        signature = hmac.new(
            self.access_key.encode(),
            sign_str.encode(),
            hashlib.sha256
        ).hexdigest().upper()

        return {
            "client_id": self.access_id,
            "sign": signature,
            "t": t,
            "sign_method": "HMAC-SHA256",
            "access_token": self.token or ""
        }

    def connect(self):
        """Get access token"""
        path = "/v1.0/token?grant_type=1"
        headers = self._sign_request("GET", path)

        response = requests.get(
            self.endpoint + path,
            headers=headers,
            timeout=15
        )

        try:
            result = response.json()
        except Exception as e:
            print("ERROR: Token response not JSON:", e)
            return False

        if result.get("success"):
            self.token = result["result"]["access_token"]
            return True
        print("ERROR: token call failed:", result)
        return False

    def get(self, path):
        """Make GET request to Tuya API"""
        headers = self._sign_request("GET", path)
        response = requests.get(
            self.endpoint + path,
            headers=headers,
            timeout=15
        )
        try:
            return response.json()
        except Exception as e:
            print("ERROR: response not JSON:", e)
            return {"success": False, "msg": "invalid response"}

def extract_temp_humidity(status_result):
    """Extract temperature and humidity from device status"""
    temp = None
    humidity = None

    for dp in status_result:
        code = dp.get("code")
        value = dp.get("value")

        # Temperature codes (some devices report temperature * 10)
        if code in ("temp_current", "temperature", "va_temperature"):
            if isinstance(value, (int, float)):
                # many Tuya sensors use integer tenths
                temp = value / 10.0 if abs(value) > 50 else value
            else:
                temp = value
        # Humidity codes (some devices report humidity * 10)
        elif code in ("humidity_value", "humidity", "va_humidity"):
            if isinstance(value, (int, float)):
                # if the value looks like tenths (e.g., >100)
                humidity = value / 10.0 if value > 100 else value
            else:
                humidity = value

    return temp, humidity

def send_to_storage(readings):
    """Send all readings to external storage if configured"""
    if STORAGE_URL:
        try:
            response = requests.post(STORAGE_URL, json=readings, timeout=10)
            print(f"Sent to storage: {response.status_code}")
        except Exception as e:
            print(f"Storage error: {e}")

def ensure_dir(path):
    try:
        os.makedirs(path, exist_ok=True)
    except Exception as e:
        print("Could not create directory", path, e)

def append_to_csv(csv_path, rows):
    header = "timestamp,device_id,temperature_c,humidity_percent\n"
    exists = os.path.exists(csv_path)
    with open(csv_path, "a", encoding="utf-8") as f:
        if not exists:
            f.write(header)
        for r in rows:
            line = f"{r['timestamp']},{r['device_id']},{r['temperature_c']},{r['humidity_percent']}\n"
            f.write(line)

def main():
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n[{timestamp}] Starting IoT data fetch for {len(DEVICE_IDS)} devices...")

    if not ACCESS_ID or not ACCESS_KEY:
        print("ERROR: TUYA_ACCESS_ID and TUYA_ACCESS_KEY must be set")
        print(f"DEBUG: ACCESS_ID present: {bool(ACCESS_ID)}, ACCESS_KEY present: {bool(ACCESS_KEY)}")
        return 1
    
    print(f"DEBUG: Using ACCESS_ID: {ACCESS_ID[:8]}... (truncated for security)")

    api = SimpleTuyaAPI(ENDPOINT, ACCESS_ID, ACCESS_KEY)

    if not api.connect():
        print(f"ERROR: Could not connect to Tuya API")
        return 2

    print("Connected successfully\n")

    all_readings = []

    # Fetch data from each device
    for device_id in DEVICE_IDS:
        print(f"--- Fetching device: {device_id} ---")

        status_response = api.get(f"/v1.0/devices/{device_id}/status")

        if not status_response.get("success"):
            print(f"ERROR: Failed to get status for {device_id} - {status_response}")
            continue

        temp, humidity = extract_temp_humidity(status_response["result"])

        if temp is not None and humidity is not None:
            reading = {
                "timestamp": timestamp,
                "device_id": device_id,
                "temperature_c": temp,
                "humidity_percent": humidity
            }
            all_readings.append(reading)
            print(f"SUCCESS: Temp: {temp}C, Humidity: {humidity}%")
        else:
            print(f"WARNING: Could not extract temperature or humidity for {device_id}")

        print()

    # Summary
    print(f"--- Summary ---")
    print(f"Successfully read {len(all_readings)}/{len(DEVICE_IDS)} devices")

    # Send to storage if configured
    if all_readings:
        send_to_storage(all_readings)

        # Write to daily CSV file
        today = date.today().strftime("%Y%m%d")
        filename = f"TROFICDORD{today}.csv"
        outdir = "data"
        ensure_dir(outdir)
        csv_path = os.path.join(outdir, filename)
        append_to_csv(csv_path, all_readings)
        print(f"Wrote/updated CSV: {csv_path}")

        # also update manifest (so main branch has manifest)
        try:
            files = sorted([f for f in os.listdir(outdir) if f.startswith('TROFICDORD') and f.endswith('.csv')])
            with open(os.path.join(outdir,'manifest.json'),'w') as fh:
                json.dump(files, fh)
        except Exception as e:
            print('Could not update manifest on main branch:', e)

        # also print as CSV format for logs
        print("\n--- CSV Format ---")
        print("timestamp,device_id,temperature_c,humidity_percent")
        for reading in all_readings:
            print(f"{reading['timestamp']},{reading['device_id']},{reading['temperature_c']},{reading['humidity_percent']}")
    else:
        print("No readings collected; nothing to write.")

    return 0

if __name__ == "__main__":
    exit(main())
