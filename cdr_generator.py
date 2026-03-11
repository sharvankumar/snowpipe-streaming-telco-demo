"""
Enhanced CDR Generator — demo_300

Extends demo_200 generator with:
  * GEOGRAPHY data  — cell tower GeoJSON points
  * GEOMETRY data   — coverage area WKT polygons
  * Semi-structured — device_info (dict), service_tags (list),
                      network_measurements (dict)
  * Raw PII fields  — caller/callee numbers (encrypted later by PIIEncryptor)
  * Traffic profiles (same 5 as demo_200)
"""

import json
import math
import random
import uuid
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import Dict, List, Tuple


class TrafficProfile(Enum):
    NORMAL = auto()
    PEAK_HOUR = auto()
    BURST = auto()
    DEGRADED = auto()
    MAINTENANCE = auto()


_PROFILE_CONFIG = {
    TrafficProfile.NORMAL:      {"batch_mult": 1.0,  "status_weights": [0.95, 0.03, 0.01, 0.01]},
    TrafficProfile.PEAK_HOUR:   {"batch_mult": 3.0,  "status_weights": [0.93, 0.04, 0.02, 0.01]},
    TrafficProfile.BURST:       {"batch_mult": 10.0, "status_weights": [0.90, 0.06, 0.03, 0.01]},
    TrafficProfile.DEGRADED:    {"batch_mult": 0.7,  "status_weights": [0.60, 0.25, 0.10, 0.05]},
    TrafficProfile.MAINTENANCE: {"batch_mult": 0.05, "status_weights": [0.50, 0.20, 0.25, 0.05]},
}


# ---------------------------------------------------------------------------
# Realistic cell tower reference data with coordinates
# ---------------------------------------------------------------------------
_TOWERS: List[Tuple[str, float, float]] = [
    # (tower_id, latitude, longitude)
    ("TOWER-NYC-001", 40.7580, -73.9855),
    ("TOWER-NYC-002", 40.7484, -73.9857),
    ("TOWER-NYC-003", 40.7614, -73.9776),
    ("TOWER-NYC-004", 40.7282, -73.7949),
    ("TOWER-LA-001",  34.0522, -118.2437),
    ("TOWER-LA-002",  34.0195, -118.4912),
    ("TOWER-LA-003",  33.9425, -118.4081),
    ("TOWER-CHI-001", 41.8781, -87.6298),
    ("TOWER-CHI-002", 41.8827, -87.6233),
    ("TOWER-MIA-001", 25.7617, -80.1918),
    ("TOWER-MIA-002", 25.7907, -80.1300),
    ("TOWER-SF-001",  37.7749, -122.4194),
    ("TOWER-SF-002",  37.8044, -122.2712),
    ("TOWER-SF-003",  37.3382, -121.8863),
    ("TOWER-SEA-001", 47.6062, -122.3321),
    ("TOWER-DEN-001", 39.7392, -104.9903),
    ("TOWER-ATL-001", 33.7490, -84.3880),
    ("TOWER-BOS-001", 42.3601, -71.0589),
    ("TOWER-DAL-001", 32.7767, -96.7970),
    ("TOWER-HOU-001", 29.7604, -95.3698),
]

# ---------------------------------------------------------------------------
# Device reference data
# ---------------------------------------------------------------------------
_DEVICE_MODELS = [
    {"make": "Apple",   "model": "iPhone 16 Pro",    "os": "iOS",     "os_version": "18.3"},
    {"make": "Apple",   "model": "iPhone 15",         "os": "iOS",     "os_version": "18.1"},
    {"make": "Samsung", "model": "Galaxy S25 Ultra",  "os": "Android", "os_version": "15"},
    {"make": "Samsung", "model": "Galaxy S24",        "os": "Android", "os_version": "14"},
    {"make": "Google",  "model": "Pixel 9 Pro",       "os": "Android", "os_version": "15"},
    {"make": "OnePlus", "model": "13",                "os": "Android", "os_version": "15"},
    {"make": "Motorola","model": "Edge 50 Ultra",     "os": "Android", "os_version": "14"},
]

_SERVICE_TAG_POOL = ["VoLTE", "VoNR", "5G-SA", "5G-NSA", "4G-LTE", "HD_VOICE",
                     "WiFi-Calling", "eSIM", "RCS", "Visual-VM"]


class CDRGenerator:
    """Generates CDR records with geography, geometry, and semi-structured data."""

    AREA_CODES = ["555", "212", "415", "310", "312", "617", "404", "305"]
    CALL_TYPES = ["VOICE", "VIDEO", "SMS", "DATA"]
    CALL_STATUSES = ["COMPLETED", "DROPPED", "FAILED", "BUSY"]
    NETWORK_TYPES = ["5G", "4G", "3G"]
    COUNTRIES = ["US", "US", "US", "CA", "MX"]

    @classmethod
    def _phone(cls) -> str:
        return f"+1-{random.choice(cls.AREA_CODES)}-{random.randint(1000, 9999)}"

    # ----- GEOGRAPHY: GeoJSON point string -----

    @staticmethod
    def _geojson_point(lat: float, lon: float) -> str:
        jitter_lat = lat + random.uniform(-0.005, 0.005)
        jitter_lon = lon + random.uniform(-0.005, 0.005)
        return json.dumps({
            "type": "Point",
            "coordinates": [round(jitter_lon, 6), round(jitter_lat, 6)],
        })

    # ----- GEOMETRY: WKT polygon (approximate hexagonal coverage) -----

    @staticmethod
    def _wkt_coverage_polygon(lat: float, lon: float, radius_deg: float = 0.01) -> str:
        points = []
        for i in range(6):
            angle = math.radians(60 * i)
            px = round(lon + radius_deg * math.cos(angle), 6)
            py = round(lat + radius_deg * math.sin(angle), 6)
            points.append(f"{px} {py}")
        points.append(points[0])
        return f"POLYGON(({', '.join(points)}))"

    # ----- Semi-structured helpers -----

    @staticmethod
    def _device_info() -> Dict:
        device = dict(random.choice(_DEVICE_MODELS))
        device["imei"] = f"{random.randint(10**14, 10**15 - 1)}"
        return device

    @staticmethod
    def _service_tags(network_type: str) -> List[str]:
        base = []
        if network_type == "5G":
            base.append(random.choice(["5G-SA", "5G-NSA"]))
            base.append("VoNR" if random.random() > 0.3 else "VoLTE")
        elif network_type == "4G":
            base.append("4G-LTE")
            base.append("VoLTE")
        else:
            base.append("VoLTE")
        extras = random.sample(
            [t for t in _SERVICE_TAG_POOL if t not in base],
            k=random.randint(0, 2),
        )
        return base + extras

    @staticmethod
    def _network_measurements(profile: TrafficProfile) -> Dict:
        degraded = profile in (TrafficProfile.DEGRADED, TrafficProfile.MAINTENANCE)
        return {
            "signal_strength_dbm": round(random.uniform(-110, -50) if not degraded
                                         else random.uniform(-130, -90), 1),
            "latency_ms":         round(random.uniform(5, 30) if not degraded
                                        else random.uniform(50, 300), 1),
            "jitter_ms":          round(random.uniform(0.5, 5) if not degraded
                                        else random.uniform(10, 50), 2),
            "packet_loss_pct":    round(random.uniform(0, 0.5) if not degraded
                                        else random.uniform(2, 15), 2),
            "throughput_mbps":    round(random.uniform(20, 500) if not degraded
                                        else random.uniform(1, 20), 1),
        }

    # ----- Main record generator -----

    @classmethod
    def generate_one(
        cls,
        profile: TrafficProfile = TrafficProfile.NORMAL,
        *,
        inject_bad_data: bool = False,
    ) -> Dict:
        cfg = _PROFILE_CONFIG[profile]

        tower_id, lat, lon = random.choice(_TOWERS)
        call_start = datetime.utcnow() - timedelta(seconds=random.randint(0, 30))
        duration = random.randint(5, 3600)
        call_end = call_start + timedelta(seconds=duration)
        call_type = random.choice(cls.CALL_TYPES)
        call_status = random.choices(cls.CALL_STATUSES, weights=cfg["status_weights"])[0]

        if call_status in ("DROPPED", "FAILED"):
            duration = random.randint(0, min(duration, 60))
            call_end = call_start + timedelta(seconds=duration)

        network_type = random.choice(cls.NETWORK_TYPES)
        data_mb = round(random.uniform(0.1, 500), 2) if call_type == "DATA" else 0.0

        record: Dict = {
            "call_id": f"CDR-{call_start:%Y%m%d%H%M%S}-{uuid.uuid4().hex[:8]}",
            "caller_number": cls._phone(),
            "callee_number": cls._phone(),
            "call_start_time": call_start.isoformat() + "Z",
            "call_end_time": call_end.isoformat() + "Z",
            "duration_seconds": duration,
            "call_type": call_type,
            "call_status": call_status,
            "cell_tower_id": tower_id,
            "network_type": network_type,
            "data_usage_mb": data_mb,
            "roaming": random.random() < 0.05,
            "country_code": random.choice(cls.COUNTRIES),

            # GEOGRAPHY — GeoJSON string (parsed by TRY_TO_GEOGRAPHY in PIPE)
            "tower_location": cls._geojson_point(lat, lon),

            # GEOMETRY — WKT string (parsed by TRY_TO_GEOMETRY in PIPE)
            "coverage_area": cls._wkt_coverage_polygon(lat, lon),

            # Semi-structured — passed as native Python types
            "device_info": cls._device_info(),
            "service_tags": cls._service_tags(network_type),
            "network_measurements": cls._network_measurements(profile),
        }

        if inject_bad_data:
            cls._corrupt_record(record)

        return record

    @classmethod
    def _corrupt_record(cls, record: Dict) -> None:
        corruption = random.choice([
            "missing_field", "wrong_type", "overflow",
            "bad_geojson", "bad_wkt",
        ])
        if corruption == "missing_field":
            del record["call_id"]
        elif corruption == "wrong_type":
            record["duration_seconds"] = "NOT_A_NUMBER"
        elif corruption == "overflow":
            record["caller_number"] = "X" * 500
        elif corruption == "bad_geojson":
            record["tower_location"] = "{invalid geojson}"
        elif corruption == "bad_wkt":
            record["coverage_area"] = "NOT_A_WKT_STRING"

    @classmethod
    def generate_batch(
        cls,
        base_size: int = 100,
        profile: TrafficProfile = TrafficProfile.NORMAL,
        *,
        bad_record_pct: float = 0.0,
    ) -> List[Dict]:
        mult = _PROFILE_CONFIG[profile]["batch_mult"]
        actual_size = max(1, int(base_size * mult))
        return [
            cls.generate_one(profile, inject_bad_data=random.random() < bad_record_pct)
            for _ in range(actual_size)
        ]

    @classmethod
    def pick_profile_for_cycle(cls, cycle: int, total_cycles: int) -> TrafficProfile:
        pct = cycle / max(total_cycles, 1)
        if pct < 0.15:
            return TrafficProfile.NORMAL
        if pct < 0.30:
            return TrafficProfile.PEAK_HOUR
        if pct < 0.40:
            return TrafficProfile.NORMAL
        if pct < 0.45:
            return TrafficProfile.BURST
        if pct < 0.55:
            return TrafficProfile.NORMAL
        if pct < 0.65:
            return TrafficProfile.DEGRADED
        if pct < 0.70:
            return TrafficProfile.MAINTENANCE
        if pct < 0.85:
            return TrafficProfile.PEAK_HOUR
        return TrafficProfile.NORMAL
