# utils.py
from __future__ import annotations
import math
import pandas as pd


def time_str_to_min(t: str) -> int:
    """Chuyển chuỗi 'HH:MM' (hoặc 'HH:MM-HH:MM') sang phút."""
    if pd.isna(t):
        return 0
    t = str(t).strip()
    if "-" in t:
        t = t.split("-")[0]
    h, m = t.split(":")
    return int(h) * 60 + int(m)


def parse_operating_hours(oh: str):
    """Ví dụ '08:00-17:00' -> (start_min, end_min)."""
    if pd.isna(oh):
        return 0, 24 * 60
    s, e = oh.split("-")
    return time_str_to_min(s), time_str_to_min(e)


def geo_distance(lat1, lon1, lat2, lon2):
    """
    Khoảng cách địa lý xấp xỉ (km) giữa 2 điểm (lat, lon).
    Hàm này luôn cố gắng trả về một số hữu hạn, không NaN/Inf.
    """
    vals = (lat1, lon1, lat2, lon2)
    for v in vals:
        if v is None:
            return 0.0
        if isinstance(v, float) and math.isnan(v):
            return 0.0

    dx = (lon2 - lon1) * math.cos((lat1 + lat2) * math.pi / 360)
    dy = (lat2 - lat1)
    d = math.sqrt(dx * dx + dy * dy) * 111

    if isinstance(d, float) and (math.isnan(d) or math.isinf(d)):
        return 0.0
    return d
