from __future__ import annotations
from .config import Instance, Route
from .utils import geo_distance

def get_dist_and_time(inst: Instance, i: str, j: str) -> tuple[float, float]:
    d = inst.distance.get(i, {}).get(j, None)
    t = inst.travel_time.get(i, {}).get(j, None)
    if d is not None and t is not None:
        return d, t

    # thử chiều ngược lại
    d2 = inst.distance.get(j, {}).get(i, None)
    t2 = inst.travel_time.get(j, {}).get(i, None)
    if d2 is not None and t2 is not None:
        return d2, t2

    # fallback geo_distance
    if i in inst.coords and j in inst.coords:
        lat1, lon1 = inst.coords[i]
        lat2, lon2 = inst.coords[j]
        d_geo = geo_distance(lat1, lon1, lat2, lon2)
        t_geo = d_geo * 3.0 # giả sử 20km/h
        return d_geo, t_geo

    return 0.0, 0.0

def check_road_allowed(inst: Instance, vid: str, i: str, j: str) -> bool:
    """Kiểm tra xe vid có được đi từ i đến j không."""
    allowed_for_vid = inst.road_allowed.get(vid, {})
    return allowed_for_vid.get(i, {}).get(j, 1) == 1

def check_capacity(inst: Instance, vid: str, stops: list[str]) -> tuple[bool, float, float]:
    """
    Kiểm tra capacity weight và volume.
    Trả về (is_valid, over_weight, over_volume)
    """
    load_w = 0.0
    load_v = 0.0
    for node in stops:
        if node in inst.customers:
            load_w += inst.demand_w[node]
            load_v += inst.demand_v[node]
    
    cap_w = inst.vehicle_cap_w[vid]
    cap_v = inst.vehicle_cap_v[vid]
    
    over_w = max(load_w - cap_w, 0.0)
    over_v = max(load_v - cap_v, 0.0)
    
    is_valid = (over_w == 0.0) and (over_v == 0.0)
    return is_valid, over_w, over_v

def calculate_route_metrics(inst: Instance, route: Route) -> dict:
    """
    Tính toán các chỉ số của route theo logic Cluster:
    Route = [Depot, c1, c2, ..., cn, Depot]
    Thực tế đi: Depot -> Centroid -> Depot
    """
    vid = route.vehicle_id
    stops = route.stops
    
    # Filter actual customers
    customer_nodes = [s for s in stops if s in inst.customers]
    
    if not customer_nodes:
        return {
            "distance": 0.0,
            "time": 0.0,
            "road_violations": 0,
            "tw_early": 0.0,
            "tw_late": 0.0,
            "overtime": 0.0,
            "load_w": 0.0,
            "load_v": 0.0
        }

    # Find Centroid
    lats = [inst.coords[c][0] for c in customer_nodes]
    lons = [inst.coords[c][1] for c in customer_nodes]
    mean_lat = sum(lats) / len(lats)
    mean_lon = sum(lons) / len(lons)
    
    best_c = None
    min_dist = float("inf")
    for c in customer_nodes:
        d = geo_distance(inst.coords[c][0], inst.coords[c][1], mean_lat, mean_lon)
        if d < min_dist:
            min_dist = d
            best_c = c
            
    depot_id = inst.depots[vid]
    
    # Distance & Time
    d_out, t_out = get_dist_and_time(inst, depot_id, best_c)
    d_in, t_in = get_dist_and_time(inst, best_c, depot_id)
    
    dist_k = d_out + d_in
    # Time: Travel out + Service (at centroid) + Travel in
    # Note: Service time logic is tricky. If one point represents all, 
    # do we sum service times or just take one?
    # User said: "1 cụm chỉ vận chuyển tới 1 điểm". 
    # Assuming we drop everything there. Let's sum service times if needed, 
    # or just use the centroid's service time. 
    # Let's assume we serve ALL customers at that location.
    # But for simplicity in "1 point" logic, let's just take the centroid's service time 
    # or a fixed drop-off time.
    # Let's stick to the centroid's service time for now to keep it simple as requested.
    t = t_out + inst.service_time[best_c] + t_in 
    
    # Road Violations
    road_violations = 0
    if not check_road_allowed(inst, vid, depot_id, best_c):
        road_violations += 1
    if not check_road_allowed(inst, vid, best_c, depot_id):
        road_violations += 1
        
    # Capacity
    load_w = sum(inst.demand_w[c] for c in customer_nodes)
    load_v = sum(inst.demand_v[c] for c in customer_nodes)
    
    # Time Window (Aggregate)
    tw_early = 0.0
    tw_late = 0.0
    arrival = t_out
    
    for c in customer_nodes:
        e_j = inst.tw_start[c]
        l_j = inst.tw_end[c]
        early = max(e_j - arrival, 0.0)
        late = max(arrival - l_j, 0.0)
        tw_early += early
        tw_late += late
            
    overtime = max(t - inst.shift_max[vid], 0.0)
    
    return {
        "distance": dist_k,
        "time": t,
        "road_violations": road_violations,
        "tw_early": tw_early,
        "tw_late": tw_late,
        "overtime": overtime,
        "load_w": load_w,
        "load_v": load_v
    }
