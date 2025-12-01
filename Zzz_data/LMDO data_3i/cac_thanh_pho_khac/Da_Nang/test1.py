# ============================================================
# LAST-MILE DELIVERY – FULL MODEL + ALNS + TABU (HỒ CHÍ MINH)
# ============================================================
# Bản này giữ nguyên toàn bộ mô hình và thuật toán từ code gốc của bạn.
# Chỉ thay hàm load_data() để phù hợp folder:
#
#   Zzz_data/LMDO processed/Ho_Chi_Minh_City/
#       customers.xlsx
#       depots.xlsx
#       vehicles.xlsx
#       roads.xlsx
#       hcm.py
# ============================================================

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Set, Tuple, Callable, Any
from collections import defaultdict
import pandas as pd
import math
import random
import os
import datetime 
import matplotlib.pyplot as plt


# ============================================================
# 1. DATA STRUCTURES
# ============================================================

@dataclass
class Instance:
    customers: Set[str]
    vehicles: List[str]
    depots: Dict[str, str]               # vehicle_id -> depot_id
    depot_capacity: Dict[str, float]     # storage cap

    distance: Dict[str, Dict[str, float]]
    travel_time: Dict[str, Dict[str, float]]
    road_allowed: Dict[str, Dict[str, Dict[str, int]]]

    demand_w: Dict[str, float]
    demand_v: Dict[str, float]
    service_time: Dict[str, float]
    tw_start: Dict[str, float]
    tw_end: Dict[str, float]
    priority: Dict[str, int]
    delivery_type: Dict[str, str]
    coords: Dict[str, Tuple[float, float]]
    customer_cluster: Dict[str, str]

    vehicle_cap_w: Dict[str, float]
    vehicle_cap_v: Dict[str, float]
    shift_max: Dict[str, float]
    max_distance: Dict[str, float]
    fixed_cost: Dict[str, float]
    var_cost: Dict[str, float]

    penalty_unserved: Dict[str, float]
    lambda_E: Dict[str, float]
    lambda_L: Dict[str, float]
    lambda_H: Dict[str, float]

    lambda_W: float
    lambda_dist_overtime: float
    lambda_depot_capacity: float

    BIG_CAP: float = 1e6
    BIG_ROAD: float = 1e6


@dataclass
class Route:
    vehicle_id: str
    stops: List[str]   # [depot, c1, c2, ..., depot]

    def copy(self) -> "Route":
        return Route(vehicle_id=self.vehicle_id, stops=list(self.stops))


@dataclass
class Solution:
    routes: Dict[str, Route]
    all_customers: Set[str]
    objective: float = math.inf
    meta: Dict[str, Any] = field(default_factory=dict)

    def copy(self) -> "Solution":
        return Solution(
            routes={k: r.copy() for k, r in self.routes.items()},
            all_customers=set(self.all_customers),
            objective=self.objective,
            meta={k: v for k, v in self.meta.items()},
        )

# ============================================================
# 2. HELPERS
# ============================================================

def time_str_to_min(t: str) -> int:
    if pd.isna(t):
        return 0
    t = str(t).strip()
    if "-" in t:
        t = t.split("-")[0]
    h, m = t.split(":")
    return int(h) * 60 + int(m)


def parse_operating_hours(oh: str):
    if pd.isna(oh):
        return 0, 24 * 60
    s, e = oh.split("-")
    return time_str_to_min(s), time_str_to_min(e)


def geo_distance(lat1, lon1, lat2, lon2):
    # Nếu bất kỳ toạ độ nào bị None/NaN -> trả về 0 để không làm hỏng delta
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


# ============================================================
# 3. BUILD INSTANCE FOR D003  – MULTI-TRIP + AUTO TÍNH SỐ TRIP
# ============================================================

def build_instance_for_depot_prefix(
    depot_prefix: str,
    customers_df: pd.DataFrame,
    depots_df: pd.DataFrame,
    vehicles_df: pd.DataFrame,
    roads_df: pd.DataFrame,
) -> Instance:

    # ========================================================
    # 0. CHUẨN BỊ CUSTOMERS TRƯỚC ĐỂ TÍNH TỔNG DEMAND
    # ========================================================
    all_customer_ids = set(customers_df["Customer_ID"].unique())
    customers_in_instance = all_customer_ids
    cust_sub = customers_df[customers_df["Customer_ID"].isin(customers_in_instance)].copy()

    demand_w = {}
    demand_v = {}
    service_time = {}
    tw_start = {}
    tw_end = {}
    priority_map = {}
    delivery_type = {}
    coords = {}

    total_demand_w = 0.0

    for _, r in cust_sub.iterrows():
        cid = r["Customer_ID"]
        w = float(r["Order_Weight"])
        v = float(r["Order_Volume"])

        demand_w[cid] = w
        demand_v[cid] = v
        service_time[cid] = float(r["Service_Time"])
        tw_start[cid] = float(time_str_to_min(r["Time_Window_Start"]))
        tw_end[cid] = float(time_str_to_min(r["Time_Window_End"]))
        priority_map[cid] = int(r["Priority_Level"])
        delivery_type[cid] = str(r["Delivery_Type"])
        coords[cid] = (float(r["Latitude"]), float(r["Longitude"]))

        total_demand_w += w

    print(f"[INFO] Tổng demand_weight khách hàng = {total_demand_w:.2f}")

    # ========================================================
    # 1. VEHICLES GỐC (chỉ depot_prefix này)
    # ========================================================
    veh_raw = vehicles_df[vehicles_df["Start_Depot_ID"].str.startswith(depot_prefix)].copy()
    if veh_raw.empty:
        raise ValueError(f"Không có xe cho prefix {depot_prefix}")

    total_cap_w_one_trip = float(veh_raw["Capacity_Weight"].sum())
    print(f"[INFO] Tổng capacity_weight đội xe (1 trip) = {total_cap_w_one_trip:.2f}")

    # Nếu tổng demand > tổng capacity 1 trip → cần nhiều trip
    # Hệ số an toàn 1.1 để tránh bị sát ngưỡng
    if total_cap_w_one_trip <= 0:
        trips_needed = 1
    else:
        ratio = total_demand_w / (total_cap_w_one_trip * 1.1)
        trips_needed = max(1, math.ceil(ratio))

    # Không để trips quá kinh khủng để tránh nổ runtime
    TRIPS_PER_VEH = min(trips_needed, 6)

    print(f"[INFO] Chọn TRIPS_PER_VEH = {TRIPS_PER_VEH} (ước tính từ demand/capacity)")

    # ========================================================
    # 2. NHÂN MỖI XE THÀNH NHIỀU TRIP ẢO
    # ========================================================
    veh_rows = []
    for _, row in veh_raw.iterrows():
        base_id = row["Vehicle_ID"]
        base_fixed = float(row["Fixed_Cost"])

        for k in range(1, TRIPS_PER_VEH + 1):
            new_row = row.copy()
            new_row["Vehicle_ID"] = f"{base_id}_T{k}"
            # chia đều fixed cost cho các trip, tổng fixed ≈ như cũ
            new_row["Fixed_Cost"] = base_fixed / TRIPS_PER_VEH
            veh_rows.append(new_row)

    veh_sub = pd.DataFrame(veh_rows)
    vehicle_ids = veh_sub["Vehicle_ID"].tolist()

    depots_map = {row["Vehicle_ID"]: row["Start_Depot_ID"] for _, row in veh_sub.iterrows()}
    max_distance = {row["Vehicle_ID"]: float(row["Max_Distance"]) for _, row in veh_sub.iterrows()}

    # ========================================================
    # 3. ROADS
    # ========================================================
    roads_sub = roads_df.copy()
    if roads_sub.empty:
        raise ValueError(f"Không có roads cho prefix {depot_prefix}")

    print(f"[INFO] build_instance_for_depot_prefix: dùng {len(customers_in_instance)} customers cho {depot_prefix}")

    # ========================================================
    # 4. DEPOT PARAMS + COORDS
    # ========================================================
    depots_sub = depots_df[depots_df["Depot_ID"].str.startswith(depot_prefix)].copy()
    depot_capacity = {r["Depot_ID"]: float(r["Capacity_Storage"]) for _, r in depots_sub.iterrows()}

    depot_coords = {}
    for _, r in depots_sub.iterrows():
        d_id = r["Depot_ID"]
        lat = float(r["Latitude"])
        lon = float(r["Longitude"])
        depot_coords[d_id] = (lat, lon)
        coords[d_id] = (lat, lon)

    # ========================================================
    # 5. VEHICLE PARAMS (CHO VEHICLE ẢO)
    # ========================================================
    vehicle_cap_w = {r["Vehicle_ID"]: float(r["Capacity_Weight"]) for _, r in veh_sub.iterrows()}
    vehicle_cap_v = {r["Vehicle_ID"]: float(r["Capacity_Volume"]) for _, r in veh_sub.iterrows()}
    fixed_cost     = {r["Vehicle_ID"]: float(r["Fixed_Cost"]) for _, r in veh_sub.iterrows()}
    var_cost       = {r["Vehicle_ID"]: float(r["Variable_Cost"]) for _, r in veh_sub.iterrows()}
    shift_max      = {r["Vehicle_ID"]: float(r["Max_Working_Hours"]) * 60 for _, r in veh_sub.iterrows()}
    vehicle_type   = {r["Vehicle_ID"]: str(r["Vehicle_Type"]) for _, r in veh_sub.iterrows()}

    # ========================================================
    # 6. DISTANCE / TIME MATRIX
    # ========================================================
    distance = defaultdict(dict)
    travel_time = defaultdict(dict)
    for _, r in roads_sub.iterrows():
        i = r["Origin_Node_ID"]
        j = r["Destination_Node_ID"]
        distance[i][j] = float(r["Distance_km"])
        travel_time[i][j] = float(r["Travel_Time_min"])

    # ========================================================
    # 7. ROAD RESTRICTIONS
    # ========================================================
    HEAVY = {"Truck", "Van", "Heavy Truck"}
    roads_sub["Road_Restrictions"] = roads_sub["Road_Restrictions"].fillna("None").astype(str)

    road_allowed = {vid: defaultdict(dict) for vid in vehicle_ids}
    for _, r in roads_sub.iterrows():
        i = r["Origin_Node_ID"]
        j = r["Destination_Node_ID"]
        rest = r["Road_Restrictions"]

        for vid in vehicle_ids:
            allow = 1
            if rest == "No Heavy Trucks" and vehicle_type[vid] in HEAVY:
                allow = 0
            road_allowed[vid][i][j] = allow

    # ========================================================
    # 8. CLUSTERING (gán depot gần nhất cho từng khách)
    # ========================================================
    customer_cluster = {}
    for cid in customers_in_instance:
        clat, clon = coords[cid]
        best_d, best_dis = None, float("inf")
        for did, (dlat, dlon) in depot_coords.items():
            d = geo_distance(clat, clon, dlat, dlon)
            if d < best_dis:
                best_dis = d
                best_d = did
        customer_cluster[cid] = best_d

    # ========================================================
    # 9. PENALTIES / WEIGHTS
    # ========================================================
    penalty_unserved: Dict[str, float] = {}
    lambda_E: Dict[str, float] = {}
    lambda_L: Dict[str, float] = {}

    for cid in customers_in_instance:
        phi = priority_map[cid]            # 1,2,3 (priority)
        w_i = max(demand_w[cid], 1.0)
        penalty_unserved[cid] = 2000 * phi * w_i
        lambda_E[cid] = 0.05 * phi
        lambda_L[cid] = 1.0 * phi

    lambda_H = {vid: 0.05 * fixed_cost[vid] for vid in vehicle_ids}
    lambda_W = 5e-4
    lambda_dist_overtime = 2.0
    lambda_depot_capacity = 0.5

    BIG_CAP = 1e6
    BIG_ROAD = 5e3

    return Instance(
        customers=customers_in_instance,
        vehicles=vehicle_ids,
        depots=depots_map,
        depot_capacity=depot_capacity,
        distance=dict(distance),
        travel_time=dict(travel_time),
        road_allowed=road_allowed,
        demand_w=demand_w,
        demand_v=demand_v,
        service_time=service_time,
        tw_start=tw_start,
        tw_end=tw_end,
        priority=priority_map,
        delivery_type=delivery_type,
        coords=coords,
        customer_cluster=customer_cluster,
        vehicle_cap_w=vehicle_cap_w,
        vehicle_cap_v=vehicle_cap_v,
        shift_max=shift_max,
        max_distance=max_distance,
        fixed_cost=fixed_cost,
        var_cost=var_cost,
        penalty_unserved=penalty_unserved,
        lambda_E=lambda_E,
        lambda_L=lambda_L,
        lambda_H=lambda_H,
        lambda_W=lambda_W,
        lambda_dist_overtime=lambda_dist_overtime,
        lambda_depot_capacity=lambda_depot_capacity,
        BIG_CAP=BIG_CAP,
        BIG_ROAD=BIG_ROAD,
    )



# ============================================================
# 4. EVALUATE (FULL MODEL) – CAPACITY HARD
# ============================================================

def evaluate(
    sol: Solution,
    inst: Instance,
    debug: bool = False,
    max_print_violations: int = 30,
) -> float:
    """
    Hàm mục tiêu mở rộng.
    - Capacity KHÔNG còn là ràng buộc hard (không trả INF),
      mà được enforce chủ yếu ở repair + Tabu neighbors.
    - Ở đây vẫn tính capacity_pen để dễ debug nếu còn nghiệm overload.
    """

    total_fixed = 0.0
    total_dist_cost = 0.0
    total_unserved_pen = 0.0
    total_tw_pen = 0.0
    total_overtime_pen = 0.0
    total_cap_pen = 0.0
    total_road_pen = 0.0
    total_dist_over_pen = 0.0
    total_depot_cap_pen = 0.0
    total_workload_pen = 0.0

    visited: Set[str] = set()
    W: Dict[str, float] = {}
    depot_load: Dict[str, float] = defaultdict(float)

    cap_violations = []
    tw_violations = []
    road_violations = []
    overtime_violations = []
    dist_over_violations = []
    depot_violations = []

    # ============================================================
    # 1. TÍNH TRÊN MỖI ROUTE
    # ============================================================
    for vid, route in sol.routes.items():
        stops = route.stops
        if len(stops) <= 1:
            W[vid] = 0.0
            continue

        total_fixed += inst.fixed_cost[vid]

        load_w = 0.0
        load_v = 0.0
        t = 0.0
        dist_k = 0.0

        depot_id = inst.depots[vid]
        allowed_for_vid = inst.road_allowed.get(vid, {})

        def get_dist_and_time(inst_: Instance, i: str, j: str) -> tuple[float, float]:
            d = inst_.distance.get(i, {}).get(j, None)
            t_ = inst_.travel_time.get(i, {}).get(j, None)
            if d is not None and t_ is not None:
                return d, t_

            d2 = inst_.distance.get(j, {}).get(i, None)
            t2 = inst_.travel_time.get(j, {}).get(i, None)
            if d2 is not None and t2 is not None:
                return d2, t2

            if i in inst_.coords and j in inst_.coords:
                lat1, lon1 = inst_.coords[i]
                lat2, lon2 = inst_.coords[j]
                d_geo = geo_distance(lat1, lon1, lat2, lon2)
                t_geo = d_geo * 3.0  # 20km/h ≈ 3 phút/km
                return d_geo, t_geo

            return 0.0, 0.0

        for i, j in zip(stops[:-1], stops[1:]):
            allow_ij = allowed_for_vid.get(i, {}).get(j, 1)
            if allow_ij == 0:
                total_road_pen += inst.BIG_ROAD
                road_violations.append((vid, i, j))
                # không return, vẫn tính tiếp

            d_ij, t_ij = get_dist_and_time(inst, i, j)
            dist_k += d_ij
            t += t_ij

            if j in inst.customers:
                load_w += inst.demand_w[j]
                load_v += inst.demand_v[j]

                # Capacity: ở đây chỉ phạt & log, không trả INF nữa
                if load_w > inst.vehicle_cap_w[vid] or load_v > inst.vehicle_cap_v[vid]:
                    over_w = max(load_w - inst.vehicle_cap_w[vid], 0.0)
                    over_v = max(load_v - inst.vehicle_cap_v[vid], 0.0)
                    total_cap_pen += (
                        inst.BIG_CAP
                        * (over_w / max(inst.vehicle_cap_w[vid], 1.0)
                           + over_v / max(inst.vehicle_cap_v[vid], 1.0))
                    )
                    cap_violations.append(
                        (vid, j, load_w, inst.vehicle_cap_w[vid],
                         load_v, inst.vehicle_cap_v[vid])
                    )

                arrival = t
                e_j = inst.tw_start[j]
                l_j = inst.tw_end[j]
                early = max(e_j - arrival, 0.0)
                late = max(arrival - l_j, 0.0)

                if early > 0 or late > 0:
                    tw_violations.append((j, arrival, e_j, l_j, early, late))

                total_tw_pen += inst.lambda_E[j] * early + inst.lambda_L[j] * late

                start_service = max(arrival, e_j)
                t = start_service + inst.service_time[j]

                visited.add(j)
                depot_load[depot_id] += inst.demand_w[j]

        W[vid] = dist_k
        total_dist_cost += inst.var_cost[vid] * dist_k

        overtime = max(t - inst.shift_max[vid], 0.0)
        if overtime > 0:
            total_overtime_pen += inst.lambda_H[vid] * overtime
            overtime_violations.append((vid, t, inst.shift_max[vid]))

        if dist_k > inst.max_distance[vid]:
            extra = dist_k - inst.max_distance[vid]
            total_dist_over_pen += inst.lambda_dist_overtime * extra
            dist_over_violations.append((vid, dist_k, inst.max_distance[vid]))

    # ============================================================
    # 2. KHÁCH KHÔNG ĐƯỢC PHỤC VỤ
    # ============================================================
    for cid in inst.customers:
        if cid not in visited:
            total_unserved_pen += inst.penalty_unserved[cid]

    # ============================================================
    # 3. DEPOT CAPACITY
    # ============================================================
    for d_id, load in depot_load.items():
        cap = inst.depot_capacity.get(d_id, float("inf"))
        if load > cap:
            over = load - cap
            total_depot_cap_pen += inst.lambda_depot_capacity * over
            depot_violations.append((d_id, load, cap))

    # ============================================================
    # 4. WORKLOAD BALANCING
    # ============================================================
    if W:
        avgW = sum(W.values()) / len(W)
        for vid in W:
            total_workload_pen += inst.lambda_W * (W[vid] - avgW) ** 2
    else:
        avgW = 0.0

    # ============================================================
    # 5. OBJECTIVE
    # ============================================================
    F = (
        total_fixed
        + total_dist_cost
        + total_unserved_pen
        + total_tw_pen
        + total_overtime_pen
        + total_cap_pen
        + total_road_pen
        + total_dist_over_pen
        + total_depot_cap_pen
        + total_workload_pen
    )

    sol.objective = F
    sol.meta = {
        "visited": visited,
        "W": W,
        "avgW": avgW,
        "depot_load": depot_load,
        "components": {
            "fixed": total_fixed,
            "distance_cost": total_dist_cost,
            "unserved_pen": total_unserved_pen,
            "tw_pen": total_tw_pen,
            "overtime_pen": total_overtime_pen,
            "capacity_pen": total_cap_pen,
            "road_pen": total_road_pen,
            "dist_over_pen": total_dist_over_pen,
            "depot_cap_pen": total_depot_cap_pen,
            "workload_pen": total_workload_pen,
        },
        "violations": {
            "capacity": cap_violations,
            "time_window": tw_violations,
            "road": road_violations,
            "overtime": overtime_violations,
            "distance_over": dist_over_violations,
            "depot_capacity": depot_violations,
        },
    }

    if debug:
        comps = sol.meta["components"]
        viols = sol.meta["violations"]

        print("\n===== DEBUG EVALUATE =====")
        print(f"Objective F = {F:.2f}")
        print("---- Components ----")
        for k, v in comps.items():
            print(f"  {k:15s}: {v:.2f}")

        print("---- Violations summary ----")
        print(f"  #capacity       = {len(viols['capacity'])}")
        print(f"  #time_window    = {len(viols['time_window'])}")
        print(f"  #road           = {len(viols['road'])}")
        print(f"  #overtime       = {len(viols['overtime'])}")
        print(f"  #dist_over      = {len(viols['distance_over'])}")
        print(f"  #depot_capacity = {len(viols['depot_capacity'])}")
        print("===== END DEBUG EVALUATE =====\n")

    return F

def get_total_distance_km(sol: Solution) -> float:
    """
    Tổng quãng đường (km) của tất cả vehicle trong nghiệm.
    Gọi evaluate trước để đảm bảo meta đã cập nhật.
    """
    W = sol.meta.get("W", {})
    return sum(W.values())


# ============================================================
# 5. ALNS: DESTROY / REPAIR OPERATORS
# ============================================================

DestroyOp = Callable[[Solution, Instance, random.Random], Solution]
RepairOp = Callable[[Solution, Instance, random.Random], Solution]

@dataclass
class OperatorState:
    name: str
    weight: float = 1.0
    score: float = 0.0
    times_used: int = 0

def roulette_select(ops: List[OperatorState], rng: random.Random) -> int:
    total_w = sum(max(op.weight, 1e-6) for op in ops)
    r = rng.random() * total_w
    s = 0.0
    for i, op in enumerate(ops):
        s += max(op.weight, 1e-6)
        if s >= r:
            return i
    return len(ops) - 1

# DESTROY OPERATORS (giữ nguyên như bạn)

def _fix_route_roundtrip(route: Route):
    if not route.stops:
        return
    depot = route.stops[0]
    if route.stops[-1] != depot:
        route.stops.append(depot)
    if len(route.stops) == 1:
        route.stops.append(depot)

def destroy_random(sol: Solution, inst: Instance, rng: random.Random, remove_ratio=0.1) -> Solution:
    new_sol = sol.copy()
    allc = list(inst.customers)
    rng.shuffle(allc)
    n_remove = max(1, int(len(allc) * remove_ratio))
    to_remove = set(allc[:n_remove])

    for r in new_sol.routes.values():
        if not r.stops:
            continue
        depot = r.stops[0]
        new_stops = [x for x in r.stops if (x not in to_remove or x == depot)]
        if not new_stops:
            new_stops = [depot, depot]
        elif len(new_stops) == 1:
            new_stops.append(depot)
        r.stops = new_stops
        _fix_route_roundtrip(r)
    return new_sol

def destroy_cluster(sol: Solution, inst: Instance, rng: random.Random) -> Solution:
    new_sol = sol.copy()
    clusters = set(inst.customer_cluster.values())
    if not clusters:
        return new_sol
    chosen_cluster = rng.choice(list(clusters))

    to_remove = {cid for cid, cl in inst.customer_cluster.items() if cl == chosen_cluster}

    for r in new_sol.routes.values():
        if not r.stops:
            continue
        depot = r.stops[0]
        new_stops = [x for x in r.stops if (x not in to_remove or x == depot)]
        if not new_stops:
            new_stops = [depot, depot]
        elif len(new_stops) == 1:
            new_stops.append(depot)
        r.stops = new_stops
        _fix_route_roundtrip(r)

    return new_sol

def destroy_shaw_related(sol: Solution, inst: Instance, rng: random.Random, remove_count: int = 20) -> Solution:
    new_sol = sol.copy()
    allc = list(inst.customers)
    if not allc:
        return new_sol
    rng.shuffle(allc)
    seed = allc[0]

    def relatedness(i, j):
        lat_i, lon_i = inst.coords[i]
        lat_j, lon_j = inst.coords[j]
        d_geo = geo_distance(lat_i, lon_i, lat_j, lon_j)
        tw_diff = abs(inst.tw_start[i] - inst.tw_start[j]) + abs(inst.tw_end[i] - inst.tw_end[j])
        pr_diff = abs(inst.priority[i] - inst.priority[j])
        return d_geo + 0.01 * tw_diff + 5.0 * pr_diff

    remaining = set(inst.customers)
    to_remove = [seed]
    remaining.remove(seed)

    target_remove = min(remove_count, len(inst.customers))
    while len(to_remove) < target_remove and remaining:
        last = rng.choice(to_remove)
        best_j = min(remaining, key=lambda j: relatedness(last, j))
        to_remove.append(best_j)
        remaining.remove(best_j)

    to_remove = set(to_remove)

    for r in new_sol.routes.values():
        if not r.stops:
            continue
        depot = r.stops[0]
        new_stops = [x for x in r.stops if (x not in to_remove or x == depot)]
        if not new_stops:
            new_stops = [depot, depot]
        elif len(new_stops) == 1:
            new_stops.append(depot)
        r.stops = new_stops
        _fix_route_roundtrip(r)

    return new_sol


# ---------- REPAIR OPERATORS ----------

def insertion_cost_distance_only(route: Route, vid: str, cid: str, pos: int, inst: Instance) -> float:
    """
    Ước lượng chi phí chèn cid vào route.stops tại vị trí pos (chỉ theo distance).
    Route: [n0, n1, ..., nk]
    Chèn giữa stops[pos-1] -> cid -> stops[pos].

    - Không bao giờ trả về NaN (nếu thiếu dữ liệu roads thì dùng geo_distance,
      nếu vẫn có vấn đề thì trả 0.0).
    """
    stops = route.stops
    if not stops:
        return float("inf")

    i = stops[pos - 1]
    j = stops[pos] if pos < len(stops) else None
    dist = inst.distance

    def get_dist(a: str, b: str) -> float:
        # 1) thử trong ma trận roads
        d = dist.get(a, {}).get(b, None)
        if d is None:
            d = dist.get(b, {}).get(a, None)

        # 2) nếu không có trong roads -> dùng geo_distance nếu có toạ độ
        if d is None:
            if a in inst.coords and b in inst.coords:
                lat1, lon1 = inst.coords[a]
                lat2, lon2 = inst.coords[b]
                d = geo_distance(lat1, lon1, lat2, lon2)
            else:
                d = 0.0  # không có info gì thì tạm cho 0

        # 3) chống NaN / Inf
        if isinstance(d, float) and (math.isnan(d) or math.isinf(d)):
            return 0.0
        return float(d)

    d_ic = get_dist(i, cid)
    d_cj = 0.0
    if j is not None:
        d_cj = get_dist(cid, j)

    d_old = 0.0
    if j is not None:
        d_old = get_dist(i, j)

    d_new = d_ic + d_cj
    return d_new - d_old


def repair_greedy(sol: Solution, inst: Instance, rng: random.Random) -> Solution:
    """
    Greedy insertion KHÔNG chặn capacity cứng:
    - Cứ chèn ở chỗ tăng distance ít nhất.
    - Vi phạm capacity sẽ bị phạt trong evaluate() qua BIG_CAP.
    """
    new_sol = sol.copy()
    evaluate(new_sol, inst)
    served = new_sol.meta.get("visited", set())
    unserved = list(inst.customers - served)
    rng.shuffle(unserved)

    MAX_INSERT = 2000
    unserved = unserved[:MAX_INSERT]

    print(f"[repair_greedy] #unserved input = {len(unserved)}")
    print("[repair_greedy] len(inst.customers) =", len(inst.customers),
          ", len(served) =", len(served),
          ", len(unserved) =", len(unserved))

    # Tính tổng tải (w,v) trên từng route hiện tại
    route_load_w = {}
    route_load_v = {}
    for vid, route in new_sol.routes.items():
        w = 0.0
        v = 0.0
        for node in route.stops:
            if node in inst.customers:
                w += inst.demand_w[node]
                v += inst.demand_v[node]
        route_load_w[vid] = w
        route_load_v[vid] = v

    inserted = 0

    for cid in unserved:
        demand_w_c = inst.demand_w[cid]
        demand_v_c = inst.demand_v[cid]

        best_delta = float("inf")
        best_vid = None
        best_pos = None

        for vid, route in new_sol.routes.items():
            # ❌ BỎ 2 CHECK CAPACITY Ở ĐÂY
            # if route_load_w[vid] + demand_w_c > inst.vehicle_cap_w[vid]:
            #     continue
            # if route_load_v[vid] + demand_v_c > inst.vehicle_cap_v[vid]:
            #     continue

            if len(route.stops) == 0:
                continue
            if len(route.stops) == 1:
                depot = route.stops[0]
                route.stops.append(depot)

            for pos in range(1, len(route.stops)):  # không chèn trước depot đầu
                delta = insertion_cost_distance_only(route, vid, cid, pos, inst)
                if delta < best_delta:
                    best_delta = delta
                    best_vid = vid
                    best_pos = pos

        if best_vid is not None and best_pos is not None and best_delta < float("inf"):
            new_sol.routes[best_vid].stops.insert(best_pos, cid)
            # cập nhật lại tải cho route đó (để sau này nếu muốn dùng info này)
            route_load_w[best_vid] += demand_w_c
            route_load_v[best_vid] += demand_v_c
            inserted += 1

    print(f"[repair_greedy] inserted = {inserted}")
    return new_sol

def repair_greedy_feasible(sol: Solution, inst: Instance, rng: random.Random) -> Solution:
    """
    Greedy insertion CÓ kiểm tra capacity cứng:
    - Chèn khách vào chỗ tăng distance ít nhất.
    - Chỉ chèn nếu tổng tải (w, v) trên route + khách mới
      không vượt capacity xe đó.
    - Hạn chế số khách chèn mỗi lần để tránh quá chậm.
    """
    new_sol = sol.copy()
    evaluate(new_sol, inst)
    served = new_sol.meta.get("visited", set())
    unserved = list(inst.customers - served)
    rng.shuffle(unserved)

    # Có thể giới hạn số khách chèn mỗi lần để tránh quá nặng
    MAX_INSERT = 2000
    unserved = unserved[:MAX_INSERT]

    # Tính tổng tải (w,v) trên từng route hiện tại
    route_load_w = {}
    route_load_v = {}
    for vid, route in new_sol.routes.items():
        w = 0.0
        v = 0.0
        for node in route.stops:
            if node in inst.customers:
                w += inst.demand_w[node]
                v += inst.demand_v[node]
        route_load_w[vid] = w
        route_load_v[vid] = v

    inserted = 0

    for cid in unserved:
        demand_w_c = inst.demand_w[cid]
        demand_v_c = inst.demand_v[cid]

        best_delta = float("inf")
        best_vid = None
        best_pos = None

        for vid, route in new_sol.routes.items():
            # ✅ CHECK CAPACITY CỨNG
            if route_load_w[vid] + demand_w_c > inst.vehicle_cap_w[vid]:
                continue
            if route_load_v[vid] + demand_v_c > inst.vehicle_cap_v[vid]:
                continue

            if len(route.stops) == 0:
                continue
            if len(route.stops) == 1:
                depot = route.stops[0]
                route.stops.append(depot)

            # Chỉ chèn sau depot đầu (pos >= 1)
            for pos in range(1, len(route.stops)):
                delta = insertion_cost_distance_only(route, vid, cid, pos, inst)
                if delta < best_delta:
                    best_delta = delta
                    best_vid = vid
                    best_pos = pos

        if best_vid is not None and best_pos is not None and best_delta < float("inf"):
            new_sol.routes[best_vid].stops.insert(best_pos, cid)
            route_load_w[best_vid] += demand_w_c
            route_load_v[best_vid] += demand_v_c
            inserted += 1

    print(f"[repair_greedy_feasible] inserted = {inserted}")
    return new_sol

def repair_regret(
    sol: Solution,
    inst: Instance,
    rng: random.Random,
    k_regret: int = 2
) -> Solution:
    """
    Regret-k insertion CÓ kiểm tra capacity cứng:
    - Với mỗi khách chưa phục vụ, xem các vị trí chèn hợp lệ (không vượt capacity)
      trên tất cả route.
    - Tính 'regret' = (chi phí chèn tốt thứ k - tốt nhất).
    - Chọn khách có regret lớn nhất để chèn vào vị trí tốt nhất của nó.
    """
    new_sol = sol.copy()
    evaluate(new_sol, inst)
    served = new_sol.meta.get("visited", set())
    unserved = list(inst.customers - served)
    rng.shuffle(unserved)
    print("[repair_regret] start, #unserved =", len(unserved))

    # Giới hạn số khách để tránh quá chậm
    MAX_INSERT = 200
    unserved = unserved[:MAX_INSERT]

    # Tính tải ban đầu trên mỗi route
    route_load_w: Dict[str, float] = {}
    route_load_v: Dict[str, float] = {}
    for vid, route in new_sol.routes.items():
        w = 0.0
        v = 0.0
        for node in route.stops:
            if node in inst.customers:
                w += inst.demand_w[node]
                v += inst.demand_v[node]
        route_load_w[vid] = w
        route_load_v[vid] = v

    inserted_global = 0

    while unserved:
        best_cid = None
        best_delta_for_cid = None
        best_regret = -1.0

        # Tìm khách tiếp theo để chèn (theo tiêu chí regret tối đa)
        for cid in list(unserved):
            demand_w_c = inst.demand_w[cid]
            demand_v_c = inst.demand_v[cid]

            insertion_candidates: List[Tuple[float, str, int]] = []

            for vid, route in new_sol.routes.items():
                # ✅ CHECK CAPACITY CỨNG CHO XE VID
                if route_load_w[vid] + demand_w_c > inst.vehicle_cap_w[vid]:
                    continue
                if route_load_v[vid] + demand_v_c > inst.vehicle_cap_v[vid]:
                    continue

                if len(route.stops) == 0:
                    continue
                if len(route.stops) == 1:
                    depot = route.stops[0]
                    route.stops.append(depot)

                # duyệt mọi vị trí chèn hợp lệ trên route này
                for pos in range(1, len(route.stops)):
                    delta = insertion_cost_distance_only(route, vid, cid, pos, inst)
                    if delta < float("inf"):
                        insertion_candidates.append((delta, vid, pos))

            if not insertion_candidates:
                # khách này tạm thời không chèn được vào route nào -> bỏ qua
                continue

            insertion_candidates.sort(key=lambda x: x[0])
            best = insertion_candidates[0][0]

            if len(insertion_candidates) >= k_regret:
                second_best = insertion_candidates[k_regret - 1][0]
            else:
                second_best = insertion_candidates[-1][0]

            regret = second_best - best

            if regret > best_regret:
                best_regret = regret
                best_cid = cid
                best_delta_for_cid = insertion_candidates[0]  # (best_delta, vid, pos)

        if best_cid is None or best_delta_for_cid is None:
            # Không còn khách nào có vị trí chèn hợp lệ
            break

        delta, vid, pos = best_delta_for_cid
        new_sol.routes[vid].stops.insert(pos, best_cid)
        route_load_w[vid] += inst.demand_w[best_cid]
        route_load_v[vid] += inst.demand_v[best_cid]
        unserved.remove(best_cid)
        inserted_global += 1

    print(f"[repair_regret] inserted = {inserted_global}")
    return new_sol

def repair_capacity_feasibilize(
    sol: Solution,
    inst: Instance,
    rng: random.Random,
) -> Solution:
    """
    Làm sạch nghiệm: bỏ bớt khách trên từng route cho tới khi
    KHÔNG còn vượt capacity (weight/volume).
    - Luôn ưu tiên GIỮ các khách có penalty_unserved lớn (priority cao, nặng).
    - Khách bị bỏ ra => trở thành unserved, bị phạt trong evaluate().
    """
    new_sol = sol.copy()

    for vid, route in new_sol.routes.items():
        cap_w = inst.vehicle_cap_w[vid]
        cap_v = inst.vehicle_cap_v[vid]

        # lấy danh sách customer trên route (bỏ depot)
        def get_customers_on_route():
            return [c for c in route.stops if c in inst.customers]

        while True:
            custs = get_customers_on_route()
            if not custs:
                break

            total_w = sum(inst.demand_w[c] for c in custs)
            total_v = sum(inst.demand_v[c] for c in custs)

            # nếu đã nằm trong capacity thì dừng
            if total_w <= cap_w and total_v <= cap_v:
                break

            # chọn khách "rẻ nhất để bỏ" = penalty_unserved nhỏ nhất trên route
            c_drop = min(custs, key=lambda c: inst.penalty_unserved[c])

            # xóa khách đó khỏi route (mọi lần xuất hiện, thường chỉ 1)
            route.stops = [node for node in route.stops if node != c_drop]

            # đảm bảo đầu/cuối là depot
            _fix_route_roundtrip(route)

    return new_sol


# ============================================================
# 6. ALNS MAIN LOOP – CHỈ DÙNG REPAIR CÓ KIỂM TRA CAPACITY
# ============================================================

def alns(
    inst: Instance,
    initial_solution: Solution,
    destroy_ops: Dict[str, DestroyOp],
    repair_ops: Dict[str, RepairOp],
    max_iter: int = 50,
    segment_length: int = 30,
    reaction_factor: float = 0.2,
    start_temperature: float = 1000.0,
    end_temperature: float = 1.0,
    rng_seed: int = 0,
) -> Solution:
    rng = random.Random(rng_seed)

    destroy_states = [OperatorState(name) for name in destroy_ops]
    repair_states = [OperatorState(name) for name in repair_ops]

    current = initial_solution.copy()
    evaluate(current, inst)
    best = current.copy()

    temperature = start_temperature

    print(f"[ALNS] Bắt đầu, objective initial = {current.objective:.2f}")

    for it in range(1, max_iter + 1):
        di = roulette_select(destroy_states, rng)
        ri = roulette_select(repair_states, rng)
        d_name = destroy_states[di].name
        r_name = repair_states[ri].name

        d_func = destroy_ops[d_name]
        r_func = repair_ops[r_name]

        partial = d_func(current.copy(), inst, rng)
        candidate = r_func(partial, inst, rng)
        F_new = evaluate(candidate, inst)
        F_cur = current.objective
        F_best = best.objective

        accept = False
        if F_new < F_cur:
            accept = True
        else:
            delta = F_new - F_cur
            if temperature > 1e-9:
                prob = math.exp(-delta / temperature)
                if rng.random() < prob:
                    accept = True

        if accept:
            current = candidate

        reward = 0.0
        if F_new < F_best:
            best = candidate.copy()
            reward = 5.0
        elif F_new < F_cur:
            reward = 1.0
        elif accept:
            reward = 0.1

        destroy_states[di].score += reward
        destroy_states[di].times_used += 1
        repair_states[ri].score += reward
        repair_states[ri].times_used += 1

        if it % segment_length == 0:
            for op in destroy_states:
                if op.times_used > 0:
                    avg = op.score / op.times_used
                    op.weight = (1 - reaction_factor) * op.weight + reaction_factor * avg
                    op.score = 0.0
                    op.times_used = 0
            for op in repair_states:
                if op.times_used > 0:
                    avg = op.score / op.times_used
                    op.weight = (1 - reaction_factor) * op.weight + reaction_factor * avg
                    op.score = 0.0
                    op.times_used = 0

        alpha = it / max_iter
        temperature = start_temperature * (1 - alpha) + end_temperature * alpha

        if it % 20 == 0 or it == 1 or it == max_iter:
            comps_cur = current.meta.get("components", {})
            comps_best = best.meta.get("components", {})
            print(f"[ALNS] it={it}, current={current.objective:.2f}, best={best.objective:.2f}, T={temperature:.2f}")
            print("   current components:", {k: round(v, 2) for k, v in comps_cur.items()})
            print("   best    components:", {k: round(v, 2) for k, v in comps_best.items()})

    print("[ALNS] Hoàn tất.")
    return best

# ============================================================
# 7. TABU SEARCH – FINAL VERSION (NO CROSS-CLUSTER / NO CROSS-DEPOT)
# ============================================================

@dataclass
class Move:
    move_type: str
    data: Any
    attr: Tuple[Any, ...]


def apply_move(sol: Solution, move: Move, inst: Instance) -> Solution:
    new_sol = sol.copy()

    if move.move_type == "relocate":
        cid, vid_from, pos_from, vid_to, pos_to = move.data
        r_from = new_sol.routes[vid_from]
        r_to = new_sol.routes[vid_to]

        if pos_from < len(r_from.stops) and r_from.stops[pos_from] == cid:
            r_from.stops.pop(pos_from)

        pos_to = min(pos_to, len(r_to.stops))
        r_to.stops.insert(pos_to, cid)

    elif move.move_type == "swap":
        cid1, vid1, pos1, cid2, vid2, pos2 = move.data
        r1 = new_sol.routes[vid1]
        r2 = new_sol.routes[vid2]

        if (
            pos1 < len(r1.stops) and r1.stops[pos1] == cid1 and
            pos2 < len(r2.stops) and r2.stops[pos2] == cid2
        ):
            r1.stops[pos1], r2.stops[pos2] = r2.stops[pos2], r1.stops[pos1]

    return new_sol


def generate_neighbors(sol: Solution, inst: Instance,
                       max_neighbors: int, rng: random.Random) -> List[Move]:

    moves: List[Move] = []
    veh_ids = list(sol.routes.keys())
    veh_depot = inst.depots

    # mapping customer positions
    customer_positions = []
    for vid, route in sol.routes.items():
        for pos, node in enumerate(route.stops):
            if node in inst.customers:
                customer_positions.append((vid, pos, node))

    # current route loads
    route_load_w = {vid: 0.0 for vid in veh_ids}
    route_load_v = {vid: 0.0 for vid in veh_ids}
    for vid, route in sol.routes.items():
        for node in route.stops:
            if node in inst.customers:
                route_load_w[vid] += inst.demand_w[node]
                route_load_v[vid] += inst.demand_v[node]

    # ============================================================
    # 1) CROSS-DEPOT RELOCATE (NEW!)
    # ============================================================
    for _ in range(max_neighbors):
        vid_from, pos_from, cid = rng.choice(customer_positions)

        # ANY depot allowed
        vid_to = rng.choice(veh_ids)
        if vid_to == vid_from:
            continue

        # capacity check
        new_w = route_load_w[vid_to] + inst.demand_w[cid]
        new_v = route_load_v[vid_to] + inst.demand_v[cid]
        if new_w > inst.vehicle_cap_w[vid_to]:
            continue
        if new_v > inst.vehicle_cap_v[vid_to]:
            continue

        r_to = sol.routes[vid_to]
        if len(r_to.stops) <= 1:
            continue

        to_pos = rng.randint(1, len(r_to.stops) - 1)
        moves.append(Move(
            move_type="relocate",
            data=(cid, vid_from, pos_from, vid_to, to_pos),
            attr=("relocate", cid, vid_from, vid_to)
        ))

    # ============================================================
    # 2) CROSS-CLUSTER SWAP (NEW!)
    # ============================================================
    for _ in range(max_neighbors):
        (vid1, pos1, c1), (vid2, pos2, c2) = rng.sample(customer_positions, 2)

        depot1 = veh_depot[vid1]
        depot2 = veh_depot[vid2]
        if depot1 != depot2:
            continue  # vẫn giữ cùng depot để tránh chaos

        # capacity check after swap
        w1 = route_load_w[vid1] - inst.demand_w[c1] + inst.demand_w[c2]
        v1 = route_load_v[vid1] - inst.demand_v[c1] + inst.demand_v[c2]
        w2 = route_load_w[vid2] - inst.demand_w[c2] + inst.demand_w[c1]
        v2 = route_load_v[vid2] - inst.demand_v[c2] + inst.demand_v[c1]

        if w1 > inst.vehicle_cap_w[vid1] or v1 > inst.vehicle_cap_v[vid1]:
            continue
        if w2 > inst.vehicle_cap_w[vid2] or v2 > inst.vehicle_cap_v[vid2]:
            continue

        moves.append(Move(
            move_type="swap",
            data=(c1, vid1, pos1, c2, vid2, pos2),
            attr=("swap", c1, c2)
        ))

    # ============================================================
    # 3) SPLIT ROUTE (NEW!)
    # ============================================================
    for vid, route in sol.routes.items():
        if len(route.stops) > 5:
            mid = len(route.stops) // 2
            cid_mid = route.stops[mid]
            if cid_mid in inst.customers:
                # try relocating the mid customer to another nearby vehicle
                vid2 = rng.choice(veh_ids)
                if vid2 != vid:
                    moves.append(Move(
                        move_type="relocate",
                        data=(cid_mid, vid, mid, vid2, 1),
                        attr=("route_split", cid_mid, vid, vid2)
                    ))

    return moves[:max_neighbors]


def tabu_search(
    inst: Instance,
    initial_solution: Solution,
    max_iter: int = 100,
    max_neighbors: int = 40,
    tabu_tenure: int = 20,
    rng_seed: int = 0,
) -> Solution:

    rng = random.Random(rng_seed)

    current = initial_solution.copy()
    evaluate(current, inst)
    best = current.copy()

    tabu_list: Dict[Tuple[Any, ...], int] = {}

    print(f"[TABU] Start – initial obj = {current.objective:.2f}")

    for it in range(1, max_iter + 1):
        if it % 15 == 0:      # mỗi 15 vòng lắc một lần
            shaken = current.copy()

            # lấy toàn bộ vị trí customer hiện có
            customer_positions = []
            for vid, route in shaken.routes.items():
                for pos, node in enumerate(route.stops):
                    if node in inst.customers:
                        customer_positions.append((vid, pos, node))

            # nếu route quá rỗng thì bỏ qua shake
            if len(customer_positions) >= 5:
                removed = []

                # remove 5 khách ngẫu nhiên
                for _ in range(5):
                    vid, pos, cid = rng.choice(customer_positions)
                    removed.append(cid)
                    shaken.routes[vid].stops.remove(cid)

                # reinsert bằng greedy_feasible
                shaken = repair_greedy_feasible(shaken, inst, rng)

                # nếu tốt hơn thì nhận
                if evaluate(shaken, inst) < current.objective:
                    print(f"[TABU] Shaking improved at it={it}")
                    current = shaken.copy()
                    # cập nhật best luôn nếu cần
                    if current.objective < best.objective:
                        best = current.copy()

        neighbors = generate_neighbors(current, inst, max_neighbors, rng)

        best_candidate = None
        best_move = None
        best_val = float("inf")

        for mv in neighbors:
            is_tabu = mv.attr in tabu_list and tabu_list[mv.attr] > 0
            cand = apply_move(current, mv, inst)
            F = evaluate(cand, inst)

            if is_tabu and F >= best.objective:
                continue

            if F < best_val:
                best_val = F
                best_candidate = cand
                best_move = mv

        if best_candidate is None:
            break

        current = best_candidate

        if best_move is not None:
            tabu_list[best_move.attr] = tabu_tenure

        to_delete = []
        for k in tabu_list:
            tabu_list[k] -= 1
            if tabu_list[k] <= 0:
                to_delete.append(k)
        for k in to_delete:
            del tabu_list[k]

        if current.objective < best.objective:
            best = current.copy()

        if it % 10 == 0 or it == 1 or it == max_iter:
            print(f"[TABU] it={it}, cur={current.objective:.2f}, best={best.objective:.2f}")

    print("[TABU] Done.")
    return best

# ============================================================
# 9. LOAD DATA FOR HCMC (ONLY THIS IS NEW)
# ============================================================

def load_data():
    """
    Load bộ dữ liệu Hồ Chí Minh (D003) nằm cùng thư mục với dn.py.
    """
    BASE_DIR = "/Users/alicecin/Documents/Lastmile/Zzz_data/LMDO data_3i/cac_thanh_pho_khac/Da_Nang"

    customers_path = os.path.join(BASE_DIR, "customers_clustered1.xlsx")
    depots_path    = os.path.join(BASE_DIR, "depots.xlsx")
    vehicles_path  = os.path.join(BASE_DIR, "vehicles.xlsx")
    roads_path     = os.path.join(BASE_DIR, "roads.xlsx")

    customers_df = pd.read_excel(customers_path)
    depots_df    = pd.read_excel(depots_path)
    vehicles_df  = pd.read_excel(vehicles_path)
    roads_df     = pd.read_excel(roads_path)

    print("=== LOAD DATA DN (D003) ===")
    print(customers_path)
    print(depots_path)
    print(vehicles_path)
    print(roads_path)

    print(f"[DATA] customers={len(customers_df)}, depots={len(depots_df)}, "
          f"vehicles={len(vehicles_df)}, roads rows={len(roads_df)}")

    return customers_df, depots_df, vehicles_df, roads_df

# ============================================================
# 10. INITIAL SOLUTION
# ============================================================

def build_initial_solution(inst: Instance) -> Solution:
    routes = {}
    for vid in inst.vehicles:
        d = inst.depots[vid]
        routes[vid] = Route(vehicle_id=vid, stops=[d, d])
    return Solution(routes=routes, all_customers=inst.customers)

# ============================================================
# 11. RUN ALNS + TABU
# ============================================================

def example_run_alns(inst: Instance) -> Solution:
    # 1) Khởi tạo rỗng
    init_empty = build_initial_solution(inst)
    evaluate(init_empty, inst)
    print("Initial (empty) obj:", init_empty.objective)

    # 2) Dùng greedy (KHÔNG check capacity) để nhét đầy khách,
    #    rồi cắt overload cho feasible.
    rng_init = random.Random(0)
    print("\n[DEBUG] Build initial solution with repair_greedy (no cap) + capacity_feasibilize ...")
    init_over = repair_greedy(init_empty.copy(), inst, rng_init)
    init_sol  = repair_capacity_feasibilize(init_over, inst, rng_init)

    evaluate(init_sol, inst)
    comps_init = init_sol.meta["components"]
    print("[DEBUG] after greedy+cap_feasible init: distance_cost =", comps_init["distance_cost"],
          ", unserved_pen =", comps_init["unserved_pen"])
    print("[DEBUG] #visited =", len(init_sol.meta["visited"]))


    destroy_ops = {
        "random":  lambda s,i,r: destroy_random(s,i,r,remove_ratio=0.05),
        "cluster": destroy_cluster,
        "shaw":    destroy_shaw_related,
    }
    repair_ops = {
        "greedy_feasible": repair_greedy_feasible,
        "greedy": repair_greedy,
        "regret": repair_regret,
    }


    best = alns(
        inst=inst,
        initial_solution=init_sol,
        destroy_ops=destroy_ops,
        repair_ops=repair_ops,
        max_iter=200,          # final run
        segment_length=30,
        reaction_factor=0.2,
        start_temperature=1000,
        end_temperature=1,
        rng_seed=1,
    )
    print("[ALNS] best obj (có thể còn overload) =", best.objective)

    # ==== BƯỚC MỚI: làm nghiệm capacity-feasible ====
    rng_cap = random.Random(123)
    best_feasible = repair_capacity_feasibilize(best, inst, rng_cap)
    evaluate(best_feasible, inst)
    print("[ALNS] best obj SAU khi cắt overload =", best_feasible.objective)

    print_solution_summary(best_feasible, inst, title="ALNS solution (capacity-feasible, final)")
    return best_feasible


def example_run_tabu(inst: Instance, sol_alns: Solution) -> Solution:
    # Dùng nghiệm ALNS đã capacity-feasible làm initial cho Tabu
    init_sol = sol_alns.copy()
    evaluate(init_sol, inst)
    print("Initial obj TABU (from ALNS):", init_sol.objective)

    best = tabu_search(
        inst=inst,
        initial_solution=init_sol,
        max_iter=100,      # có thể 100–150 nếu đủ thời gian
        max_neighbors=300,
        tabu_tenure=25,
        rng_seed=2,
    )
    print("[TABU] best obj =", best.objective)
    print_solution_summary(best, inst, title="TABU solution (final)")
    return best



# ============================================================
# 12. MAIN
# ============================================================

def print_solution_summary(sol: Solution, inst: Instance, title: str = ""):
    # Đảm bảo meta đã cập nhật
    evaluate(sol, inst)
    comps = sol.meta["components"]
    visited = sol.meta["visited"]
    total_dist_km = get_total_distance_km(sol)

    if title:
        print("\n====", title, "====")
    print("Objective:", round(sol.objective, 2))
    print("  - total_distance_km :", round(total_dist_km, 2))
    print("  - fixed         :", round(comps["fixed"], 2))
    print("  - distance_cost :", round(comps["distance_cost"], 2))
    print("  - unserved_pen  :", round(comps["unserved_pen"], 2))
    print("  - tw_pen        :", round(comps["tw_pen"], 2))
    print("  - overtime_pen  :", round(comps["overtime_pen"], 2))
    print("  - cap_pen       :", round(comps["capacity_pen"], 2))
    print("  - road_pen      :", round(comps["road_pen"], 2))
    print("  - dist_over_pen :", round(comps["dist_over_pen"], 2))
    print("  - depot_cap_pen :", round(comps["depot_cap_pen"], 2))
    print("  - workload_pen  :", round(comps["workload_pen"], 2))

    print("Số khách phục vụ :", len(visited), "/", len(inst.customers))

    # In thử 3 route đầu
    print("Một vài route mẫu:")
    for i, (vid, r) in enumerate(sol.routes.items()):
        if i >= 3:
            break
        print(f"  Route {vid}: length {len(r.stops)}")
        print("    ", r.stops[:12], "...")
        
# ============================================================
# 13. EXPORT SOLUTION TO EXCEL
# ============================================================

def get_output_dir() -> str:
    """
    Tạo (nếu chưa có) folder OUTPUT nằm cùng thư mục với file .py hiện tại.
    VD: /Users/.../Python_processing/optimizer/OUTPUT
    """
    base_dir = os.path.dirname(os.path.abspath(__file__))
    out_dir = os.path.join(base_dir, "OUTPUT")
    os.makedirs(out_dir, exist_ok=True)
    return out_dir

def get_image_dir() -> str:
    """
    Tạo (nếu chưa có) folder IMAGE nằm cùng thư mục với file .py hiện tại.
    VD: /.../python_processing/IMAGE
    """
    base_dir = os.path.dirname(os.path.abspath(__file__))
    img_dir = os.path.join(base_dir, "IMAGE")
    os.makedirs(img_dir, exist_ok=True)
    return img_dir


def export_solution_to_excel(sol: Solution, inst: Instance, run_name: str = "lan1_opt"):
    """
    Ghi nghiệm ra file Excel:
    - Sheet 1: routes (từng điểm dừng theo thứ tự)
    - Sheet 2: components (các thành phần objective)
    Tên file: <run_name>_năm-tháng-ngày__giờhphút.xlsx
    """
    # Đảm bảo meta đã tính đầy đủ
    evaluate(sol, inst)

    out_dir = get_output_dir()

    # Tạo timestamp kiểu: 2025-02-14__21h37
    ts = datetime.datetime.now().strftime("%Y-%m-%d__%Hh%M")
    filename = f"{run_name}_{ts}.xlsx"
    out_path = os.path.join(out_dir, filename)

    # --------- Sheet 1: Routes chi tiết ---------
    rows = []
    for vid, route in sol.routes.items():
        depot_id = inst.depots.get(vid, "")
        for idx, node in enumerate(route.stops):
            # ❗ BỎ QUA TẤT CẢ NODE DEPOT
            if not node.startswith("P"):
                continue
            is_customer = int(node in inst.customers)
            demand_w = inst.demand_w.get(node, 0.0)
            demand_v = inst.demand_v.get(node, 0.0)
            lat, lon = inst.coords.get(node, (None, None))
            cluster = inst.customer_cluster.get(node, None)

            rows.append({
                "Vehicle_ID": vid,
                "Stop_Order": idx,
                "Node_ID": node,
                "Is_Customer": is_customer,
                "Depot_of_Vehicle": depot_id,
                "Demand_Weight": demand_w,
                "Demand_Volume": demand_v,
                "Latitude": lat,
                "Longitude": lon,
                "Cluster/Depot_Assigned": cluster,
            })

    routes_df = pd.DataFrame(rows)

    # --------- Sheet 2: Objective components ---------
    comps = sol.meta.get("components", {})
    comp_df = pd.DataFrame([comps])

    # Ghi ra Excel với 2 sheet
    with pd.ExcelWriter(out_path, engine="xlsxwriter") as writer:
        routes_df.to_excel(writer, sheet_name="routes", index=False)
        comp_df.to_excel(writer, sheet_name="objective_components", index=False)

    print(f"[OUTPUT] Đã ghi file kết quả: {out_path}")

def visualize_solution_map(
    inst: Instance,
    sol: Solution,
    run_name: str = "alns_opt",
    max_routes_to_draw: int = 9999,
) -> str:
    """
    Vẽ bản đồ:
    - Khách được phục vụ: chấm màu đậm
    - Khách không phục vụ: chấm xám mờ (background)
    - Các depot: tam giác đỏ
    - Các route: nối theo thứ tự stops

    Lưu ảnh vào folder IMAGE, trả về path.
    """
    # đảm bảo meta đã cập nhật
    evaluate(sol, inst)

    img_dir = get_image_dir()
    ts = datetime.datetime.now().strftime("%Y-%m-%d__%Hh%M")
    filename = f"map_{run_name}_{ts}.png"
    out_path = os.path.join(img_dir, filename)

    visited = sol.meta.get("visited", set())
    all_customers = inst.customers
    unserved = all_customers - visited

    # ----------------- tách toạ độ -----------------
    # khách được phục vụ
    serv_lats, serv_lons = [], []
    # khách không phục vụ
    uns_lats, uns_lons = [], []

    for cid in all_customers:
        lat, lon = inst.coords.get(cid, (None, None))
        if lat is None or lon is None:
            continue
        if cid in visited:
            serv_lats.append(lat)
            serv_lons.append(lon)
        else:
            uns_lats.append(lat)
            uns_lons.append(lon)

    # depot
    depot_lats = []
    depot_lons = []
    depot_labels = []
    for vid, d_id in inst.depots.items():
        if d_id in depot_labels:
            continue
        lat, lon = inst.coords.get(d_id, (None, None))
        if lat is None or lon is None:
            continue
        depot_lats.append(lat)
        depot_lons.append(lon)
        depot_labels.append(d_id)

    plt.figure(figsize=(8, 8))

    # khách không phục vụ – nền xám mờ
    if uns_lats:
        plt.scatter(
            uns_lons,
            uns_lats,
            s=6,
            alpha=0.25,
            color="lightgray",
            label="Unserved customers",
        )

    # khách được phục vụ – màu xanh dương đậm
    if serv_lats:
        plt.scatter(
            serv_lons,
            serv_lats,
            s=10,
            alpha=0.9,
            color="tab:blue",
            label="Served customers",
        )

    # depot
    if depot_lats:
        plt.scatter(
            depot_lons,
            depot_lats,
            s=150,
            marker="^",
            edgecolor="black",
            linewidth=1.0,
            color="red",
            label="Depots",
        )
        for d_id, lat, lon in zip(depot_labels, depot_lats, depot_lons):
            plt.text(
                lon + 0.001,
                lat + 0.001,
                d_id,
                fontsize=9,
                fontweight="bold",
            )

    # ----------------- vẽ route -----------------
    for idx, (vid, route) in enumerate(sol.routes.items()):
        if idx >= max_routes_to_draw:
            break
        xs, ys = [], []
        for node in route.stops:
            lat, lon = inst.coords.get(node, (None, None))
            if lat is None or lon is None:
                continue
            xs.append(lon)
            ys.append(lat)
        if len(xs) >= 2:
            # mỗi route 1 đường mảnh
            plt.plot(xs, ys, linewidth=0.8, alpha=0.6)

    plt.title(f"DN – Routes & Served Customers ({run_name})")
    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    plt.legend()
    plt.grid(True, linewidth=0.5, alpha=0.3)
    plt.tight_layout()

    plt.savefig(out_path, dpi=300)
    plt.close()

    print(f"[IMAGE] Đã lưu bản đồ: {out_path}")
    return out_path

def visualize_depot_customer_spokes(
    inst: Instance,
    sol: Solution,
    run_name: str = "spokes_opt",
    only_served: bool = True,
    max_lines_per_vehicle: int = 999999,
) -> str:
    """
    Vẽ giống hình HEIC:
    - Không vẽ đường đi qua nhiều khách.
    - Chỉ vẽ các đoạn thẳng từ depot của xe -> từng customer trên route.
    - customers: chấm tròn
    - depots: tam giác
    """

    # cập nhật meta (visited, ...)
    evaluate(sol, inst)
    visited = sol.meta["visited"]

    img_dir = get_image_dir()
    ts = datetime.datetime.now().strftime("%Y-%m-%d__%Hh%M")
    filename = f"spokes_{run_name}_{ts}.png"
    out_path = os.path.join(img_dir, filename)

    plt.figure(figsize=(8, 8))

    # --- vẽ customers (served / unserved) ---
    served_lats, served_lons = [], []
    unserved_lats, unserved_lons = [], []

    for cid in inst.customers:
        lat, lon = inst.coords.get(cid, (None, None))
        if lat is None or lon is None:
            continue
        if cid in visited:
            served_lats.append(lat)
            served_lons.append(lon)
        else:
            unserved_lats.append(lat)
            unserved_lons.append(lon)

    if not only_served and unserved_lats:
        plt.scatter(
            unserved_lons,
            unserved_lats,
            s=8,
            alpha=0.4,
            label="Unserved customers",
        )

    if served_lats:
        plt.scatter(
            served_lons,
            served_lats,
            s=8,
            alpha=0.8,
            label="Served customers",
        )

    # --- vẽ depots ---
    depot_lats = []
    depot_lons = []
    depot_labels = []
    for vid, d_id in inst.depots.items():
        if d_id in depot_labels:
            continue
        lat, lon = inst.coords.get(d_id, (None, None))
        if lat is None or lon is None:
            continue
        depot_lats.append(lat)
        depot_lons.append(lon)
        depot_labels.append(d_id)

    if depot_lats:
        plt.scatter(
            depot_lons,
            depot_lats,
            s=150,
            marker="^",
            edgecolor="black",
            linewidth=1.0,
            color="red",
            label="Depots",
        )
        for d_id, lat, lon in zip(depot_labels, depot_lats, depot_lons):
            plt.text(lon + 0.001, lat + 0.001, d_id, fontsize=9, fontweight="bold")

    # --- vẽ các tia thẳng depot -> từng customer trên route ---
    # mỗi vehicle có depot riêng trong inst.depots[vid]
    for vid, route in sol.routes.items():
        d_id = inst.depots[vid]
        d_lat, d_lon = inst.coords.get(d_id, (None, None))
        if d_lat is None or d_lon is None:
            continue

        count_line = 0
        for node in route.stops:
            if node not in inst.customers:
                continue
            if only_served and node not in visited:
                continue

            c_lat, c_lon = inst.coords.get(node, (None, None))
            if c_lat is None or c_lon is None:
                continue

            # đoạn thẳng từ depot -> customer
            plt.plot(
                [d_lon, c_lon],
                [d_lat, c_lat],
                linewidth=0.4,
                alpha=0.4,
            )

            count_line += 1
            if count_line >= max_lines_per_vehicle:
                break

    plt.title(f"DN – Depot → Customers ({run_name})")
    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(out_path, dpi=300)
    plt.close()

    print(f"[IMAGE] Đã lưu bản đồ (spokes): {out_path}")
    return out_path

if __name__ == "__main__":
    import traceback

    try:
        customers_df, depots_df, vehicles_df, roads_df = load_data()
        inst = build_instance_for_depot_prefix("D003", customers_df, depots_df, vehicles_df, roads_df)
        print(">>> SỐ CUSTOMER TRONG INSTANCE:", len(inst.customers))
        print(">>> ALNS D003")
        sol_alns = example_run_alns(inst)
        print(">>> DONE ALNS:", sol_alns.objective)

        print("\n>>> TABU D003")
        sol_tabu = example_run_tabu(inst, sol_alns)
        print(">>> DONE TABU:", sol_tabu.objective)
        export_solution_to_excel(sol_alns, inst, run_name="alns_opt")
        export_solution_to_excel(sol_tabu, inst, run_name="tabu_opt")
        
        visualize_solution_map(inst, sol_alns, run_name="alns_opt")
        visualize_solution_map(inst, sol_tabu, run_name="tabu_opt")
        
        # bản đồ kiểu HEIC: depot -> customer
        visualize_depot_customer_spokes(inst, sol_alns, run_name="alns_opt")
        visualize_depot_customer_spokes(inst, sol_tabu, run_name="tabu_opt")


    except Exception as e:
        traceback.print_exc()