# ============================================================
# LAST-MILE DELIVERY – FULL MODEL (PDF) + ALNS + TABU + CLUSTER
# ============================================================
# - Map từ:
#   + customers_vietnam.xlsx
#   + depots_vietnam.xlsx
#   + vehicles_vietnam.xlsx
#   + roads_Dxxx_y.csv (D001_1..D010_5)
#   + multi-depot last-mile delivery problem (PDF)
#   G=(V,E), C, D, K, q_i^w, q_i^v, [e_i, l_i], s_i, phi_i, rho_u,v, sigma_k^w,v,
#   tau_k^max, L_k^max, eta_d, alpha_k, beta_k, ...
# - Mở rộng hàm mục tiêu f = f1 + f2 bằng các penalty mềm
#   cho ALNS/Tabu: unserved, TW violation, overtime, max distance,
#   depot capacity, workload balance.
# ============================================================

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Set, Tuple, Callable, Any, Optional
from collections import defaultdict
import pandas as pd
import math
import random
import glob

# ============================================================
# 1. DATA STRUCTURES
# ============================================================

@dataclass
class Instance:
    # Sets
    customers: Set[str]                 # C
    vehicles: List[str]                 # K
    depots: Dict[str, str]             # vehicle_id -> depot_id (Start_Depot_ID)
    depot_capacity: Dict[str, float]   # eta_d (Capacity_Storage)

    # Graph
    distance: Dict[str, Dict[str, float]]      # d_{u,v}
    travel_time: Dict[str, Dict[str, float]]   # t_{u,v}
    road_allowed: Dict[str, Dict[str, Dict[str, int]]]  # rho_u,v^k

    # Customer parameters (from PDF + Excel)
    demand_w: Dict[str, float]         # q_i^w
    demand_v: Dict[str, float]         # q_i^v
    service_time: Dict[str, float]     # s_i (phút)
    tw_start: Dict[str, float]         # e_i (phút từ 0h)
    tw_end: Dict[str, float]           # l_i (phút)
    priority: Dict[str, int]           # phi_i (1,2,3)
    delivery_type: Dict[str, str]      # theta_i (Home/Locker)
    coords: Dict[str, Tuple[float,float]]  # (lat, lon)
    customer_cluster: Dict[str, str]   # cluster id = depot_id gần nhất

    # Vehicle parameters (sigma_k^w, sigma_k^v, tau_k^max, L_k^max, alpha_k, beta_k)
    vehicle_cap_w: Dict[str, float]    # sigma_k^w
    vehicle_cap_v: Dict[str, float]    # sigma_k^v
    shift_max: Dict[str, float]        # tau_k^max (minutes)
    max_distance: Dict[str, float]     # L_k^max
    fixed_cost: Dict[str, float]       # alpha_k
    var_cost: Dict[str, float]         # beta_k

    # Penalty coefficients for metaheuristics (extension of PDF model)
    penalty_unserved: Dict[str, float]       # P_i (depends on priority & demand)
    lambda_E: Dict[str, float]               # early penalty
    lambda_L: Dict[str, float]               # late penalty
    lambda_H: Dict[str, float]               # overtime penalty per vehicle
    lambda_W: float                          # workload balance
    lambda_dist_overtime: float              # exceed max distance penalty
    lambda_depot_capacity: float             # exceed depot capacity penalty

    # BIG penalties for “hard-ish” constraints
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
    routes: Dict[str, Route]          # veh_id -> Route
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
# 2. HELPER FUNCTIONS (TIME, DIST)
# ============================================================

def time_str_to_min(t: str) -> int:
    """
    "09:30" -> 570 minutes.
    """
    if pd.isna(t):
        return 0
    t = str(t).strip()
    if "-" in t:  # in case "06:00-22:00"
        t = t.split("-")[0]
    h, m = t.split(":")
    return int(h) * 60 + int(m)

def parse_operating_hours(oh: str) -> Tuple[int,int]:
    """
    "06:00-22:00" -> (360, 1320). (Not currently used in evaluate, but available.)
    """
    if pd.isna(oh):
        return 0, 24*60
    oh = str(oh).strip()
    try:
        s, e = oh.split("-")
        return time_str_to_min(s), time_str_to_min(e)
    except Exception:
        return 0, 24*60

def geo_distance(lat1, lon1, lat2, lon2) -> float:
    """
    Approx Euclidean distance in km (good enough for clustering).
    """
    # very rough, not haversine, just for relative similarity
    dx = (lon2 - lon1) * math.cos((lat1 + lat2) * math.pi / 360.0)
    dy = (lat2 - lat1)
    return math.sqrt(dx*dx + dy*dy) * 111.0

# ============================================================
# 3. BUILD INSTANCE FROM EXCEL/CSV + PDF SCHEMA
# ============================================================

def build_instance_for_depot_prefix(
    depot_prefix: str,
    customers_df: pd.DataFrame,
    depots_df: pd.DataFrame,
    vehicles_df: pd.DataFrame,
    roads_df: pd.DataFrame,
) -> Instance:
    """
    Xây instance cho 1 cụm đường Dxxx (VD: "D001"):
    - K_d: tất cả vehicles có Start_Depot_ID bắt đầu bằng depot_prefix.
    - E: tất cả cung roads có Origin_Node_ID bắt đầu bằng depot_prefix.
    - C: tất cả khách xuất hiện trong Destination_Node_ID ∩ customers_vietnam.
    Clustering: gán mỗi khách vào depot gần nhất (trong group prefix) theo lat/lon.
    """

    # --- Vehicles K_d ---
    veh_sub = vehicles_df[vehicles_df["Start_Depot_ID"].str.startswith(depot_prefix)].copy()
    vehicle_ids = veh_sub["Vehicle_ID"].tolist()
    if not vehicle_ids:
        raise ValueError(f"Không có xe nào cho prefix {depot_prefix}")

    # vehicle -> depot_id
    depots_map = {row["Vehicle_ID"]: row["Start_Depot_ID"] for _, row in veh_sub.iterrows()}

    # max distance L_k^max
    max_distance = {row["Vehicle_ID"]: float(row["Max_Distance"]) for _, row in veh_sub.iterrows()}

    # --- Roads E ---
    roads_sub = roads_df[roads_df["Origin_Node_ID"].str.startswith(depot_prefix)].copy()
    if roads_sub.empty:
        raise ValueError(f"Không có roads cho prefix {depot_prefix}")

    origin_nodes = set(roads_sub["Origin_Node_ID"].unique())
    dest_nodes = set(roads_sub["Destination_Node_ID"].unique())

    # --- Customers C ---
    all_customer_ids = set(customers_df["Customer_ID"].unique())
    customers_in_instance = dest_nodes & all_customer_ids
    if not customers_in_instance:
        raise ValueError(f"Không có khách thuộc prefix {depot_prefix}")

    cust_sub = customers_df[customers_df["Customer_ID"].isin(customers_in_instance)].copy()

    # --- Customer params ---
    demand_w = {}
    demand_v = {}
    service_time = {}
    tw_start = {}
    tw_end = {}
    priority = {}
    delivery_type = {}
    coords = {}

    for _, row in cust_sub.iterrows():
        cid = row["Customer_ID"]
        demand_w[cid] = float(row["Order_Weight"])
        demand_v[cid] = float(row["Order_Volume"])
        service_time[cid] = float(row["Service_Time"])       # phút
        tw_start[cid] = float(time_str_to_min(row["Time_Window_Start"]))
        tw_end[cid] = float(time_str_to_min(row["Time_Window_End"]))
        priority[cid] = int(row["Priority_Level"])
        delivery_type[cid] = str(row["Delivery_Type"])
        coords[cid] = (float(row["Latitude"]), float(row["Longitude"]))

    # --- Depot params ---
    depots_sub = depots_df[depots_df["Depot_ID"].str.startswith(depot_prefix)].copy()
    depot_capacity = {row["Depot_ID"]: float(row["Capacity_Storage"]) for _, row in depots_sub.iterrows()}

    # (operating hours nếu muốn dùng sau)
    depot_open = {}
    depot_close = {}
    for _, row in depots_sub.iterrows():
        d_id = row["Depot_ID"]
        op_start, op_end = parse_operating_hours(row["Operating_Hours"])
        depot_open[d_id] = op_start
        depot_close[d_id] = op_end

    # --- Vehicle params ---
    vehicle_cap_w = {row["Vehicle_ID"]: float(row["Capacity_Weight"]) for _, row in veh_sub.iterrows()}
    vehicle_cap_v = {row["Vehicle_ID"]: float(row["Capacity_Volume"]) for _, row in veh_sub.iterrows()}
    fixed_cost = {row["Vehicle_ID"]: float(row["Fixed_Cost"]) for _, row in veh_sub.iterrows()}
    var_cost = {row["Vehicle_ID"]: float(row["Variable_Cost"]) for _, row in veh_sub.iterrows()}
    shift_max = {row["Vehicle_ID"]: float(row["Max_Working_Hours"]) * 60.0 for _, row in veh_sub.iterrows()}

    # --- Distance / Time matrix ---
    distance: Dict[str, Dict[str, float]] = defaultdict(dict)
    travel_time: Dict[str, Dict[str, float]] = defaultdict(dict)

    for _, row in roads_sub.iterrows():
        i = row["Origin_Node_ID"]
        j = row["Destination_Node_ID"]
        distance[i][j] = float(row["Distance_km"])
        travel_time[i][j] = float(row["Travel_Time_min"])

    # --- Road restrictions rho_u,v^k ---
    HEAVY_TYPES = {"Truck", "Heavy Truck", "Lorry"}  # tuỳ bạn điều chỉnh
    vehicle_type = {
        row["Vehicle_ID"]: str(row["Vehicle_Type"])
        for _, row in veh_sub.iterrows()
    }
    roads_sub["Road_Restrictions"] = roads_sub["Road_Restrictions"].fillna("None").astype(str).str.strip()

    road_allowed: Dict[str, Dict[str, Dict[str, int]]] = {
        vid: defaultdict(dict) for vid in vehicle_ids
    }

    for _, row in roads_sub.iterrows():
        i = row["Origin_Node_ID"]
        j = row["Destination_Node_ID"]
        restr = row["Road_Restrictions"]
        for vid in vehicle_ids:
            allow = 1
            vtype = vehicle_type[vid]
            if restr == "No Heavy Trucks" and vtype in HEAVY_TYPES:
                allow = 0
            # "One-Way" đã thể hiện bằng việc chỉ tồn tại cung i->j
            road_allowed[vid][i][j] = allow

    # --- Clustering: assign each customer to nearest depot in prefix ---
    #   cluster_id = depot_id gần nhất (về mặt lat/lon)
    #   => phù hợp multi-depot trong PDF.
    # build depot coords
    depot_coords: Dict[str, Tuple[float,float]] = {}
    for _, row in depots_sub.iterrows():
        d_id = row["Depot_ID"]
        depot_coords[d_id] = (float(row["Latitude"]), float(row["Longitude"]))

    customer_cluster: Dict[str, str] = {}
    for cid in customers_in_instance:
        clat, clon = coords[cid]
        best_d = None
        best_dist = float("inf")
        for d_id, (dlat, dlon) in depot_coords.items():
            gd = geo_distance(clat, clon, dlat, dlon)
            if gd < best_dist:
                best_dist = gd
                best_d = d_id
        customer_cluster[cid] = best_d

    # --- Penalties (metaheuristic extension) ---
    penalty_unserved: Dict[str, float] = {}
    lambda_E: Dict[str, float] = {}
    lambda_L: Dict[str, float] = {}
    for cid in customers_in_instance:
        phi = priority[cid]  # 1,2,3
        # phạt unserved phụ thuộc priority + weight
        penalty_unserved[cid] = 500.0 * phi * max(demand_w[cid], 1.0)
        # đến sớm nhẹ, trễ nặng, mở rộng từ TW cứng trong PDF
        lambda_E[cid] = 0.5 * phi
        lambda_L[cid] = 5.0 * phi

    lambda_H: Dict[str, float] = {}
    for vid in vehicle_ids:
        # penalty overtime tỉ lệ với fixed cost (có thể tune)
        lambda_H[vid] = fixed_cost[vid]

    lambda_W = 0.01                 # workload balance
    lambda_dist_overtime = 10.0     # vượt L_k^max
    lambda_depot_capacity = 10.0    # vượt sức chứa kho

    inst = Instance(
        customers=set(customers_in_instance),
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
        priority=priority,
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
    )

    return inst

# ============================================================
# 4. EVALUATION FUNCTION (f1 + f2 + penalties)
# ============================================================

def evaluate(sol: Solution, inst: Instance) -> float:
    """
    Hàm mục tiêu mở rộng:
    f = f1 + f2
        + penalty_unserved
        + penalty TW (early/late)
        + penalty overtime (thời lượng > tau_k^max)
        + penalty max distance (distance > L_k^max)
        + penalty depot capacity (sum q_i^w > eta_d)
        + penalty workload balance.
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
    W: Dict[str, float] = {}         # W_k quãng đường mỗi xe
    depot_load: Dict[str, float] = defaultdict(float)  # sum q_i^w per depot

    for vid, route in sol.routes.items():
        stops = route.stops
        if len(stops) <= 1:
            W[vid] = 0.0
            continue

        # f1: fixed cost khi u_k = 1
        total_fixed += inst.fixed_cost[vid]

        load_w = 0.0
        load_v = 0.0
        t = 0.0
        dist_k = 0.0

        depot_id = inst.depots[vid]

        for i, j in zip(stops[:-1], stops[1:]):
            # road restriction
            if inst.road_allowed[vid].get(i, {}).get(j, 0) == 0:
                total_road_pen += inst.BIG_ROAD

            d_ij = inst.distance.get(i, {}).get(j, 0.0)
            t_ij = inst.travel_time.get(i, {}).get(j, 0.0)
            dist_k += d_ij
            t += t_ij

            # nếu j là khách hàng
            if j in inst.customers:
                load_w += inst.demand_w[j]
                load_v += inst.demand_v[j]

                # capacity (hard-ish)
                if load_w > inst.vehicle_cap_w[vid]:
                    total_cap_pen += inst.BIG_CAP * (load_w - inst.vehicle_cap_w[vid]) / max(inst.vehicle_cap_w[vid], 1.0)
                if load_v > inst.vehicle_cap_v[vid]:
                    total_cap_pen += inst.BIG_CAP * (load_v - inst.vehicle_cap_v[vid]) / max(inst.vehicle_cap_v[vid], 1.0)

                # time window soft
                a_j = t  # arrival
                E_j = max(inst.tw_start[j] - a_j, 0.0)
                L_j = max(a_j - inst.tw_end[j], 0.0)
                total_tw_pen += inst.lambda_E[j] * E_j + inst.lambda_L[j] * L_j

                # service time
                t += inst.service_time[j]

                visited.add(j)
                depot_load[depot_id] += inst.demand_w[j]

        W[vid] = dist_k
        # f2: variable cost
        total_dist_cost += inst.var_cost[vid] * dist_k

        # overtime: route duration > tau_k^max
        overtime = max(t - inst.shift_max[vid], 0.0)
        if overtime > 0:
            total_overtime_pen += inst.lambda_H[vid] * overtime

        # max distance L_k^max
        if dist_k > inst.max_distance[vid]:
            extra = dist_k - inst.max_distance[vid]
            total_dist_over_pen += inst.lambda_dist_overtime * extra

    # phạt unserved
    for cid in inst.customers:
        if cid not in visited:
            total_unserved_pen += inst.penalty_unserved[cid]

    # depot capacity (sum q_i^w <= eta_d)
    for d_id, load in depot_load.items():
        cap = inst.depot_capacity.get(d_id, float("inf"))
        if load > cap:
            total_depot_cap_pen += inst.lambda_depot_capacity * (load - cap)

    # workload balance
    if W:
        avgW = sum(W.values()) / len(W)
        for vid in W:
            total_workload_pen += inst.lambda_W * (W[vid] - avgW) ** 2

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
        }
    }
    return F

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

# ---------- DESTROY OPERATORS ----------

def destroy_random(sol: Solution, inst: Instance, rng: random.Random, remove_ratio=0.1) -> Solution:
    """
    Destroy: remove ngẫu nhiên một tỉ lệ khách hàng khỏi tất cả route.
    """
    new_sol = sol.copy()
    allc = list(inst.customers)
    rng.shuffle(allc)
    n_remove = max(1, int(len(allc) * remove_ratio))
    to_remove = set(allc[:n_remove])

    for r in new_sol.routes.values():
        # giữ depot đầu & cuối
        depot = r.stops[0] if r.stops else None
        new_stops = [x for x in r.stops if x not in to_remove or x == depot]
        if len(new_stops) == 0 and depot is not None:
            new_stops = [depot]
        r.stops = new_stops
    return new_sol

def destroy_cluster(sol: Solution, inst: Instance, rng: random.Random) -> Solution:
    """
    Destroy theo cluster: chọn 1 cluster (depot) và xoá tất cả khách thuộc cluster đó.
    """
    new_sol = sol.copy()
    clusters = set(inst.customer_cluster.values())
    if not clusters:
        return new_sol
    chosen_cluster = rng.choice(list(clusters))

    to_remove = {cid for cid, cl in inst.customer_cluster.items() if cl == chosen_cluster}

    for r in new_sol.routes.values():
        depot = r.stops[0] if r.stops else None
        new_stops = [x for x in r.stops if x not in to_remove or x == depot]
        if not new_stops and depot is not None:
            new_stops = [depot]
        r.stops = new_stops

    return new_sol

def destroy_shaw_related(sol: Solution, inst: Instance, rng: random.Random, remove_count: int = 20) -> Solution:
    """
    Shaw removal: remove nhóm khách 'liên quan' (gần nhau, TW gần nhau, priority giống).
    """
    new_sol = sol.copy()
    allc = list(inst.customers)
    if not allc:
        return new_sol
    rng.shuffle(allc)
    seed = allc[0]

    def relatedness(i, j):
        # khoảng cách geo
        lat_i, lon_i = inst.coords[i]
        lat_j, lon_j = inst.coords[j]
        d_geo = geo_distance(lat_i, lon_i, lat_j, lon_j)
        # TW
        tw_diff = abs(inst.tw_start[i] - inst.tw_start[j]) + abs(inst.tw_end[i] - inst.tw_end[j])
        # priority
        pr_diff = abs(inst.priority[i] - inst.priority[j])
        return d_geo + 0.01 * tw_diff + 5.0 * pr_diff

    remaining = set(inst.customers)
    to_remove = [seed]
    remaining.remove(seed)

    while len(to_remove) < min(remove_count, len(inst.customers)) and remaining:
        last = rng.choice(to_remove)
        # chọn khách trong remaining có relatedness nhỏ nhất với last
        best_j = min(remaining, key=lambda j: relatedness(last, j))
        to_remove.append(best_j)
        remaining.remove(best_j)

    to_remove = set(to_remove)

    for r in new_sol.routes.values():
        depot = r.stops[0] if r.stops else None
        new_stops = [x for x in r.stops if x not in to_remove or x == depot]
        if not new_stops and depot is not None:
            new_stops = [depot]
        r.stops = new_stops

    return new_sol

# ---------- REPAIR OPERATORS ----------

def insertion_cost_distance_only(route: Route, vid: str, cid: str, pos: int, inst: Instance) -> float:
    """
    Ước lượng chi phí chèn cid vào route.stops tại vị trí pos (chỉ theo distance).
    Route: [n0, n1, ..., nk]
    Chèn giữa stops[pos-1] -> cid -> stops[pos].
    """
    stops = route.stops
    if not stops:
        return 0.0
    i = stops[pos-1]
    j = stops[pos] if pos < len(stops) else None
    d_old = 0.0
    d_new = 0.0
    if j is not None:
        d_old = inst.distance.get(i, {}).get(j, 0.0)
    d_new += inst.distance.get(i, {}).get(cid, 0.0)
    if j is not None:
        d_new += inst.distance.get(cid, {}).get(j, 0.0)
    return d_new - d_old

def repair_greedy(sol: Solution, inst: Instance, rng: random.Random) -> Solution:
    """
    Repair: chèn các khách chưa phục vụ bằng greedy insertion theo distance.
    """
    new_sol = sol.copy()
    evaluate(new_sol, inst)
    served = new_sol.meta.get("visited", set())
    unserved = list(inst.customers - served)
    rng.shuffle(unserved)

    for cid in unserved:
        best_delta = float("inf")
        best_vid = None
        best_pos = None

        for vid, route in new_sol.routes.items():
            if len(route.stops) == 0:
                continue
            # đảm bảo depot đầu & cuối
            if len(route.stops) == 1:
                # chỉ có depot -> thêm depot cuối
                depot = route.stops[0]
                route.stops.append(depot)

            for pos in range(1, len(route.stops)):  # không chèn trước depot đầu
                delta = insertion_cost_distance_only(route, vid, cid, pos, inst)
                if delta < best_delta:
                    best_delta = delta
                    best_vid = vid
                    best_pos = pos

        if best_vid is not None:
            new_sol.routes[best_vid].stops.insert(best_pos, cid)

    return new_sol

def repair_regret(sol: Solution, inst: Instance, rng: random.Random, k_regret: int = 2) -> Solution:
    """
    Regret-k insertion: lặp cho đến khi chèn hết unserved.
    """
    new_sol = sol.copy()
    evaluate(new_sol, inst)
    served = new_sol.meta.get("visited", set())
    unserved = list(inst.customers - served)

    while unserved:
        best_cid = None
        best_vid_pos = None
        best_regret = -1.0
        best_delta_for_cid = None

        for cid in unserved:
            insertion_candidates = []
            for vid, route in new_sol.routes.items():
                if len(route.stops) == 0:
                    continue
                if len(route.stops) == 1:
                    depot = route.stops[0]
                    route.stops.append(depot)
                for pos in range(1, len(route.stops)):
                    delta = insertion_cost_distance_only(route, vid, cid, pos, inst)
                    insertion_candidates.append((delta, vid, pos))

            if not insertion_candidates:
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
                best_delta_for_cid = insertion_candidates[0]

        if best_cid is None or best_vid_pos is None and best_delta_for_cid is None:
            break

        delta, vid, pos = best_delta_for_cid
        new_sol.routes[vid].stops.insert(pos, best_cid)
        unserved.remove(best_cid)

    return new_sol

# ============================================================
# 6. ALNS MAIN LOOP
# ============================================================

def alns(
    inst: Instance,
    initial_solution: Solution,
    destroy_ops: Dict[str, DestroyOp],
    repair_ops: Dict[str, RepairOp],
    max_iter: int = 300,
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

        # reward
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

        # update weights
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

        # cooling
        alpha = it / max_iter
        temperature = start_temperature * (1 - alpha) + end_temperature * alpha

    return best

# ============================================================
# 7. TABU SEARCH: relocate + swap
# ============================================================

@dataclass
class Move:
    move_type: str           # "relocate" or "swap"
    data: Any                # detail
    attr: Tuple[Any, ...]    # tabu attribute

def apply_move(sol: Solution, move: Move, inst: Instance) -> Solution:
    new_sol = sol.copy()
    if move.move_type == "relocate":
        cid, from_vid, from_pos, to_vid, to_pos = move.data
        r_from = new_sol.routes[from_vid]
        r_to = new_sol.routes[to_vid]

        # remove from r_from
        if from_pos < len(r_from.stops) and r_from.stops[from_pos] == cid:
            r_from.stops.pop(from_pos)
        # insert into r_to
        r_to.stops.insert(to_pos, cid)

    elif move.move_type == "swap":
        cid1, vid1, pos1, cid2, vid2, pos2 = move.data
        r1 = new_sol.routes[vid1]
        r2 = new_sol.routes[vid2]

        if r1.stops[pos1] == cid1 and r2.stops[pos2] == cid2:
            r1.stops[pos1], r2.stops[pos2] = r2.stops[pos2], r1.stops[pos1]

    return new_sol

def generate_neighbors(sol: Solution, inst: Instance, max_neighbors: int, rng: random.Random) -> List[Move]:
    moves: List[Move] = []
    veh_ids = list(sol.routes.keys())

    # Build list of (vid, pos, cid) for all customers
    customer_positions = []
    for vid, route in sol.routes.items():
        for pos, node in enumerate(route.stops):
            if node in inst.customers:
                customer_positions.append((vid, pos, node))

    # Relocate moves
    for _ in range(max_neighbors // 2):
        if not customer_positions:
            break
        vid_from, pos_from, cid = rng.choice(customer_positions)
        vid_to = rng.choice(veh_ids)
        r_to = sol.routes[vid_to]
        # vị trí chèn ngẫu nhiên (không chèn trước depot đầu)
        if len(r_to.stops) <= 1:
            continue
        to_pos = rng.randint(1, len(r_to.stops) - 1)

        move = Move(
            move_type="relocate",
            data=(cid, vid_from, pos_from, vid_to, to_pos),
            attr=("relocate", cid, vid_from, vid_to),
        )
        moves.append(move)

    # Swap moves
    for _ in range(max_neighbors // 2):
        if len(customer_positions) < 2:
            break
        (vid1, pos1, cid1), (vid2, pos2, cid2) = rng.sample(customer_positions, 2)
        move = Move(
            move_type="swap",
            data=(cid1, vid1, pos1, cid2, vid2, pos2),
            attr=("swap", cid1, cid2),
        )
        moves.append(move)

    return moves[:max_neighbors]

def tabu_search(
    inst: Instance,
    initial_solution: Solution,
    max_iter: int = 1000,
    max_neighbors: int = 50,
    tabu_tenure: int = 15,
    rng_seed: int = 0,
) -> Solution:
    rng = random.Random(rng_seed)

    current = initial_solution.copy()
    evaluate(current, inst)
    best = current.copy()

    tabu: Dict[Tuple[Any, ...], int] = {}

    for it in range(1, max_iter + 1):
        neighbors = generate_neighbors(current, inst, max_neighbors, rng)
        best_cand = None
        best_move = None
        best_val = float("inf")

        for mv in neighbors:
            is_tabu = mv.attr in tabu and tabu[mv.attr] > 0
            cand = apply_move(current, mv, inst)
            F_new = evaluate(cand, inst)

            # Aspiration: nếu tốt hơn best global thì bỏ qua tabu
            if is_tabu and F_new >= best.objective:
                continue

            if F_new < best_val:
                best_val = F_new
                best_cand = cand
                best_move = mv

        if best_cand is None:
            break

        current = best_cand

        # update tabu list
        tabu[best_move.attr] = tabu_tenure
        to_remove = []
        for a in tabu:
            tabu[a] -= 1
            if tabu[a] <= 0:
                to_remove.append(a)
        for a in to_remove:
            del tabu[a]

        if current.objective < best.objective:
            best = current.copy()

    return best

# ============================================================
# 8. LOAD DATA & EXAMPLE RUN
# ============================================================

def load_data():
    customers_df = pd.read_excel("/mnt/data/customers_vietnam.xlsx")
    depots_df = pd.read_excel("/mnt/data/depots_vietnam.xlsx")
    vehicles_df = pd.read_excel("/mnt/data/vehicles_vietnam.xlsx")

    road_files = glob.glob("/mnt/data/roads_*.csv")
    roads_df = pd.concat([pd.read_csv(f) for f in road_files], ignore_index=True)
    return customers_df, depots_df, vehicles_df, roads_df

def build_initial_solution(inst: Instance) -> Solution:
    """
    Initial: mỗi xe chỉ có depot start (chưa gán khách).
    Sau đó ALNS/Tabu + clustering sẽ xây route.
    """
    routes = {}
    for vid in inst.vehicles:
        d = inst.depots[vid]
        # route kiểu [depot, depot] để dễ chèn
        routes[vid] = Route(vehicle_id=vid, stops=[d, d])
    return Solution(routes=routes, all_customers=inst.customers)

def example_run_alns(prefix: str = "D001"):
    customers_df, depots_df, vehicles_df, roads_df = load_data()
    inst = build_instance_for_depot_prefix(prefix, customers_df, depots_df, vehicles_df, roads_df)
    init_sol = build_initial_solution(inst)

    destroy_ops = {
        "random": lambda s, i, r: destroy_random(s, i, r, remove_ratio=0.05),
        "cluster": destroy_cluster,
        "shaw": destroy_shaw_related,
    }
    repair_ops = {
        "greedy": repair_greedy,
        "regret": repair_regret,
    }

    best = alns(
        inst=inst,
        initial_solution=init_sol,
        destroy_ops=destroy_ops,
        repair_ops=repair_ops,
        max_iter=300,
        rng_seed=1,
    )
    print(f"[ALNS] Best objective for {prefix}: {best.objective:.2f}")
    print("Components:", best.meta.get("components", {}))
    return best

def example_run_tabu(prefix: str = "D001"):
    customers_df, depots_df, vehicles_df, roads_df = load_data()
    inst = build_instance_for_depot_prefix(prefix, customers_df, depots_df, vehicles_df, roads_df)
    init_sol = build_initial_solution(inst)

    best = tabu_search(
        inst=inst,
        initial_solution=init_sol,
        max_iter=300,
        max_neighbors=100,
        tabu_tenure=15,
        rng_seed=2,
    )
    print(f"[TABU] Best objective for {prefix}: {best.objective:.2f}")
    print("Components:", best.meta.get("components", {}))
    return best

if __name__ == "__main__":
    # chạy demo cho D001 (HCM)
    example_run_alns("D001")
    example_run_tabu("D001")
