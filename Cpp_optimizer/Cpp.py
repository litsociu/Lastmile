
# Cpp_fixed.py
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Set, Tuple, Callable, Any, Optional
from collections import defaultdict
import pandas as pd
import math
import random
import glob
import os

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
    try:
        h, m = t.split(":")
        return int(h) * 60 + int(m)
    except Exception:
        return 0

def parse_operating_hours(oh: str) -> Tuple[int,int]:
    """
    "06:00-22:00" -> (360, 1320).
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
    Xây instance cho 1 cụm đường Dxxx (VD: "D001").
    """
    vehicles_df["Start_Depot_ID"] = vehicles_df["Start_Depot_ID"].fillna("").astype(str)
    veh_sub = vehicles_df[vehicles_df["Start_Depot_ID"].str.startswith(depot_prefix)].copy()
    vehicle_ids = veh_sub["Vehicle_ID"].tolist()
    if not vehicle_ids:
        raise ValueError(f"Không có xe nào cho prefix {depot_prefix}")

    depots_map = {row["Vehicle_ID"]: row["Start_Depot_ID"] for _, row in veh_sub.iterrows()}
    max_distance = {row["Vehicle_ID"]: float(row["Max_Distance"]) for _, row in veh_sub.iterrows()}

    roads_df["Origin_Node_ID"] = roads_df["Origin_Node_ID"].fillna("").astype(str)
    roads_sub = roads_df[roads_df["Origin_Node_ID"].str.startswith(depot_prefix)].copy()
    if roads_sub.empty:
        raise ValueError(f"Không có roads cho prefix {depot_prefix}")

    origin_nodes = set(roads_sub["Origin_Node_ID"].unique())
    dest_nodes = set(roads_sub["Destination_Node_ID"].unique())

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
        def safe_float(x, default=0.0):
            try:
                if pd.isna(x):
                    return default
                return float(x)
            except Exception:
                return default

        def safe_int(x, default=1):
            try:
                if pd.isna(x):
                    return default
                return int(x)
            except Exception:
                return default

        cid = row["Customer_ID"]
        demand_w[cid] = safe_float(row.get("Order_Weight", 0.0), 0.0)
        demand_v[cid] = safe_float(row.get("Order_Volume", 0.0), 0.0)
        service_time[cid] = safe_float(row.get("Service_Time", 0.0), 0.0)       # phút
        tw_start[cid] = float(time_str_to_min(row.get("Time_Window_Start", 0)))
        tw_end[cid] = float(time_str_to_min(row.get("Time_Window_End", 24*60)))
        priority[cid] = safe_int(row.get("Priority_Level", 1), 1)
        delivery_type[cid] = str(row.get("Delivery_Type", "Home"))
        coords[cid] = (float(row.get("Latitude", 0.0)), float(row.get("Longitude", 0.0)))

    # --- Depot params ---
    depots_df["Depot_ID"] = depots_df["Depot_ID"].fillna("").astype(str)
    depots_sub = depots_df[depots_df["Depot_ID"].str.startswith(depot_prefix)].copy()
    depot_capacity = {row["Depot_ID"]: float(row["Capacity_Storage"]) for _, row in depots_sub.iterrows()}

    depot_open = {}
    depot_close = {}
    for _, row in depots_sub.iterrows():
        d_id = row["Depot_ID"]
        op_start, op_end = parse_operating_hours(row.get("Operating_Hours", None))
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
        try:
            distance[i][j] = float(row["Distance_km"])
        except Exception:
            distance[i][j] = 0.0
        try:
            travel_time[i][j] = float(row["Travel_Time_min"])
        except Exception:
            travel_time[i][j] = 0.0

    # --- Road restrictions rho_u,v^k ---
    HEAVY_TYPES = {"Truck", "Heavy Truck", "Lorry"}  # tuỳ bạn điều chỉnh
    vehicle_type = {
        row["Vehicle_ID"]: str(row["Vehicle_Type"])
        for _, row in veh_sub.iterrows()
    }
    roads_sub["Road_Restrictions"] = roads_sub.get("Road_Restrictions", "").fillna("None").astype(str).str.strip()

    road_allowed: Dict[str, Dict[str, Dict[str, int]]] = {
        vid: defaultdict(dict) for vid in vehicle_ids
    }

    for _, row in roads_sub.iterrows():
        i = row["Origin_Node_ID"]
        j = row["Destination_Node_ID"]
        restr = row["Road_Restrictions"]
        for vid in vehicle_ids:
            allow = 1
            vtype = vehicle_type.get(vid, "")
            if restr == "No Heavy Trucks" and vtype in HEAVY_TYPES:
                allow = 0
            road_allowed[vid][i][j] = allow

    # --- Clustering: assign each customer to nearest depot in prefix ---
    depot_coords: Dict[str, Tuple[float,float]] = {}
    for _, row in depots_sub.iterrows():
        d_id = row["Depot_ID"]
        depot_coords[d_id] = (float(row.get("Latitude", 0.0)), float(row.get("Longitude", 0.0)))

    customer_cluster: Dict[str, str] = {}
    for cid in customers_in_instance:
        clat, clon = coords.get(cid, (0.0, 0.0))
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
        phi = priority.get(cid, 1)
        penalty_unserved[cid] = 500.0 * phi * max(demand_w.get(cid, 0.0), 1.0)
        lambda_E[cid] = 0.5 * phi
        lambda_L[cid] = 5.0 * phi

    lambda_H: Dict[str, float] = {}
    for vid in vehicle_ids:
        lambda_H[vid] = fixed_cost.get(vid, 1.0)

    lambda_W = 0.01
    lambda_dist_overtime = 10.0
    lambda_depot_capacity = 10.0

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
    Evaluate a solution with complete hardening:
    - Detect malformed routes
    - Limit penalty accumulation
    - Faster and safe ALNS evaluation
    """

    # 1. Initialize component costs
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

    visited = set()
    W = {}
    depot_load = defaultdict(float)

    # ------------------------------------------------------------------
    # 2. Evaluate each vehicle route
    # ------------------------------------------------------------------
    for vid, route in sol.routes.items():
        stops = route.stops

        # If route is missing, empty or malformed
        if not stops or len(stops) < 2:
            W[vid] = 0.0
            continue
        
        # Fixed cost
        total_fixed += inst.fixed_cost.get(vid, 0.0)

        load_w = 0.0
        load_v = 0.0
        t = 0.0
        dist_k = 0.0
        depot_id = inst.depots.get(vid, None)

        route_invalid = False
        
        # Iterate each arc
        for i, j in zip(stops[:-1], stops[1:]):

            allowed = inst.road_allowed.get(vid, {}).get(i, {}).get(j, 1)
            d_ij = inst.distance.get(i, {}).get(j)
            t_ij = inst.travel_time.get(i, {}).get(j)

            if (not allowed) or (d_ij is None) or (t_ij is None):
                # Penalize once for invalid arc
                total_road_pen += inst.BIG_ROAD
                route_invalid = True
                break

            dist_k += d_ij
            t += t_ij

            if j in inst.customers:

                # load
                load_w += inst.demand_w.get(j, 0.0)
                load_v += inst.demand_v.get(j, 0.0)

                # capacity (only penalize once per violation)
                cap_w = inst.vehicle_cap_w.get(vid, float("inf"))
                cap_v = inst.vehicle_cap_v.get(vid, float("inf"))

                if load_w > cap_w:
                    total_cap_pen += inst.BIG_CAP
                if load_v > cap_v:
                    total_cap_pen += inst.BIG_CAP

                # soft TW
                a_j = t
                E = inst.tw_start.get(j, 0.0)
                L = inst.tw_end.get(j, 24*60)

                if a_j < E:
                    total_tw_pen += inst.lambda_E.get(j, 0.0) * (E - a_j)
                elif a_j > L:
                    total_tw_pen += inst.lambda_L.get(j, 0.0) * (a_j - L)

                # service time
                t += inst.service_time.get(j, 0.0)

                visited.add(j)
                if depot_id:
                    depot_load[depot_id] += inst.demand_w.get(j, 0.0)

        # Save travel distance
        W[vid] = dist_k
        total_dist_cost += inst.var_cost.get(vid, 0.0) * dist_k

        # Overtime
        max_shift = inst.shift_max.get(vid, float("inf"))
        if t > max_shift:
            total_overtime_pen += inst.lambda_H.get(vid, 0.0) * (t - max_shift)

        # Max distance
        limit_dist = inst.max_distance.get(vid, float("inf"))
        if dist_k > limit_dist:
            total_dist_over_pen += inst.lambda_dist_overtime * (dist_k - limit_dist)

    # ------------------------------------------------------------------
    # 3. Unserved customers
    # ------------------------------------------------------------------
    for cid in inst.customers:
        if cid not in visited:
            total_unserved_pen += inst.penalty_unserved.get(cid, 0.0)

    # ------------------------------------------------------------------
    # 4. Depot capacity
    # ------------------------------------------------------------------
    for d_id, load in depot_load.items():
        cap = inst.depot_capacity.get(d_id, float("inf"))
        if load > cap:
            total_depot_cap_pen += inst.lambda_depot_capacity * (load - cap)

    # ------------------------------------------------------------------
    # 5. Workload balance
    # ------------------------------------------------------------------
    if W:
        avgW = sum(W.values()) / len(W)
        for vid in W:
            total_workload_pen += inst.lambda_W * (W[vid] - avgW) ** 2

    # ------------------------------------------------------------------
    # 6. Total objective
    # ------------------------------------------------------------------
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
        "depot_load": depot_load
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

def _ensure_route_has_double_depot(route: Route):
    """
    Guarantee route invariant: if route has at least one depot, ensure
    it has form [d, d] when no customers exist. If route empty, do nothing.
    """
    if not route.stops:
        return
    if len(route.stops) == 1:
        d = route.stops[0]
        route.stops[:] = [d, d]

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
        depot = r.stops[0] if r.stops else None
        new_stops = [x for x in r.stops if (x not in to_remove) or (x == depot)]
        if not new_stops and depot is not None:
            new_stops = [depot]
        r.stops = new_stops
        _ensure_route_has_double_depot(r)
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
        new_stops = [x for x in r.stops if (x not in to_remove) or (x == depot)]
        if not new_stops and depot is not None:
            new_stops = [depot]
        r.stops = new_stops
        _ensure_route_has_double_depot(r)
    return new_sol

def destroy_shaw_related(sol: Solution, inst: Instance, rng: random.Random, remove_count: int = 20) -> Solution:
    """
    Shaw removal: remove nhóm khách 'liên quan'
    """
    new_sol = sol.copy()
    allc = list(inst.customers)
    if not allc:
        return new_sol
    rng.shuffle(allc)
    seed = allc[0]

    def relatedness(i, j):
        lat_i, lon_i = inst.coords.get(i, (0.0, 0.0))
        lat_j, lon_j = inst.coords.get(j, (0.0, 0.0))
        d_geo = geo_distance(lat_i, lon_i, lat_j, lon_j)
        tw_diff = abs(inst.tw_start.get(i, 0.0) - inst.tw_start.get(j, 0.0)) + abs(inst.tw_end.get(i, 24*60) - inst.tw_end.get(j, 24*60))
        pr_diff = abs(inst.priority.get(i, 1) - inst.priority.get(j, 1))
        return d_geo + 0.01 * tw_diff + 5.0 * pr_diff

    remaining = set(inst.customers)
    to_remove = [seed]
    if seed in remaining:
        remaining.remove(seed)

    while len(to_remove) < min(remove_count, len(inst.customers)) and remaining:
        last = rng.choice(to_remove)
        best_j = min(remaining, key=lambda j: relatedness(last, j))
        to_remove.append(best_j)
        remaining.remove(best_j)

    to_remove = set(to_remove)

    for r in new_sol.routes.values():
        depot = r.stops[0] if r.stops else None
        new_stops = [x for x in r.stops if (x not in to_remove) or (x == depot)]
        if not new_stops and depot is not None:
            new_stops = [depot]
        r.stops = new_stops
        _ensure_route_has_double_depot(r)

    return new_sol

# ---------- REPAIR OPERATORS ----------

def insertion_cost_distance_only(route: Route, vid: str, cid: str, pos: int, inst: Instance) -> float:
    """
    Compute additional distance when inserting cid into route at position pos.
    Important: check existence of required arcs and road_allowed for this vehicle.
    Note: We DO NOT require that the old arc i->j exists (it may not exist if j is depot),
    but we do require arcs i->cid and cid->j (if j exists) to exist and be allowed.
    """
    stops = route.stops
    if not stops:
        return float("inf")
    if pos <= 0 or pos > len(stops):
        return float("inf")

    i = stops[pos-1]
    j = stops[pos] if pos < len(stops) else None

    # check existence and allowed of i->cid
    d_i_c = inst.distance.get(i, {}).get(cid, None)
    if d_i_c is None:
        return float("inf")
    if inst.road_allowed.get(vid, {}).get(i, {}).get(cid, 1) == 0:
        return float("inf")

    # if j exists (inserting before j) check cid->j exists and allowed
    if j is not None:
        d_c_j = inst.distance.get(cid, {}).get(j, None)
        if d_c_j is None:
            return float("inf")
        if inst.road_allowed.get(vid, {}).get(cid, {}).get(j, 1) == 0:
            return float("inf")
        # old distance i->j: if missing we consider old distance = large to avoid insertion
        d_i_j = inst.distance.get(i, {}).get(j, None)
        if d_i_j is None:
            # allow insertion even if i->j missing (we replace non-existing arc with two existing arcs),
            # but in such case the delta should be d_i_c + d_c_j (no subtraction)
            d_old = 0.0
        else:
            d_old = d_i_j
        d_new = d_i_c + d_c_j
    else:
        # inserting at the end before no node (shouldn't happen because we ensure depot duplication)
        d_old = 0.0
        d_new = d_i_c

    return d_new - d_old

def repair_greedy(sol: Solution, inst: Instance, rng: random.Random) -> Solution:
    """
    Repair: chèn các khách chưa phục vụ bằng greedy insertion theo distance.
    """
    def ensure_routes_have_end_depot(sol: Solution):
        for vid, route in sol.routes.items():
            if len(route.stops) == 0:
                continue
            if len(route.stops) == 1:
                d = route.stops[0]
                route.stops[:] = [d, d]

    new_sol = sol.copy()
    ensure_routes_have_end_depot(new_sol)
    evaluate(new_sol, inst)
    served = set(new_sol.meta.get("visited", set()))
    unserved = list(inst.customers - served)
    rng.shuffle(unserved)

    for cid in unserved:
        best_delta = float("inf")
        best_vid = None
        best_pos = None

        for vid, route in new_sol.routes.items():
            if len(route.stops) == 0:
                continue
            if len(route.stops) == 1:
                depot = route.stops[0]
                route.stops.append(depot)

            # try all insertion positions between 1 .. len(route.stops)-1
            for pos in range(1, len(route.stops)):
                delta = insertion_cost_distance_only(route, vid, cid, pos, inst)
                if delta < best_delta:
                    best_delta = delta
                    best_vid = vid
                    best_pos = pos

        if best_vid is not None and best_pos is not None and best_delta < float("inf"):
            new_sol.routes[best_vid].stops.insert(best_pos, cid)
            # optionally re-evaluate regularly for stronger feasibility checking
            evaluate(new_sol, inst)

    return new_sol

def repair_regret(sol: Solution, inst: Instance, rng: random.Random, k_regret: int = 2) -> Solution:
    """
    Regret-k insertion: lặp cho đến khi chèn hết unserved.
    """
    def ensure_routes_have_end_depot(sol: Solution):
        for vid, route in sol.routes.items():
            if len(route.stops) == 0:
                continue
            if len(route.stops) == 1:
                d = route.stops[0]
                route.stops[:] = [d, d]

    new_sol = sol.copy()
    ensure_routes_have_end_depot(new_sol)
    evaluate(new_sol, inst)
    served = set(new_sol.meta.get("visited", set()))
    unserved = list(inst.customers - served)

    while unserved:
        best_cid = None
        best_delta_for_cid = None
        best_regret = -1.0

        for cid in unserved:
            insertion_candidates = []
            for vid, route in new_sol.routes.items():
                if len(route.stops) <= 1:
                    continue
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

        if best_cid is None or best_delta_for_cid is None:
            break

        delta, vid, pos = best_delta_for_cid
        if delta < float("inf"):
            new_sol.routes[vid].stops.insert(pos, best_cid)
            evaluate(new_sol, inst)
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

        if it % 50 == 0:
            print(f"[ALNS] Iteration {it}, current obj = {current.objective:.2f}, best = {best.objective:.2f}")

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
        # bounds checks
        if from_vid not in new_sol.routes or to_vid not in new_sol.routes:
            return new_sol
        r_from = new_sol.routes[from_vid]
        r_to = new_sol.routes[to_vid]

        # safe remove from r_from
        if 0 <= from_pos < len(r_from.stops) and r_from.stops[from_pos] == cid:
            r_from.stops.pop(from_pos)
        else:
            # fallback: try to find and remove by value
            try:
                idx = r_from.stops.index(cid)
                r_from.stops.pop(idx)
            except ValueError:
                pass

        # safe insert into r_to
        if to_pos < 0:
            to_pos = 1
        if to_pos > len(r_to.stops):
            to_pos = len(r_to.stops)
        r_to.stops.insert(to_pos, cid)

        # ensure invariants
        _ensure_route_has_double_depot(r_from)
        _ensure_route_has_double_depot(r_to)

    elif move.move_type == "swap":
        cid1, vid1, pos1, cid2, vid2, pos2 = move.data
        if vid1 not in new_sol.routes or vid2 not in new_sol.routes:
            return new_sol
        r1 = new_sol.routes[vid1]
        r2 = new_sol.routes[vid2]

        # validate positions
        if 0 <= pos1 < len(r1.stops) and 0 <= pos2 < len(r2.stops):
            if r1.stops[pos1] == cid1 and r2.stops[pos2] == cid2:
                r1.stops[pos1], r2.stops[pos2] = r2.stops[pos2], r1.stops[pos1]
        else:
            # attempt to find indices and swap if exist
            try:
                idx1 = r1.stops.index(cid1)
                idx2 = r2.stops.index(cid2)
                r1.stops[idx1], r2.stops[idx2] = r2.stops[idx2], r1.stops[idx1]
            except ValueError:
                pass

        _ensure_route_has_double_depot(r1)
        _ensure_route_has_double_depot(r2)

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
        # skip if target route cannot accept insertion (needs at least depot pair)
        if len(r_to.stops) <= 1:
            continue
        to_pos = rng.randint(1, max(1, len(r_to.stops) - 1))
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
        if best_move is not None:
            tabu[best_move.attr] = tabu_tenure
        for a in list(tabu.keys()):
            tabu[a] -= 1
            if tabu[a] <= 0:
                del tabu[a]

        if current.objective < best.objective:
            best = current.copy()

    return best

# ============================================================
# 8. LOAD DATA & EXAMPLE RUN
# ============================================================

import os
import glob
import pandas as pd

def load_data():
    # Thư mục chứa file lastmile_solver.py
    if "__file__" in globals():
        this_dir = os.path.dirname(os.path.abspath(__file__))
    else:
        this_dir = os.getcwd()

    project_root = os.path.dirname(this_dir)
    data_root = os.path.join(project_root, "Zzz_data", "LMDO data_3i")

    customers_path = os.path.join(data_root, "customers_vietnam.xlsx")
    depots_path    = os.path.join(data_root, "depots_vietnam.xlsx")
    vehicles_path  = os.path.join(data_root, "vehicles_vietnam.xlsx")

    roads_pattern = os.path.join(data_root, "roads", "**", "roads_*.csv")
    road_files = glob.glob(roads_pattern, recursive=True)

    print("=== DEBUG PATH ===")
    print("data_root:", data_root)
    print("Looking for roads pattern:", roads_pattern)
    print("Found roads:", len(road_files))

    for f in road_files:
        print("  -", f)

    if not road_files:
        raise FileNotFoundError("Không tìm thấy bất kỳ file roads_*.csv nào!")

    customers_df = pd.read_excel(customers_path)
    depots_df    = pd.read_excel(depots_path)
    vehicles_df  = pd.read_excel(vehicles_path)
    roads_df     = pd.concat([pd.read_csv(f) for f in road_files], ignore_index=True)

    return customers_df, depots_df, vehicles_df, roads_df

def build_initial_solution(inst: Instance) -> Solution:
    """
    Initial: mỗi xe chỉ có depot start (chưa gán khách).
    Route kiểu [depot, depot] để dễ chèn.
    """
    routes = {}
    for vid in inst.vehicles:
        d = inst.depots.get(vid, None)
        if d is None:
            # if missing depot mapping, pick arbitrary depot if any
            d = next(iter(inst.depot_capacity.keys()), None)
            if d is None:
                d = ""  # fallback
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
    import traceback

    print(">>> BẮT ĐẦU CHẠY ALNS D001")
    try:
        best_alns = example_run_alns("D001")
        print(">>> ALNS D001 DONE, OBJ =", best_alns.objective)
    except Exception as e:
        print(">>> LỖI KHI CHẠY ALNS D001:", e)
        traceback.print_exc()

    print("\n>>> BẮT ĐẦU CHẠY TABU D001")
    try:
        best_tabu = example_run_tabu("D001")
        print(">>> TABU D001 DONE, OBJ =", best_tabu.objective)
    except Exception as e:
        print(">>> LỖI KHI CHẠY TABU D001:", e)
        traceback.print_exc()
