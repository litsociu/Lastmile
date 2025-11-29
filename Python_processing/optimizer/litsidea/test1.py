# ============================================================
# LAST-MILE DELIVERY – FULL MODEL (PDF) + ALNS + TABU + CLUSTER
# ============================================================
# Map từ:
#   + customers_vietnam.xlsx
#   + depots_vietnam.xlsx
#   + vehicles_vietnam.xlsx
#   + roads_Dxxx_y.csv (D001_1..D010_5)
#   + multi-depot last-mile delivery problem (PDF)
#
# - Mô hình: multi-depot VRP với:
#   G=(V,E), C, D, K, q_i^w, q_i^v, [e_i, l_i], s_i, phi_i,
#   rho_u,v^k, sigma_k^w,v, tau_k^max, L_k^max, eta_d, alpha_k, beta_k, ...
#
# - Hàm mục tiêu mở rộng:
#   f = chi phí cố định + chi phí biến đổi
#       + phạt khách không phục vụ
#       + phạt vi phạm time window (sớm / trễ)
#       + phạt overtime
#       + phạt vượt quãng đường tối đa
#       + phạt vượt sức chứa depot
#       + phạt mất cân bằng workload giữa các xe
#
# - Thuật toán:
#   + ALNS (adaptive large neighborhood search)
#   + Tabu search (relocate + swap)
#   + Clustering theo depot gần nhất
# ============================================================

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Set, Tuple, Callable, Any
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

    # Customer parameters
    demand_w: Dict[str, float]         # q_i^w
    demand_v: Dict[str, float]         # q_i^v
    service_time: Dict[str, float]     # s_i (min)
    tw_start: Dict[str, float]         # e_i (min from 0h)
    tw_end: Dict[str, float]           # l_i
    priority: Dict[str, int]           # phi_i (1,2,3)
    delivery_type: Dict[str, str]      # theta_i (Home/Locker)
    coords: Dict[str, Tuple[float,float]]  # (lat, lon)
    customer_cluster: Dict[str, str]   # cluster id = depot_id gần nhất

    # Vehicle parameters
    vehicle_cap_w: Dict[str, float]    # sigma_k^w
    vehicle_cap_v: Dict[str, float]    # sigma_k^v
    shift_max: Dict[str, float]        # tau_k^max (minutes)
    max_distance: Dict[str, float]     # L_k^max
    fixed_cost: Dict[str, float]       # alpha_k
    var_cost: Dict[str, float]         # beta_k

    # Penalty coefficients
    penalty_unserved: Dict[str, float]       # P_i
    lambda_E: Dict[str, float]               # early penalty
    lambda_L: Dict[str, float]               # late penalty
    lambda_H: Dict[str, float]               # overtime penalty per vehicle
    lambda_W: float                          # workload balance
    lambda_dist_overtime: float              # exceed max distance
    lambda_depot_capacity: float             # exceed depot capacity

    # BIG penalties
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
    Approx Euclidean distance in km (for clustering).
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
    Xây instance cho 1 cụm đường Dxxx (VD: "D001"):
    - K_d: tất cả vehicles có Start_Depot_ID bắt đầu bằng depot_prefix.
    - E: tất cả cung roads có Origin_Node_ID bắt đầu bằng depot_prefix.
    - C: tất cả khách xuất hiện trong Destination_Node_ID ∩ customers_vietnam.
    """

    # --- Vehicles K_d ---
    veh_sub = vehicles_df[vehicles_df["Start_Depot_ID"].str.startswith(depot_prefix)].copy()
    vehicle_ids = veh_sub["Vehicle_ID"].tolist()
    if not vehicle_ids:
        raise ValueError(f"Không có xe nào cho prefix {depot_prefix}")

    depots_map = {row["Vehicle_ID"]: row["Start_Depot_ID"] for _, row in veh_sub.iterrows()}
    max_distance = {row["Vehicle_ID"]: float(row["Max_Distance"]) for _, row in veh_sub.iterrows()}

    # --- Roads E ---
    roads_sub = roads_df[roads_df["Origin_Node_ID"].str.startswith(depot_prefix)].copy()
    if roads_sub.empty:
        raise ValueError(f"Không có roads cho prefix {depot_prefix}")

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
        service_time[cid] = float(row["Service_Time"])       # phút
        tw_start[cid] = float(time_str_to_min(row["Time_Window_Start"]))
        tw_end[cid] = float(time_str_to_min(row["Time_Window_End"]))
        priority[cid] = int(row["Priority_Level"])
        delivery_type[cid] = str(row["Delivery_Type"])
        coords[cid] = (float(row["Latitude"]), float(row["Longitude"]))

    # --- Depot params ---
    depots_sub = depots_df[depots_df["Depot_ID"].str.startswith(depot_prefix)].copy()
    depot_capacity = {row["Depot_ID"]: float(row["Capacity_Storage"]) for _, row in depots_sub.iterrows()}

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
    var_cost   = {row["Vehicle_ID"]: float(row["Variable_Cost"]) for _, row in veh_sub.iterrows()}
    shift_max  = {row["Vehicle_ID"]: float(row["Max_Working_Hours"]) * 60.0 for _, row in veh_sub.iterrows()}

    # --- Distance / Time matrix ---
    distance: Dict[str, Dict[str, float]] = defaultdict(dict)
    travel_time: Dict[str, Dict[str, float]] = defaultdict(dict)

    for _, row in roads_sub.iterrows():
        i = row["Origin_Node_ID"]
        j = row["Destination_Node_ID"]
        distance[i][j] = float(row["Distance_km"])
        travel_time[i][j] = float(row["Travel_Time_min"])

    # --- Road restrictions rho_u,v^k ---
    HEAVY_TYPES = {"Truck", "Heavy Truck", "Lorry"}
    vehicle_type = {row["Vehicle_ID"]: str(row["Vehicle_Type"]) for _, row in veh_sub.iterrows()}
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
            road_allowed[vid][i][j] = allow

    # --- Clustering: depot gần nhất ---
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

    # --- Penalties ---
    penalty_unserved: Dict[str, float] = {}
    lambda_E: Dict[str, float] = {}
    lambda_L: Dict[str, float] = {}
    for cid in customers_in_instance:
        phi = priority[cid]  # 1,2,3
        penalty_unserved[cid] = 500.0 * phi * max(demand_w[cid], 1.0)
        lambda_E[cid] = 0.5 * phi
        lambda_L[cid] = 5.0 * phi

    lambda_H: Dict[str, float] = {}
    for vid in vehicle_ids:
        lambda_H[vid] = fixed_cost[vid]

    lambda_W = 0.01
    lambda_dist_overtime   = 10.0
    lambda_depot_capacity  = 10.0

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

def evaluate(
    sol: Solution,
    inst: Instance,
    debug: bool = False,
    max_print_violations: int = 30,
) -> float:
    """
    Hàm mục tiêu mở rộng của bài toán multi-depot last-mile:

    f =  sum_k alpha_k * u_k                        (chi phí cố định mở tuyến)
       + sum_k beta_k * W_k                         (chi phí quãng đường)
       + sum_{i unserved} P_i                       (phạt khách không phục vụ)
       + sum_{i served} [ λ_E_i * (early_i) + λ_L_i * (late_i) ]
       + sum_k λ_H_k * (overtime_k)+                (phạt vượt ca làm việc)
       + sum_k λ_dist * (W_k - L_k^max)+            (phạt vượt quãng đường cho phép)
       + sum_d λ_depot * (load_d - eta_d)+         (phạt vượt sức chứa kho)
       + λ_W * ∑_k (W_k - avgW)^2                  (phạt mất cân bằng workload)
       + BIG_ROAD * #(cung (i,j) bị cấm nhưng vẫn đi)
       + BIG_CAP  * mức độ vượt tải xe (w,v)

    Đầu ra:
      - sol.objective: giá trị f
      - sol.meta["components"]: tổng từng thành phần penalty
      - sol.meta["violations"]: chi tiết các vi phạm để debug
    """

    # ----- Tổng chi phí / penalty -----
    total_fixed = 0.0           # ∑_k alpha_k * u_k
    total_dist_cost = 0.0       # ∑_k beta_k * W_k
    total_unserved_pen = 0.0    # ∑_i P_i
    total_tw_pen = 0.0          # ∑_i (λ_E_i * early_i + λ_L_i * late_i)
    total_overtime_pen = 0.0    # ∑_k λ_H_k * overtime_k+
    total_cap_pen = 0.0         # ∑_k BIG_CAP * mức vượt capacity
    total_road_pen = 0.0        # BIG_ROAD * #(cung cấm)
    total_dist_over_pen = 0.0   # λ_dist * (W_k - L_k^max)+
    total_depot_cap_pen = 0.0   # λ_depot * (load_d - eta_d)+
    total_workload_pen = 0.0    # λ_W * ∑(W_k - avgW)^2

    # ----- Để thống kê / debug -----
    visited: Set[str] = set()
    W: Dict[str, float] = {}        # W_k: tổng quãng đường mỗi xe
    depot_load: Dict[str, float] = defaultdict(float)

    # List chi tiết vi phạm (để in / xem trong sol.meta["violations"])
    cap_violations = []       # [(vid, node, load_w, cap_w, load_v, cap_v), ...]
    tw_violations = []        # [(cid, arrival, e_i, l_i, early, late), ...]
    road_violations = []      # [(vid, i, j), ...]
    overtime_violations = []  # [(vid, t, tau_k_max), ...]
    dist_over_violations = [] # [(vid, W_k, L_k_max), ...]
    depot_violations = []     # [(depot_id, load, cap), ...]

    # ============================================================
    # 1. DUYỆT TỪNG TUYẾN CỦA MỖI XE
    #    -> tính chi phí, tải, thời gian, vi phạm đường, TW, capacity
    # ============================================================
    for vid, route in sol.routes.items():
        stops = route.stops
        if len(stops) <= 1:
            # xe không thực sự đi đâu: W_k = 0
            W[vid] = 0.0
            continue

        # u_k = 1 => cộng chi phí cố định
        total_fixed += inst.fixed_cost[vid]

        load_w = 0.0
        load_v = 0.0
        t = 0.0              # thời gian tích lũy trên tuyến (phút)
        dist_k = 0.0         # tổng quãng đường xe k

        depot_id = inst.depots[vid]
        allowed_for_vid = inst.road_allowed.get(vid, {})

        # Duyệt từng cung (i -> j) trên route
        for i, j in zip(stops[:-1], stops[1:]):
            # 1a) Ràng buộc đường: nếu rho_u,v^k = 0 => phạt BIG_ROAD
            allow_ij = allowed_for_vid.get(i, {}).get(j, 1)  # mặc định allowed
            if allow_ij == 0:
                total_road_pen += inst.BIG_ROAD
                road_violations.append((vid, i, j))

            # 1b) Tính distance/time trên cung
            d_ij = inst.distance.get(i, {}).get(j, 0.0)
            t_ij = inst.travel_time.get(i, {}).get(j, 0.0)
            dist_k += d_ij
            t += t_ij

            # 1c) Nếu j là khách hàng -> cập nhật tải & time window
            if j in inst.customers:
                load_w += inst.demand_w[j]
                load_v += inst.demand_v[j]

                # --- Capacity constraint (weight/volume) ---
                if load_w > inst.vehicle_cap_w[vid] or load_v > inst.vehicle_cap_v[vid]:
                    over_w = max(load_w - inst.vehicle_cap_w[vid], 0.0)
                    over_v = max(load_v - inst.vehicle_cap_v[vid], 0.0)
                    if over_w > 0 or over_v > 0:
                        total_cap_pen += (
                            inst.BIG_CAP
                            * (over_w / max(inst.vehicle_cap_w[vid], 1.0)
                               + over_v / max(inst.vehicle_cap_v[vid], 1.0))
                        )
                        cap_violations.append(
                            (vid, j, load_w, inst.vehicle_cap_w[vid],
                             load_v, inst.vehicle_cap_v[vid])
                        )

                # --- Time window constraint (soft): e_i, l_i ---
                a_j = t  # arrival time tại khách j
                E_j = max(inst.tw_start[j] - a_j, 0.0)  # đến sớm
                L_j = max(a_j - inst.tw_end[j], 0.0)    # đến trễ
                if E_j > 0 or L_j > 0:
                    tw_violations.append((j, a_j, inst.tw_start[j], inst.tw_end[j], E_j, L_j))
                total_tw_pen += inst.lambda_E[j] * E_j + inst.lambda_L[j] * L_j

                # --- Thời gian phục vụ tại khách ---
                t += inst.service_time[j]

                # Đánh dấu khách được phục vụ
                visited.add(j)
                depot_load[depot_id] += inst.demand_w[j]

        # Lưu W_k
        W[vid] = dist_k
        # Chi phí biến đổi: beta_k * W_k
        total_dist_cost += inst.var_cost[vid] * dist_k

        # --- Overtime constraint: tổng thời gian > tau_k^max ---
        overtime = max(t - inst.shift_max[vid], 0.0)
        if overtime > 0:
            total_overtime_pen += inst.lambda_H[vid] * overtime
            overtime_violations.append((vid, t, inst.shift_max[vid]))

        # --- Max distance constraint: W_k > L_k^max ---
        if dist_k > inst.max_distance[vid]:
            extra = dist_k - inst.max_distance[vid]
            total_dist_over_pen += inst.lambda_dist_overtime * extra
            dist_over_violations.append((vid, dist_k, inst.max_distance[vid]))

    # ============================================================
    # 2. PHẠT KHÁCH KHÔNG ĐƯỢC PHỤC VỤ
    # ============================================================
    for cid in inst.customers:
        if cid not in visited:
            total_unserved_pen += inst.penalty_unserved[cid]

    # ============================================================
    # 3. RÀNG BUỘC SỨC CHỨA DEPOT
    # ============================================================
    for d_id, load in depot_load.items():
        cap = inst.depot_capacity.get(d_id, float("inf"))
        if load > cap:
            over = load - cap
            total_depot_cap_pen += inst.lambda_depot_capacity * over
            depot_violations.append((d_id, load, cap))

    # ============================================================
    # 4. RÀNG BUỘC CÂN BẰNG WORKLOAD GIỮA CÁC XE
    # ============================================================
    if W:
        avgW = sum(W.values()) / len(W)
        for vid in W:
            total_workload_pen += inst.lambda_W * (W[vid] - avgW) ** 2
    else:
        avgW = 0.0

    # ============================================================
    # 5. GHÉP TẤT CẢ VÀO HÀM MỤC TIÊU
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

    # ============================================================
    # 6. IN DEBUG (OPTIONAL)
    # ============================================================
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

        # In chi tiết một số vi phạm đầu tiên
        def _head(lst):
            return lst[:max_print_violations]

        if viols["capacity"]:
            print("\n  *Capacity violations (vid, node, load_w, cap_w, load_v, cap_v):")
            for v in _head(viols["capacity"]):
                print("   ", v)

        if viols["time_window"]:
            print("\n  *Time-window violations (cid, arrival, e_i, l_i, early, late):")
            for v in _head(viols["time_window"]):
                print("   ", v)

        if viols["road"]:
            print("\n  *Road violations (vid, i, j):")
            for v in _head(viols["road"]):
                print("   ", v)

        if viols["overtime"]:
            print("\n  *Overtime violations (vid, t, tau_max):")
            for v in _head(viols["overtime"]):
                print("   ", v)

        if viols["distance_over"]:
            print("\n  *Max-distance violations (vid, W_k, L_k_max):")
            for v in _head(viols["distance_over"]):
                print("   ", v)

        if viols["depot_capacity"]:
            print("\n  *Depot capacity violations (depot_id, load, cap):")
            for v in _head(viols["depot_capacity"]):
                print("   ", v)

        print("===== END DEBUG EVALUATE =====\n")

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

def _fix_route_roundtrip(route: Route):
    """Đảm bảo route luôn dạng [depot, ..., depot]."""
    if not route.stops:
        return
    depot = route.stops[0]
    if route.stops[-1] != depot:
        route.stops.append(depot)
    if len(route.stops) == 1:
        route.stops.append(depot)

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

    Lưu ý:
    - Chỉ bắt buộc tồn tại cung i -> cid.
    - Nếu thiếu cid -> j, ta dùng:
        + d(j, cid) nếu tồn tại (giả sử đối xứng),
        + nếu vẫn không có thì cho 0.0 (hoặc 1 giá trị default).
    - Như vậy sẽ không bị "tắc" chỉ vì thiếu cạnh ngược trong dữ liệu roads.
    """
    stops = route.stops
    if not stops:
        return float("inf")

    i = stops[pos - 1]
    j = stops[pos] if pos < len(stops) else None

    # Lấy ma trận khoảng cách
    dist = inst.distance

    # --- d(i, cid): bắt buộc phải có (hoặc gần như bắt buộc) ---
    d_ic = dist.get(i, {}).get(cid, None)
    if d_ic is None:
        # thử đối xứng
        d_ic = dist.get(cid, {}).get(i, None)
    if d_ic is None:
        # nếu vẫn không có thì coi như không chèn được
        return float("inf")

    # --- d(cid, j): cố gắng lấy, nhưng mềm hơn ---
    d_cj = 0.0
    if j is not None:
        d_cj = dist.get(cid, {}).get(j, None)
        if d_cj is None:
            # thử đối xứng
            d_cj = dist.get(j, {}).get(cid, None)
        if d_cj is None:
            # nếu vẫn không có, cho 0.0 (hoặc 1 giá trị default nhỏ)
            d_cj = 0.0

    # --- d(i, j) cũ ---
    d_old = 0.0
    if j is not None:
        d_old = dist.get(i, {}).get(j, None)
        if d_old is None:
            d_old = dist.get(j, {}).get(i, 0.0)

    d_new = d_ic + d_cj
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
        best_delta_for_cid = None
        best_regret = -1.0

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
                    if delta < float("inf"):
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
    repair_states  = [OperatorState(name) for name in repair_ops]

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

        partial   = d_func(current.copy(), inst, rng)
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
        repair_states[ri].score  += reward
        repair_states[ri].times_used  += 1

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

        # LOG vòng lặp
                # LOG vòng lặp
        if it % 20 == 0 or it == 1 or it == max_iter:
            comps_cur = current.meta.get("components", {})
            comps_best = best.meta.get("components", {})
            print(f"[ALNS] it={it}, current={current.objective:.2f}, best={best.objective:.2f}, T={temperature:.2f}")
            print("   current components:", {k: round(v, 2) for k, v in comps_cur.items()})
            print("   best    components:", {k: round(v, 2) for k, v in comps_best.items()})


    print("[ALNS] Hoàn tất.")
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
        r_to   = new_sol.routes[to_vid]

        if from_pos < len(r_from.stops) and r_from.stops[from_pos] == cid:
            r_from.stops.pop(from_pos)
        if to_pos > len(r_to.stops):
            to_pos = len(r_to.stops)
        r_to.stops.insert(to_pos, cid)

    elif move.move_type == "swap":
        cid1, vid1, pos1, cid2, vid2, pos2 = move.data
        r1 = new_sol.routes[vid1]
        r2 = new_sol.routes[vid2]

        if pos1 < len(r1.stops) and pos2 < len(r2.stops):
            if r1.stops[pos1] == cid1 and r2.stops[pos2] == cid2:
                r1.stops[pos1], r2.stops[pos2] = r2.stops[pos2], r1.stops[pos1]

    return new_sol

def generate_neighbors(sol: Solution, inst: Instance, max_neighbors: int, rng: random.Random) -> List[Move]:
    moves: List[Move] = []
    veh_ids = list(sol.routes.keys())

    # (vid, pos, cid) cho mọi khách
    customer_positions = []
    for vid, route in sol.routes.items():
        for pos, node in enumerate(route.stops):
            if node in inst.customers:
                customer_positions.append((vid, pos, node))

    # Relocate
    for _ in range(max_neighbors // 2):
        if not customer_positions:
            break
        vid_from, pos_from, cid = rng.choice(customer_positions)
        vid_to = rng.choice(veh_ids)
        r_to = sol.routes[vid_to]
        if len(r_to.stops) <= 1:
            continue
        to_pos = rng.randint(1, len(r_to.stops) - 1)

        move = Move(
            move_type="relocate",
            data=(cid, vid_from, pos_from, vid_to, to_pos),
            attr=("relocate", cid, vid_from, vid_to),
        )
        moves.append(move)

    # Swap
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
    max_iter: int = 300,
    max_neighbors: int = 50,
    tabu_tenure: int = 15,
    rng_seed: int = 0,
) -> Solution:
    rng = random.Random(rng_seed)

    current = initial_solution.copy()
    evaluate(current, inst)
    best = current.copy()

    tabu: Dict[Tuple[Any, ...], int] = {}

    print(f"[TABU] Bắt đầu, objective initial = {current.objective:.2f}")

    for it in range(1, max_iter + 1):
        neighbors = generate_neighbors(current, inst, max_neighbors, rng)
        best_cand = None
        best_move = None
        best_val  = float("inf")

        for mv in neighbors:
            is_tabu = mv.attr in tabu and tabu[mv.attr] > 0
            cand = apply_move(current, mv, inst)
            F_new = evaluate(cand, inst)

            # Aspiration
            if is_tabu and F_new >= best.objective:
                continue

            if F_new < best_val:
                best_val = F_new
                best_cand = cand
                best_move = mv

        if best_cand is None:
            break

        current = best_cand

        if best_move is not None:
            tabu[best_move.attr] = tabu_tenure

        # giảm tenure
        to_remove = []
        for a in list(tabu.keys()):
            tabu[a] -= 1
            if tabu[a] <= 0:
                to_remove.append(a)
        for a in to_remove:
            del tabu[a]

        if current.objective < best.objective:
            best = current.copy()
        
        if it % 20 == 0 or it == 1 or it == max_iter:
            comps_cur = current.meta.get("components", {})
            comps_best = best.meta.get("components", {})
            print(f"[TABU] it={it}, current={current.objective:.2f}, best={best.objective:.2f}")
            print("   current components:", {k: round(v, 2) for k, v in comps_cur.items()})
            print("   best    components:", {k: round(v, 2) for k, v in comps_best.items()})

    print("[TABU] Hoàn tất.")
    return best

# ============================================================
# 8. LOAD DATA & EXAMPLE RUN
# ============================================================

def load_data():
    # Thư mục chứa file lastmile_solver.py
    this_dir = os.path.dirname(os.path.abspath(__file__))

    # Thư mục gốc project
    project_root = os.path.dirname(this_dir)

    # Đường dẫn tới thư mục data
    data_root = os.path.join(project_root, "Zzz_data", "LMDO data_3i")

    # File Excel
    customers_path = os.path.join(data_root, "customers_vietnam.xlsx")
    depots_path    = os.path.join(data_root, "depots_vietnam.xlsx")
    vehicles_path  = os.path.join(data_root, "vehicles_vietnam.xlsx")

    # File roads
    roads_pattern = os.path.join(data_root, "roads", "**", "roads_*.csv")
    road_files = glob.glob(roads_pattern, recursive=True)

    print("=== DEBUG PATH ===")
    print("data_root:", data_root)
    print("Looking for roads pattern:", roads_pattern)
    print("Found roads:", len(road_files))
    for f in road_files[:10]:
        print("  -", f)
    if len(road_files) > 10:
        print("  ...")

    if not road_files:
        raise FileNotFoundError("Không tìm thấy bất kỳ file roads_*.csv nào!")

    customers_df = pd.read_excel(customers_path)
    depots_df    = pd.read_excel(depots_path)
    vehicles_df  = pd.read_excel(vehicles_path)
    roads_df     = pd.concat([pd.read_csv(f) for f in road_files], ignore_index=True)

    print(f"[DATA] customers: {len(customers_df)}, depots: {len(depots_df)}, vehicles: {len(vehicles_df)}, roads rows: {len(roads_df)}")

    return customers_df, depots_df, vehicles_df, roads_df

def build_initial_solution(inst: Instance) -> Solution:
    """
    Initial: mỗi xe chỉ có [depot, depot].
    """
    routes = {}
    for vid in inst.vehicles:
        d = inst.depots[vid]
        routes[vid] = Route(vehicle_id=vid, stops=[d, d])
    return Solution(routes=routes, all_customers=inst.customers)

def example_run_alns(prefix: str = "D001"):
    customers_df, depots_df, vehicles_df, roads_df = load_data()
    print(f"[BUILD] Đang xây instance cho prefix {prefix} ...")
    inst = build_instance_for_depot_prefix(prefix, customers_df, depots_df, vehicles_df, roads_df)
    print(f"[BUILD] Done. |C|={len(inst.customers)}, |K|={len(inst.vehicles)}")

    init_sol = build_initial_solution(inst)
    evaluate(init_sol, inst)
    print(f"[INIT] Objective initial = {init_sol.objective:.2f}")

    destroy_ops = {
        "random":  lambda s, i, r: destroy_random(s, i, r, remove_ratio=0.05),
        "cluster": destroy_cluster,
        "shaw":    destroy_shaw_related,
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
        max_iter=50,      # để debug nhanh, sau có thể tăng
        rng_seed=1,
    )
    print(f"[ALNS] Best objective for {prefix}: {best.objective:.2f}")
    print("Components:", best.meta.get("components", {}))
    print("\n>>> DEBUG CONSTRAINTS FOR ALNS BEST")
    evaluate(best, inst, debug=True)
    return best


def example_run_tabu(prefix: str = "D001"):
    customers_df, depots_df, vehicles_df, roads_df = load_data()
    print(f"[BUILD] Đang xây instance cho prefix {prefix} ...")
    inst = build_instance_for_depot_prefix(prefix, customers_df, depots_df, vehicles_df, roads_df)
    print(f"[BUILD] Done. |C|={len(inst.customers)}, |K|={len(inst.vehicles)}")

    init_sol = build_initial_solution(inst)
    evaluate(init_sol, inst)
    print(f"[INIT] Objective initial = {init_sol.objective:.2f}")

    best = tabu_search(
        inst=inst,
        initial_solution=init_sol,
        max_iter=200,
        max_neighbors=80,
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
