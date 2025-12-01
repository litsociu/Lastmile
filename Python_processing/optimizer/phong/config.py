from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Set, Tuple, Any
import math
import os
import pandas as pd
from collections import defaultdict
from .utils import time_str_to_min, geo_distance

@dataclass
class Instance:
    customers: Set[str]
    vehicles: List[str]
    depots: Dict[str, str]               # vehicle_id -> depot_id
    depot_capacity: Dict[str, float]     # storage cap

    distance: Dict[str, Dict[str, float]]
    travel_time: Dict[str, Dict[str, float]]
    road_allowed: Dict[str, Dict[str, Dict[str, int]]]

    # --- Tham số khách hàng ---
    demand_w: Dict[str, float]
    demand_v: Dict[str, float]
    service_time: Dict[str, float]

    # Time window CHỈ cho khách
    tw_start: Dict[str, float]
    tw_end: Dict[str, float]

    priority: Dict[str, int]
    delivery_type: Dict[str, str]
    coords: Dict[str, Tuple[float,float]]
    customer_cluster: Dict[str, str]

    # --- Tham số kho (depot) ---
    depot_open: Dict[str, float]
    depot_close: Dict[str, float]

    # --- Tham số xe ---
    vehicle_cap_w: Dict[str, float]
    vehicle_cap_v: Dict[str, float]
    shift_max: Dict[str, float]
    max_distance: Dict[str, float]
    fixed_cost: Dict[str, float]
    var_cost: Dict[str, float]

    # --- Penalty & lambda ---
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
    meta: Dict[str, Any] = field(default_factory=dict)

    def copy(self) -> "Route":
        return Route(
            vehicle_id=self.vehicle_id, 
            stops=list(self.stops),
            meta={k: v for k, v in self.meta.items()}
        )


@dataclass
class Solution:
    """
    Solution with Multi-Trip support.
    routes: Dict[vehicle_id, List[Route]] - Each vehicle can have multiple trips
    """
    routes: Dict[str, List[Route]]
    all_customers: Set[str]
    objective: float = math.inf
    meta: Dict[str, Any] = field(default_factory=dict)

    def copy(self) -> "Solution":
        return Solution(
            routes={k: [r.copy() for r in route_list] for k, route_list in self.routes.items()},
            all_customers=set(self.all_customers),
            objective=self.objective,
            meta={k: v for k, v in self.meta.items()},
        )
    
    def get_all_routes(self) -> List[Route]:
        """Flatten all routes from all vehicles"""
        all_routes = []
        for route_list in self.routes.values():
            all_routes.extend(route_list)
        return all_routes

def load_data(base_dir: str = None):
    """Load all 4 data files with validation"""
    if base_dir is None:
        default_path = r"D:\A UEH_UNIVERSITY\UEH_Subjects\operation reseach\LMDO\Lastmile\Zzz_data\LMDO processed\Ho_Chi_Minh_City"
        if os.path.exists(default_path):
            base_dir = default_path
        else:
            base_dir = "." 
            print(f"[WARN] Không tìm thấy đường dẫn mặc định, đang thử: {os.path.abspath(base_dir)}")

    customers_path = os.path.join(base_dir, "customers.xlsx")
    depots_path    = os.path.join(base_dir, "depots.xlsx")
    vehicles_path  = os.path.join(base_dir, "vehicles.xlsx")
    roads_path     = os.path.join(base_dir, "roads.xlsx")

    required_files = {
        'customers': customers_path,
        'depots': depots_path,
        'vehicles': vehicles_path,
        'roads': roads_path
    }
    
    missing_files = []
    for name, path in required_files.items():
        if not os.path.exists(path):
            missing_files.append(f"{name} ({path})")
    
    if missing_files:
        raise FileNotFoundError(
            f"Không tìm thấy các file sau:\n" + 
            "\n".join(f"  - {f}" for f in missing_files)
        )
    
    print(f"[INFO] Loading data from: {base_dir}")
    print("[INFO] Loading customers...")
    customers_df = pd.read_excel(customers_path)
    print(f"[INFO] ✓ Customers: {len(customers_df)} rows")
    
    print("[INFO] Loading depots...")
    depots_df = pd.read_excel(depots_path)
    print(f"[INFO] ✓ Depots: {len(depots_df)} rows")
    
    print("[INFO] Loading vehicles...")
    vehicles_df = pd.read_excel(vehicles_path)
    print(f"[INFO] ✓ Vehicles: {len(vehicles_df)} rows")
    
    print("[INFO] Loading roads...")
    roads_df = pd.read_excel(roads_path)
    print(f"[INFO] ✓ Roads: {len(roads_df)} rows")
    
    # Validate columns
    required_customer_cols = ['Customer_ID', 'Order_Weight', 'Order_Volume', 'Latitude', 'Longitude']
    if not all(col in customers_df.columns for col in required_customer_cols):
        raise ValueError(f"customers.xlsx thiếu cột bắt buộc")
    
    print("[INFO] ✓ All data validated")
    return customers_df, depots_df, vehicles_df, roads_df

def build_instance(
    depot_prefix: str,
    customers_df: pd.DataFrame,
    depots_df: pd.DataFrame,
    vehicles_df: pd.DataFrame,
    roads_df: pd.DataFrame,
) -> Instance:
    """Build Instance from dataframes"""
    
    print(f"\n[INFO] Building instance for depot: {depot_prefix}")
    
    # VEHICLES
    veh_sub = vehicles_df[vehicles_df["Start_Depot_ID"].str.startswith(depot_prefix)].copy()
    vehicle_ids = veh_sub["Vehicle_ID"].tolist()
    if not vehicle_ids:
        raise ValueError(f"Không có xe cho depot '{depot_prefix}'")
    
    print(f"[INFO] ✓ Found {len(vehicle_ids)} vehicles")

    depots_map = {row["Vehicle_ID"]: row["Start_Depot_ID"] for _, row in veh_sub.iterrows()}
    max_distance = {row["Vehicle_ID"]: float(row["Max_Distance"]) for _, row in veh_sub.iterrows()}

    # ROADS
    roads_sub = roads_df[roads_df["Origin_Node_ID"].str.startswith(depot_prefix)].copy()
    
    if roads_sub.empty:
         print(f"[WARN] Không có roads data cho {depot_prefix}")

    dest_nodes = set(roads_sub["Destination_Node_ID"].unique())
    all_customer_ids = set(customers_df["Customer_ID"].unique())

    intersection = dest_nodes & all_customer_ids

    if intersection:
        customers_in_instance = intersection
        print(f"[INFO] ✓ Using {len(customers_in_instance)} customers from intersections")
    else:
        customers_in_instance = all_customer_ids
        print(f"[INFO] Using all {len(customers_in_instance)} customers")

    cust_sub = customers_df[customers_df["Customer_ID"].isin(customers_in_instance)].copy()

    # CUSTOMER PARAMS
    demand_w = {}
    demand_v = {}
    service_time = {}
    tw_start = {}
    tw_end = {}
    priority_map = {}
    delivery_type = {}
    coords = {}

    for _, r in cust_sub.iterrows():
        cid = r["Customer_ID"]
        demand_w[cid] = float(r["Order_Weight"])
        demand_v[cid] = float(r["Order_Volume"])
        service_time[cid] = float(r["Service_Time"])
        tw_start[cid] = float(time_str_to_min(r["Time_Window_Start"]))
        tw_end[cid] = float(time_str_to_min(r["Time_Window_End"]))
        priority_map[cid] = int(r["Priority_Level"])
        delivery_type[cid] = str(r["Delivery_Type"])
        coords[cid] = (float(r["Latitude"]), float(r["Longitude"]))

    # DEPOT PARAMS
    depots_sub = depots_df[depots_df["Depot_ID"].str.startswith(depot_prefix)].copy()
    depot_capacity = {r["Depot_ID"]: float(r["Capacity_Storage"]) for _, r in depots_sub.iterrows()}
    
    depot_open = {}
    depot_close = {}
    depot_coords = {}
    
    for _, r in depots_sub.iterrows():
        d_id = r["Depot_ID"]
        lat = float(r["Latitude"])
        lon = float(r["Longitude"])
        depot_coords[d_id] = (lat, lon)
        coords[d_id] = (lat, lon)
        depot_open[d_id] = 0.0
        depot_close[d_id] = 24 * 60.0

    # VEHICLE PARAMS
    vehicle_cap_w = {r["Vehicle_ID"]: float(r["Capacity_Weight"]) for _, r in veh_sub.iterrows()}
    vehicle_cap_v = {r["Vehicle_ID"]: float(r["Capacity_Volume"]) for _, r in veh_sub.iterrows()}
    fixed_cost = {r["Vehicle_ID"]: float(r["Fixed_Cost"]) for _, r in veh_sub.iterrows()}
    var_cost   = {r["Vehicle_ID"]: float(r["Variable_Cost"]) for _, r in veh_sub.iterrows()}
    shift_max  = {r["Vehicle_ID"]: float(r["Max_Working_Hours"])*60 for _, r in veh_sub.iterrows()}
    vehicle_type = {r["Vehicle_ID"]: str(r["Vehicle_Type"]) for _, r in veh_sub.iterrows()}

    # DISTANCE / TIME MATRIX
    distance = defaultdict(dict)
    travel_time = defaultdict(dict)
    for _, r in roads_sub.iterrows():
        i = r["Origin_Node_ID"]
        j = r["Destination_Node_ID"]
        distance[i][j] = float(r["Distance_km"])
        travel_time[i][j] = float(r["Travel_Time_min"])

    # ROAD RESTRICTIONS
    HEAVY = {"Truck", "Van", "Heavy Truck"}
    if "Road_Restrictions" in roads_sub.columns:
        roads_sub["Road_Restrictions"] = roads_sub["Road_Restrictions"].fillna("None").astype(str)
    else:
        roads_sub["Road_Restrictions"] = "None"

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

    # CLUSTERING
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

    # PENALTIES
    penalty_unserved: Dict[str, float] = {}
    lambda_E: Dict[str, float] = {}
    lambda_L: Dict[str, float] = {}

    for cid in customers_in_instance:
        phi = priority_map[cid]
        w_i = max(demand_w[cid], 1.0)
        penalty_unserved[cid] = 1e5 * phi * w_i
        lambda_E[cid] = 0.05 * phi
        lambda_L[cid] = 1.0  * phi

    lambda_H = {vid: 0.05 * fixed_cost[vid] for vid in vehicle_ids}
    lambda_W = 5e-4
    lambda_dist_overtime = 2.0
    lambda_depot_capacity = 0.5
    BIG_CAP = 5e3
    BIG_ROAD = 5e3
    
    print(f"[INFO] ✓ Instance complete: {len(customers_in_instance)} customers, {len(vehicle_ids)} vehicles\n")

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
        depot_open=depot_open,
        depot_close=depot_close,
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
