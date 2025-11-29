# lastmile_solver.py
"""
Full implementation for MD-HVRPTW (Multi-Depot Heterogeneous VRP with Time Windows)
on real directed road graphs where nodes are Depot_ID or Customer_ID.

Features:
- Data loading from Excel / CSV (customers_vietnam.xlsx, depots_vietnam.xlsx, vehicles_vietnam.xlsx, roads_*.csv)
- Builds NetworkX DiGraph per depot-prefix (filtering edges by origin prefix)
- Builds vehicle-specific graphs to respect "No Heavy Trucks" and other restrictions
- Precomputes shortest path distances and times per vehicle graph using Dijkstra (cached)
- Evaluation function for VRPTW with:
    - travel time accumulation, waiting, service time
    - time window early/late penalties (soft)
    - overtime, max-distance penalties
    - vehicle capacity (weight/volume) large penalty (hard-ish)
    - road infeasibility penalty (BIG)
    - depot capacity penalty
    - workload balance penalty (only active vehicles considered)
- ALNS with destroy (random, cluster, shaw) and repair (greedy, regret-k)
- Tabu Search local improvement (relocate, swap)
- Example run functions for ALNS and Tabu for a prefix like "D001"

Author: ChatGPT (adapted for user's dataset)
"""
from __future__ import annotations
import os
import glob
import math
import random
from dataclasses import dataclass, field
from typing import Dict, List, Set, Tuple, Callable, Any, Optional
from collections import defaultdict
import pandas as pd
import networkx as nx
import numpy as np
import time as pytime

# -------------------------
# 1. DATA STRUCTURES
# -------------------------

@dataclass
class Instance:
    customers: Set[str]                 # set of Customer_ID in this instance
    vehicles: List[str]                 # list of Vehicle_ID relevant to prefix
    depots: Dict[str, str]             # vehicle_id -> depot_id (Start_Depot_ID)
    depot_capacity: Dict[str, float]   # eta_d (Capacity_Storage)
    depot_open: Dict[str, float]       # depot open time in minutes
    depot_close: Dict[str, float]      # depot close time in minutes

    # Graphs per prefix (global graph) - nodes are IDs used in roads file
    graph: nx.DiGraph                   # master directed graph for this prefix (all edges)
    # For per-vehicle feasibilities we will build filtered graphs and shortest paths

    # Customer parameters
    demand_w: Dict[str, float]         # q_i^w
    demand_v: Dict[str, float]         # q_i^v
    service_time: Dict[str, float]     # s_i (minutes)
    tw_start: Dict[str, float]         # e_i (minutes from 0h)
    tw_end: Dict[str, float]           # l_i (minutes)
    priority: Dict[str, int]           # phi_i (1,2,3)
    delivery_type: Dict[str, str]      # "Home"/"Locker"
    coords: Dict[str, Tuple[float, float]]  # lat/lon for customers and depots if present
    customer_cluster: Dict[str, str]   # nearest depot_id in prefix

    # Vehicle parameters
    vehicle_type: Dict[str, str]       # Vehicle_ID -> Vehicle_Type (text)
    vehicle_cap_w: Dict[str, float]    # sigma_k^w
    vehicle_cap_v: Dict[str, float]    # sigma_k^v
    shift_max: Dict[str, float]        # tau_k^max (minutes)
    max_distance: Dict[str, float]     # L_k^max (km)
    fixed_cost: Dict[str, float]       # alpha_k
    var_cost: Dict[str, float]         # beta_k
    start_depot: Dict[str, str]        # vehicle start depot id
    end_depot: Dict[str, str]          # vehicle end depot id (if any)

    # Penalty coefficients
    penalty_unserved: Dict[str, float]
    lambda_E: Dict[str, float]
    lambda_L: Dict[str, float]
    lambda_H: Dict[str, float]
    lambda_W: float
    lambda_dist_overtime: float
    lambda_depot_capacity: float

    # Large penalties
    BIG_CAP: float = 1e7
    BIG_ROAD: float = 1e7

    # internal caches (filled after building per-vehicle graphs)
    vehicle_graphs: Dict[str, nx.DiGraph] = field(default_factory=dict)  # vid -> DiGraph filtered
    sp_dist_cache: Dict[str, Dict[str, Dict[str, float]]] = field(default_factory=dict)  # vid -> {u: {v: dist}}
    sp_time_cache: Dict[str, Dict[str, Dict[str, float]]] = field(default_factory=dict)

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

# -------------------------
# 2. UTILITIES
# -------------------------

def time_str_to_min(t: Any) -> int:
    """
    "09:30" -> 570 minutes. Accepts NaN -> returns 0.
    """
    try:
        if pd.isna(t):
            return 0
    except Exception:
        pass
    t = str(t).strip()
    if "-" in t:
        t = t.split("-")[0]
    try:
        h, m = t.split(":")
        return int(h) * 60 + int(m)
    except Exception:
        # fallback if not time-like
        return 0

def parse_operating_hours(oh: Any) -> Tuple[int, int]:
    """
    "06:00-22:00" -> (360, 1320)
    """
    try:
        if pd.isna(oh):
            return 0, 24*60
    except Exception:
        pass
    oh = str(oh).strip()
    if "-" in oh:
        s, e = oh.split("-", 1)
        return time_str_to_min(s), time_str_to_min(e)
    # fallback
    return 0, 24*60

def geo_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Approximate distance in km using local cos(latitude) correction.
    """
    dx = (lon2 - lon1) * math.cos((lat1 + lat2) * math.pi / 360.0)
    dy = (lat2 - lat1)
    return math.sqrt(dx*dx + dy*dy) * 111.0

# -------------------------
# 3. DATA LOADING & GRAPH BUILD
# -------------------------

def load_data(data_root: Optional[str] = None):
    """
    Load customers, depots, vehicles and roads from data_root directory.
    Default assumes project structure where data sits in Zzz_data/LMDO data_3i (as user used earlier).
    Returns dataframes.
    """
    if data_root is None:
        # attempt to follow user's earlier path heuristics
        this_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
        project_root = os.path.dirname(this_dir)
        data_root = os.path.join(project_root, "Cpp_optimizer")
    print(f"[load_data] Using data_root = {data_root}")

    customers_path = os.path.join(data_root, "customers.csv")
    depots_path = os.path.join(data_root, "depots.csv")
    vehicles_path = os.path.join(data_root, "vehicles.csv")
    roads_pattern = os.path.join(data_root, "roads.csv")
    road_files = glob.glob(roads_pattern, recursive=True)
    if not os.path.exists(customers_path):
        raise FileNotFoundError(f"customers file not found: {customers_path}")
    if not os.path.exists(depots_path):
        raise FileNotFoundError(f"depots file not found: {depots_path}")
    if not os.path.exists(vehicles_path):
        raise FileNotFoundError(f"vehicles file not found: {vehicles_path}")
    if not road_files:
        raise FileNotFoundError(f"No roads files found with pattern: {roads_pattern}")

    print(f"[load_data] Found {len(road_files)} roads CSV files.")
    customers_df = pd.read_excel(customers_path)
    depots_df = pd.read_excel(depots_path)
    vehicles_df = pd.read_excel(vehicles_path)
    roads_df = pd.concat([pd.read_csv(f) for f in road_files], ignore_index=True)
    return customers_df, depots_df, vehicles_df, roads_df

def build_instance_for_depot_prefix(
    depot_prefix: str,
    customers_df: pd.DataFrame,
    depots_df: pd.DataFrame,
    vehicles_df: pd.DataFrame,
    roads_df: pd.DataFrame,
) -> Instance:
    """
    Build Instance for a prefix like 'D001'. Assumes nodes in roads file are Depot_ID or Customer_ID
    (as per the user's supplied roads.xlsx).
    """
    # normalize columns
    for df, name in [(customers_df, "customers"), (depots_df, "depots"), (vehicles_df, "vehicles"), (roads_df, "roads")]:
        # ensure string types for id columns
        pass

    # select vehicles whose Start_Depot_ID startswith prefix
    vehicles_df["Start_Depot_ID"] = vehicles_df["Start_Depot_ID"].fillna("").astype(str)
    veh_sub = vehicles_df[vehicles_df["Start_Depot_ID"].str.startswith(depot_prefix)].copy()
    vehicle_ids = veh_sub["Vehicle_ID"].astype(str).tolist()
    if not vehicle_ids:
        raise ValueError(f"No vehicles found for prefix {depot_prefix}")

    # map vehicle->start depot
    depots_map = {str(row["Vehicle_ID"]): str(row["Start_Depot_ID"]) for _, row in veh_sub.iterrows()}

    # filter roads that belong to this prefix: since node ids include D001_... and Cxxxxx,
    # we select any edge where either origin or destination starts with prefix or origin starts with prefix.
    roads_df["Origin_Node_ID"] = roads_df["Origin_Node_ID"].fillna("").astype(str)
    roads_df["Destination_Node_ID"] = roads_df["Destination_Node_ID"].fillna("").astype(str)
    # selecting edges where origin or destination or either contains the prefix - to gather the local road graph.
    # Safer: choose edges where origin startswith prefix OR destination startswith prefix OR either node's prefix == depot_prefix
    roads_sub = roads_df[
        roads_df["Origin_Node_ID"].str.startswith(depot_prefix) |
        roads_df["Destination_Node_ID"].str.startswith(depot_prefix) |
        roads_df["Origin_Node_ID"].str.contains(depot_prefix) |
        roads_df["Destination_Node_ID"].str.contains(depot_prefix)
    ].copy()
    if roads_sub.empty:
        raise ValueError(f"No roads entries for prefix {depot_prefix}")

    # Build master directed graph for this prefix
    G = nx.DiGraph()
    # default values if missing
    def safe_float(x, default=0.0):
        try:
            if pd.isna(x):
                return default
            return float(x)
        except Exception:
            return default
    def safe_str(x, default=""):
        try:
            if pd.isna(x):
                return default
            return str(x).strip()
        except Exception:
            return default

    for _, row in roads_sub.iterrows():
        u = safe_str(row["Origin_Node_ID"])
        v = safe_str(row["Destination_Node_ID"])
        dist = safe_float(row.get("Distance_km", 0.0), 0.0)
        ttime = safe_float(row.get("Travel_Time_min", 0.0), 0.0)
        traffic = safe_str(row.get("Traffic_Level", "None"))
        restriction = safe_str(row.get("Road_Restrictions", "None"))
        velocity = safe_float(row.get("Velocity", 0.0), 0.0)
        # add edge with attributes
        G.add_edge(u, v, distance=dist, travel_time=ttime, traffic=traffic, restriction=restriction, velocity=velocity)

    # collect customer nodes present in the roads_sub destination or origin
    all_customer_ids = set(customers_df["Customer_ID"].astype(str).unique())
    nodes_in_graph = set(G.nodes())
    customers_in_instance = set([n for n in nodes_in_graph if n in all_customer_ids])
    if not customers_in_instance:
        # fallback: maybe customers_df includes nodes not in graph; raise explicit
        raise ValueError(f"No customers found in graph for prefix {depot_prefix}. Graph nodes sample: {list(nodes_in_graph)[:10]}")

    # subset customers DF to customers_in_instance
    cust_sub = customers_df[customers_df["Customer_ID"].astype(str).isin(customers_in_instance)].copy()

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
        cid = str(row["Customer_ID"])
        demand_w[cid] = safe_float(row.get("Order_Weight", 0.0), 0.0)
        demand_v[cid] = safe_float(row.get("Order_Volume", 0.0), 0.0)
        service_time[cid] = safe_float(row.get("Service_Time", 0.0), 0.0)
        tw_start[cid] = float(time_str_to_min(row.get("Time_Window_Start", 0)))
        tw_end[cid] = float(time_str_to_min(row.get("Time_Window_End", 24*60)))
        priority[cid] = int(safe_float(row.get("Priority_Level", 1), 1))
        delivery_type[cid] = safe_str(row.get("Delivery_Type", "Home"))
        lat = safe_float(row.get("Latitude", 0.0), 0.0)
        lon = safe_float(row.get("Longitude", 0.0), 0.0)
        coords[cid] = (lat, lon)

    # --- Depots subset ---
    depots_df["Depot_ID"] = depots_df["Depot_ID"].fillna("").astype(str)
    depots_sub = depots_df[depots_df["Depot_ID"].str.startswith(depot_prefix)].copy()
    depot_capacity = {}
    depot_open = {}
    depot_close = {}
    depot_coords = {}
    for _, row in depots_sub.iterrows():
        d_id = str(row["Depot_ID"])
        depot_capacity[d_id] = safe_float(row.get("Capacity_Storage", float("inf")), float("inf"))
        op_start, op_end = parse_operating_hours(row.get("Operating_Hours", None))
        depot_open[d_id] = op_start
        depot_close[d_id] = op_end
        lat = safe_float(row.get("Latitude", 0.0), 0.0)
        lon = safe_float(row.get("Longitude", 0.0), 0.0)
        depot_coords[d_id] = (lat, lon)

    # --- Vehicles params ---
    vehicle_type = {str(row["Vehicle_ID"]): safe_str(row.get("Vehicle_Type", "")) for _, row in veh_sub.iterrows()}
    vehicle_cap_w = {str(row["Vehicle_ID"]): safe_float(row.get("Capacity_Weight", float("inf")), float("inf")) for _, row in veh_sub.iterrows()}
    vehicle_cap_v = {str(row["Vehicle_ID"]): safe_float(row.get("Capacity_Volume", float("inf")), float("inf")) for _, row in veh_sub.iterrows()}
    fixed_cost = {str(row["Vehicle_ID"]): safe_float(row.get("Fixed_Cost", 0.0), 0.0) for _, row in veh_sub.iterrows()}
    var_cost = {str(row["Vehicle_ID"]): safe_float(row.get("Variable_Cost", 0.0), 0.0) for _, row in veh_sub.iterrows()}
    shift_max = {str(row["Vehicle_ID"]): safe_float(row.get("Max_Working_Hours", 0.0), 0.0) * 60.0 for _, row in veh_sub.iterrows()}
    max_distance = {str(row["Vehicle_ID"]): safe_float(row.get("Max_Distance", float("inf")), float("inf")) for _, row in veh_sub.iterrows()}
    start_depot = {str(row["Vehicle_ID"]): safe_str(row.get("Start_Depot_ID", "")) for _, row in veh_sub.iterrows()}
    end_depot = {str(row["Vehicle_ID"]): safe_str(row.get("End_Depot_ID", "")) if "End_Depot_ID" in veh_sub.columns else safe_str(row.get("Start_Depot_ID", "")) for _, row in veh_sub.iterrows()}

    # --- Clustering: nearest depot in prefix for each customer (using geo_distance if coords available) ---
    customer_cluster = {}
    # if depots present, assign nearest by geo distance; else assign None
    if depot_coords:
        for cid in customers_in_instance:
            clat, clon = coords.get(cid, (0.0,0.0))
            best_d = None
            best_dist = float("inf")
            for d_id, (dlat, dlon) in depot_coords.items():
                gd = geo_distance(clat, clon, dlat, dlon)
                if gd < best_dist:
                    best_dist = gd
                    best_d = d_id
            customer_cluster[cid] = best_d
    else:
        for cid in customers_in_instance:
            customer_cluster[cid] = None

    # --- Penalties setup ---
    penalty_unserved = {}
    lambda_E = {}
    lambda_L = {}
    for cid in customers_in_instance:
        phi = priority.get(cid, 1)
        # use max weight scaling but cap effect for very tiny orders
        base_weight = max(1.0, demand_w.get(cid, 0.0))
        penalty_unserved[cid] = 500.0 * phi * base_weight
        lambda_E[cid] = 0.5 * phi
        lambda_L[cid] = 5.0 * phi

    lambda_H = {}
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
        depot_open=depot_open,
        depot_close=depot_close,
        graph=G,
        demand_w=demand_w,
        demand_v=demand_v,
        service_time=service_time,
        tw_start=tw_start,
        tw_end=tw_end,
        priority=priority,
        delivery_type=delivery_type,
        coords=coords,
        customer_cluster=customer_cluster,
        vehicle_type=vehicle_type,
        vehicle_cap_w=vehicle_cap_w,
        vehicle_cap_v=vehicle_cap_v,
        shift_max=shift_max,
        max_distance=max_distance,
        fixed_cost=fixed_cost,
        var_cost=var_cost,
        start_depot=start_depot,
        end_depot=end_depot,
        penalty_unserved=penalty_unserved,
        lambda_E=lambda_E,
        lambda_L=lambda_L,
        lambda_H=lambda_H,
        lambda_W=lambda_W,
        lambda_dist_overtime=lambda_dist_overtime,
        lambda_depot_capacity=lambda_depot_capacity,
    )

    # build per-vehicle filtered graphs and compute shortest-path caches
    build_vehicle_graphs_and_caches(inst)

    return inst

def build_vehicle_graphs_and_caches(inst: Instance):
    """
    For each vehicle, build a filtered graph that removes edges forbidden to that vehicle.
    Then precompute single-source Dijkstra shortest distances and times for nodes in that graph.
    Results stored in inst.vehicle_graphs, inst.sp_dist_cache, inst.sp_time_cache.
    """
    HEAVY_TYPES = {"Truck", "Heavy Truck", "Lorry"}  # adjust as needed
    G_master = inst.graph
    for vid in inst.vehicles:
        vtype = inst.vehicle_type.get(vid, "")
        # build filtered graph copy
        Gv = nx.DiGraph()
        for u, v, data in G_master.edges(data=True):
            restriction = str(data.get("restriction", "None")).strip()
            # check No Heavy Trucks
            if restriction == "No Heavy Trucks" and vtype in HEAVY_TYPES:
                continue
            # one-way is represented by single directed edge; reverse edge not present if not in master
            # if there are other restrictions you can add here
            # allowed -> add with same attributes
            Gv.add_edge(u, v, **data)
        # store graph
        inst.vehicle_graphs[vid] = Gv

        # Precompute shortest path distances & times from each node present (single-source Dijkstra).
        # We'll compute only for nodes that are stops: all nodes in graph. This can be heavy but correct.
        sp_dist = {}
        sp_time = {}
        # choose weight keys: distance and travel_time; ensure positive weights
        for src in Gv.nodes():
            try:
                dist_d, path_d = nx.single_source_dijkstra(Gv, src, weight="distance")
            except Exception:
                dist_d = {}
            try:
                time_d, path_t = nx.single_source_dijkstra(Gv, src, weight="travel_time")
            except Exception:
                time_d = {}
            sp_dist[src] = dist_d
            sp_time[src] = time_d
        inst.sp_dist_cache[vid] = sp_dist
        inst.sp_time_cache[vid] = sp_time

# -------------------------
# 4. EVALUATION: VRPTW correct
# -------------------------

def evaluate(sol: Solution, inst: Instance) -> float:
    """
    Evaluate a Solution with full objective (fixed costs + variable distance cost + penalties).
    - Uses shortest-path distances & times precomputed for each vehicle.
    - Arrival times follow: arrival = prev_departure + travel_time; start_service = max(arrival, tw_start); departure = start_service + service_time
    - Waiting (early) and Late incur penalties lambda_E, lambda_L per customer.
    - Overtime penalty if vehicle total time > shift_max
    - Distance over max penalty if dist > L_k^max
    - Vehicle capacity overloads generate BIG_CAP penalty (hard-ish)
    - Road infeasible (missing path) -> BIG_ROAD penalty
    - Depot capacity: sum of weights assigned to depot > capacity -> penalty
    - Workload balance between active vehicles penalized by lambda_W * squared deviation of distances
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
    dist_per_vehicle: Dict[str, float] = {}
    depot_load: Dict[str, float] = defaultdict(float)

    # iterate routes
    for vid, route in sol.routes.items():
        stops = route.stops
        if not stops or len(stops) < 2:
            # vehicle unused
            dist_per_vehicle[vid] = 0.0
            continue

        # ensure route begins and ends at start_depot or at least depot duplication
        start_dep = inst.start_depot.get(vid, None)
        if start_dep is None or start_dep == "":
            # fallback to first stop
            start_dep = stops[0]
        # treat fixed cost if vehicle used (has at least two stops and possibly customers)
        # vehicle considered used if route includes any customer node
        has_customer = any(node in inst.customers for node in stops)
        if has_customer:
            total_fixed += inst.fixed_cost.get(vid, 0.0)

        # initialize
        t_clock = 0.0  # time in minutes since midnight for the vehicle route (we assume vehicles start at depot open)
        load_w = 0.0
        load_v = 0.0
        dist_k = 0.0
        invalid_route = False

        # start time: if depot has opening, start at its open time (or 0)
        depid_for_route = inst.depots.get(vid, start_dep)
        if depid_for_route and depid_for_route in inst.depot_open:
            t_clock = float(inst.depot_open.get(depid_for_route, 0.0))
        else:
            t_clock = 0.0

        # traverse edges between consecutive stops, using shortest path in vehicle-specific graph
        spd = inst.sp_dist_cache.get(vid, {})
        spt = inst.sp_time_cache.get(vid, {})

        for a, b in zip(stops[:-1], stops[1:]):
            # find shortest path time/distance from a->b for this vehicle
            if a not in spd or b not in spd[a]:
                # no path in vehicle graph -> infeasible edge
                total_road_pen += inst.BIG_ROAD
                invalid_route = True
                break
            d_ab = spd[a][b]
            tt_ab = spt[a].get(b, 0.0)
            # add
            dist_k += d_ab
            t_clock += tt_ab

            # if b is customer -> service/waiting/TW
            if b in inst.customers:
                # update loads (delivery => unload at customer)
                load_w += inst.demand_w.get(b, 0.0)
                load_v += inst.demand_v.get(b, 0.0)

                # check capacity overload (hard-ish)
                cap_w = inst.vehicle_cap_w.get(vid, float("inf"))
                cap_v = inst.vehicle_cap_v.get(vid, float("inf"))
                if load_w > cap_w:
                    total_cap_pen += inst.BIG_CAP * (load_w - cap_w) / max(cap_w, 1.0)
                if load_v > cap_v:
                    total_cap_pen += inst.BIG_CAP * (load_v - cap_v) / max(cap_v, 1.0)

                # arrival
                arrival = t_clock
                # waiting if arrive early before tw_start
                e_j = inst.tw_start.get(b, 0.0)
                l_j = inst.tw_end.get(b, 24*60)
                if arrival < e_j:
                    wait = e_j - arrival
                    total_tw_pen += inst.lambda_E.get(b, 0.0) * wait
                    start_service = e_j
                else:
                    start_service = arrival
                # late penalty if arrive after end
                if arrival > l_j:
                    late = arrival - l_j
                    total_tw_pen += inst.lambda_L.get(b, 0.0) * late

                # service time
                t_clock = start_service + inst.service_time.get(b, 0.0)

                # record visited and depot load
                visited.add(b)
                if depid_for_route is not None:
                    depot_load[depid_for_route] += inst.demand_w.get(b, 0.0)

        # include return to depot if last stop not depot
        last_node = stops[-1]
        if last_node not in inst.depot_capacity:
            # check if route ends at depot node; if not, try to return to assigned end_depot
            end_depot = inst.end_depot.get(vid, None) or inst.start_depot.get(vid, None)
            if end_depot and last_node in inst.sp_dist_cache.get(vid, {}) and end_depot in inst.sp_dist_cache[vid].get(last_node, {}):
                # add travel back
                d_back = inst.sp_dist_cache[vid][last_node].get(end_depot, 0.0)
                tt_back = inst.sp_time_cache[vid][last_node].get(end_depot, 0.0)
                dist_k += d_back
                t_clock += tt_back
            else:
                # if cannot return -> penalize heavily
                # but still proceed
                total_road_pen += inst.BIG_ROAD

        # collect per-vehicle metrics
        dist_per_vehicle[vid] = dist_k
        total_dist_cost += inst.var_cost.get(vid, 0.0) * dist_k

        # overtime penalty
        overtime = max(0.0, t_clock - inst.shift_max.get(vid, float("inf")))
        if overtime > 0:
            total_overtime_pen += inst.lambda_H.get(vid, 0.0) * overtime

        # max distance penalty
        if dist_k > inst.max_distance.get(vid, float("inf")):
            extra = dist_k - inst.max_distance.get(vid, 0.0)
            total_dist_over_pen += inst.lambda_dist_overtime * extra

    # unserved customers
    for cid in inst.customers:
        if cid not in visited:
            total_unserved_pen += inst.penalty_unserved.get(cid, 0.0)

    # depot capacity penalty
    for d_id, load in depot_load.items():
        cap = inst.depot_capacity.get(d_id, float("inf"))
        if load > cap:
            total_depot_cap_pen += inst.lambda_depot_capacity * (load - cap)

    # workload balance: consider only active vehicles (dist>0)
    active_dists = [dist for dist in dist_per_vehicle.values() if dist > 0]
    if active_dists:
        avg = sum(active_dists) / len(active_dists)
        for dist in active_dists:
            total_workload_pen += inst.lambda_W * (dist - avg) ** 2

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
        "dist_per_vehicle": dist_per_vehicle,
        "depot_load": dict(depot_load),
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

# -------------------------
# 5. ALNS operators (destroy & repair)
# -------------------------

DestroyOp = Callable[[Solution, Instance, random.Random], Solution]
RepairOp = Callable[[Solution, Instance, random.Random], Solution]

@dataclass
class OperatorState:
    name: str
    weight: float = 1.0
    score: float = 0.0
    times_used: int = 0

def roulette_select(ops: List[OperatorState], rng: random.Random) -> int:
    total_w = sum(max(op.weight, 1e-9) for op in ops)
    r = rng.random() * total_w
    s = 0.0
    for i, op in enumerate(ops):
        s += max(op.weight, 1e-9)
        if s >= r:
            return i
    return len(ops)-1

def _ensure_route_has_double_depot(route: Route):
    """
    Ensure route invariants: if route has at least one depot, ensure [d, d] when no customers exist.
    """
    if not route.stops:
        return
    # there's at least one element - ensure last equals first (end depot)
    if len(route.stops) == 1:
        d = route.stops[0]
        route.stops[:] = [d, d]
    else:
        if route.stops[0] != route.stops[-1]:
            # if for whatever reason end not depot, keep as is but many functions expect end-depot.
            # we'll not forcibly duplicate here to avoid incorrect duplication if explicit end depot provided.
            pass

def destroy_random(sol: Solution, inst: Instance, rng: random.Random, remove_ratio: float = 0.1) -> Solution:
    new_sol = sol.copy()
    allc = list(inst.customers)
    rng.shuffle(allc)
    n_remove = max(1, int(len(allc) * remove_ratio))
    to_remove = set(allc[:n_remove])

    for r in new_sol.routes.values():
        if not r.stops:
            continue
        depot = r.stops[0]
        new_stops = [x for x in r.stops if (x not in to_remove) or (x == depot)]
        if not new_stops:
            new_stops = [depot, depot]
        r.stops = new_stops
        _ensure_route_has_double_depot(r)
    return new_sol

def destroy_cluster(sol: Solution, inst: Instance, rng: random.Random) -> Solution:
    new_sol = sol.copy()
    clusters = set(v for v in inst.customer_cluster.values() if v is not None)
    if not clusters:
        return new_sol
    chosen_cluster = rng.choice(list(clusters))
    to_remove = {cid for cid, cl in inst.customer_cluster.items() if cl == chosen_cluster}
    if not to_remove:
        return new_sol
    for r in new_sol.routes.values():
        if not r.stops:
            continue
        depot = r.stops[0]
        new_stops = [x for x in r.stops if (x not in to_remove) or (x == depot)]
        if not new_stops:
            new_stops = [depot, depot]
        r.stops = new_stops
        _ensure_route_has_double_depot(r)
    return new_sol

def destroy_shaw_related(sol: Solution, inst: Instance, rng: random.Random, remove_count: int = 20) -> Solution:
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
    to_remove = [seed] if seed in remaining else []
    if seed in remaining:
        remaining.remove(seed)

    while len(to_remove) < min(remove_count, len(inst.customers)) and remaining:
        last = rng.choice(to_remove)
        best_j = min(remaining, key=lambda j: relatedness(last, j))
        to_remove.append(best_j)
        remaining.remove(best_j)

    to_remove_set = set(to_remove)
    for r in new_sol.routes.values():
        if not r.stops:
            continue
        depot = r.stops[0]
        new_stops = [x for x in r.stops if (x not in to_remove_set) or (x == depot)]
        if not new_stops:
            new_stops = [depot, depot]
        r.stops = new_stops
        _ensure_route_has_double_depot(r)
    return new_sol

# ---------- REPAIR helpers ----------
def ensure_routes_have_end_depot(sol: Solution, inst: Instance):
    for vid, route in sol.routes.items():
        if not route.stops:
            # assign start depot if available
            sd = inst.start_depot.get(vid, None) or next(iter(inst.depot_capacity.keys()), "")
            if sd:
                route.stops[:] = [sd, sd]
            continue
        if len(route.stops) == 1:
            d = route.stops[0]
            route.stops[:] = [d, d]
        elif route.stops[0] != route.stops[-1]:
            # ensure last equal first if last is empty or not a depot
            # if end_depot specified and different, keep as is
            if route.stops[-1] not in inst.depot_capacity:
                route.stops.append(route.stops[0])

def insertion_cost_using_sp(route: Route, vid: str, cid: str, pos: int, inst: Instance) -> float:
    """
    Compute additional distance (and time) when inserting cid into route at position pos,
    using precomputed shortest-path distances for vehicle vid.
    Returns delta distance (float('inf') if infeasible).
    """
    stops = route.stops
    if not stops:
        return float("inf")
    if pos <= 0 or pos > len(stops):
        return float("inf")
    spd = inst.sp_dist_cache.get(vid, {})
    # nodes between which insertion occurs
    i = stops[pos-1]
    j = stops[pos] if pos < len(stops) else None
    # check i -> cid
    if i not in spd or cid not in spd[i]:
        return float("inf")
    d_i_c = spd[i][cid]
    # check cid -> j
    if j is not None:
        if cid not in spd or j not in spd[cid]:
            return float("inf")
        d_c_j = spd[cid][j]
        # old distance i->j
        if i in spd and j in spd[i]:
            d_i_j = spd[i][j]
        else:
            d_i_j = 0.0  # if i->j path didn't exist originally, treat old as 0 (we will be adding path)
        return d_i_c + d_c_j - d_i_j
    else:
        # inserting at end before nothing -> just i->cid
        return d_i_c

def repair_greedy(sol: Solution, inst: Instance, rng: random.Random) -> Solution:
    new_sol = sol.copy()
    ensure_routes_have_end_depot(new_sol, inst)
    evaluate(new_sol, inst)
    served = set(new_sol.meta.get("visited", set()))
    unserved = list(inst.customers - served)
    rng.shuffle(unserved)

    for cid in unserved:
        best_delta = float("inf")
        best_vid = None
        best_pos = None
        for vid, route in new_sol.routes.items():
            if not route.stops:
                continue
            # ensure end depot invariant
            if len(route.stops) == 1:
                route.stops.append(route.stops[0])
            for pos in range(1, len(route.stops)):
                delta = insertion_cost_using_sp(route, vid, cid, pos, inst)
                if delta < best_delta:
                    best_delta = delta
                    best_vid = vid
                    best_pos = pos
        if best_vid is not None and best_pos is not None and best_delta < float("inf"):
            new_sol.routes[best_vid].stops.insert(best_pos, cid)
            # optional: re-evaluate progressively for feasibility and updated meta
            evaluate(new_sol, inst)
    return new_sol

def repair_regret(sol: Solution, inst: Instance, rng: random.Random, k_regret: int = 2) -> Solution:
    new_sol = sol.copy()
    ensure_routes_have_end_depot(new_sol, inst)
    evaluate(new_sol, inst)
    served = set(new_sol.meta.get("visited", set()))
    unserved = list(inst.customers - served)

    while unserved:
        best_cid = None
        best_choice = None
        best_regret = -float("inf")
        for cid in unserved:
            candidates = []
            for vid, route in new_sol.routes.items():
                if not route.stops:
                    continue
                for pos in range(1, len(route.stops)):
                    delta = insertion_cost_using_sp(route, vid, cid, pos, inst)
                    if delta < float("inf"):
                        candidates.append((delta, vid, pos))
            if not candidates:
                continue
            candidates.sort(key=lambda x: x[0])
            best = candidates[0][0]
            if len(candidates) >= k_regret:
                second = candidates[k_regret-1][0]
            else:
                second = candidates[-1][0]
            regret = second - best
            if regret > best_regret:
                best_regret = regret
                best_cid = cid
                best_choice = candidates[0]

        if best_cid is None or best_choice is None:
            break
        delta, vid, pos = best_choice
        new_sol.routes[vid].stops.insert(pos, best_cid)
        evaluate(new_sol, inst)
        if best_cid in unserved:
            unserved.remove(best_cid)
    return new_sol

# -------------------------
# 6. ALNS main
# -------------------------

def alns(
    inst: Instance,
    initial_solution: Solution,
    destroy_ops: Dict[str, DestroyOp],
    repair_ops: Dict[str, RepairOp],
    max_iter: int = 500,
    segment_length: int = 50,
    reaction_factor: float = 0.2,
    start_temperature: float = 1000.0,
    end_temperature: float = 1.0,
    rng_seed: int = 0,
) -> Solution:
    rng = random.Random(rng_seed)
    destroy_states = [OperatorState(name) for name in destroy_ops.keys()]
    repair_states = [OperatorState(name) for name in repair_ops.keys()]

    current = initial_solution.copy()
    evaluate(current, inst)
    best = current.copy()

    temperature = start_temperature

    # main loop
    for it in range(1, max_iter+1):
        di = roulette_select(destroy_states, rng)
        ri = roulette_select(repair_states, rng)
        d_name = destroy_states[di].name
        r_name = repair_states[ri].name
        d_func = destroy_ops[d_name]
        r_func = repair_ops[r_name]

        # apply destroy & repair
        partial = d_func(current.copy(), inst, rng)
        candidate = r_func(partial, inst, rng)
        F_new = evaluate(candidate, inst)
        F_cur = current.objective
        F_best = best.objective

        # acceptance SA
        accept = False
        if F_new < F_cur:
            accept = True
        else:
            delta = F_new - F_cur
            if temperature > 1e-12:
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

        # update weights every segment
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

        # cooling schedule linear interpolation
        alpha = it / max_iter
        temperature = start_temperature * (1 - alpha) + end_temperature * alpha

        if it % 50 == 0 or it == 1:
            print(f"[ALNS] iter {it}/{max_iter}, current {current.objective:.2f}, best {best.objective:.2f}, temp {temperature:.4f}")

    return best

# -------------------------
# 7. TABU SEARCH (local improvement)
# -------------------------

@dataclass
class Move:
    move_type: str
    data: Any
    attr: Tuple[Any, ...]  # attribute used for tabu list

def apply_move(sol: Solution, move: Move, inst: Instance) -> Solution:
    new_sol = sol.copy()
    if move.move_type == "relocate":
        cid, from_vid, from_pos, to_vid, to_pos = move.data
        # validate
        if from_vid not in new_sol.routes or to_vid not in new_sol.routes:
            return new_sol
        r_from = new_sol.routes[from_vid]
        r_to = new_sol.routes[to_vid]
        # remove cid from r_from
        removed = False
        if 0 <= from_pos < len(r_from.stops) and r_from.stops[from_pos] == cid:
            r_from.stops.pop(from_pos)
            removed = True
        else:
            # attempt find and remove
            if cid in r_from.stops:
                r_from.stops.remove(cid)
                removed = True
        # insert in r_to at to_pos
        if not removed:
            # cannot relocate
            return new_sol
        if to_pos < 0:
            to_pos = 1
        if to_pos > len(r_to.stops):
            to_pos = len(r_to.stops)
        r_to.stops.insert(to_pos, cid)
        _ensure_route_has_double_depot(r_from)
        _ensure_route_has_double_depot(r_to)
    elif move.move_type == "swap":
        cid1, vid1, pos1, cid2, vid2, pos2 = move.data
        if vid1 not in new_sol.routes or vid2 not in new_sol.routes:
            return new_sol
        r1 = new_sol.routes[vid1]
        r2 = new_sol.routes[vid2]
        # attempt direct swap by index
        if 0 <= pos1 < len(r1.stops) and 0 <= pos2 < len(r2.stops) and r1.stops[pos1] == cid1 and r2.stops[pos2] == cid2:
            r1.stops[pos1], r2.stops[pos2] = r2.stops[pos2], r1.stops[pos1]
        else:
            # try find indices
            if cid1 in r1.stops and cid2 in r2.stops:
                i1 = r1.stops.index(cid1)
                i2 = r2.stops.index(cid2)
                r1.stops[i1], r2.stops[i2] = r2.stops[i2], r1.stops[i1]
        _ensure_route_has_double_depot(r1)
        _ensure_route_has_double_depot(r2)
    return new_sol

def generate_neighbors(sol: Solution, inst: Instance, max_neighbors: int, rng: random.Random) -> List[Move]:
    moves: List[Move] = []
    veh_ids = list(sol.routes.keys())
    # collect customer positions
    cust_pos = []
    for vid, route in sol.routes.items():
        for pos, node in enumerate(route.stops):
            if node in inst.customers:
                cust_pos.append((vid, pos, node))
    if not cust_pos:
        return moves
    # relocate moves
    for _ in range(max_neighbors // 2):
        vid_from, pos_from, cid = rng.choice(cust_pos)
        vid_to = rng.choice(veh_ids)
        r_to = sol.routes[vid_to]
        if len(r_to.stops) <= 1:
            continue
        to_pos = rng.randint(1, max(1, len(r_to.stops)-1))
        move = Move("relocate", (cid, vid_from, pos_from, vid_to, to_pos), ("relocate", cid, vid_from, vid_to))
        moves.append(move)
    # swap moves
    for _ in range(max_neighbors // 2):
        if len(cust_pos) < 2:
            break
        (vid1,pos1,cid1),(vid2,pos2,cid2) = rng.sample(cust_pos, 2)
        move = Move("swap", (cid1, vid1, pos1, cid2, vid2, pos2), ("swap", cid1, cid2))
        moves.append(move)
    return moves[:max_neighbors]

def tabu_search(
    inst: Instance,
    initial_solution: Solution,
    max_iter: int = 500,
    max_neighbors: int = 100,
    tabu_tenure: int = 15,
    rng_seed: int = 0,
) -> Solution:
    rng = random.Random(rng_seed)
    current = initial_solution.copy()
    evaluate(current, inst)
    best = current.copy()
    tabu: Dict[Tuple[Any, ...], int] = {}

    for it in range(1, max_iter+1):
        neighbors = generate_neighbors(current, inst, max_neighbors, rng)
        best_cand = None
        best_move = None
        best_val = float("inf")
        for mv in neighbors:
            is_tabu = mv.attr in tabu and tabu[mv.attr] > 0
            cand = apply_move(current, mv, inst)
            F_new = evaluate(cand, inst)
            # aspiration: allow tabu if improves global best
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
        # decrement tenures
        for a in list(tabu.keys()):
            tabu[a] -= 1
            if tabu[a] <= 0:
                del tabu[a]
        # update global best
        if current.objective < best.objective:
            best = current.copy()
        if it % 50 == 0:
            print(f"[TABU] iter {it}, current {current.objective:.2f}, best {best.objective:.2f}")
    return best

# -------------------------
# 8. Utility: initial solution builder & data loader examples
# -------------------------

def build_initial_solution(inst: Instance) -> Solution:
    """
    Start with each vehicle assigned to its depot and no customers (depot duplicated as [d,d]).
    """
    routes = {}
    for vid in inst.vehicles:
        d = inst.start_depot.get(vid, None) or inst.depots.get(vid, None) or next(iter(inst.depot_capacity.keys()), "")
        if not d:
            d = ""
        routes[vid] = Route(vehicle_id=vid, stops=[d, d])
    return Solution(routes=routes, all_customers=inst.customers)

# -------------------------
# 9. Example run functions
# -------------------------

def example_run_alns(prefix: str = "D001", data_root: Optional[str] = None):
    customers_df, depots_df, vehicles_df, roads_df = load_data(data_root)
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

    start_time = pytime.time()
    best = alns(inst, init_sol, destroy_ops, repair_ops, max_iter=400, segment_length=40, rng_seed=1)
    elapsed = pytime.time() - start_time
    print(f"[ALNS] Best objective for {prefix}: {best.objective:.2f} (time {elapsed:.1f}s)")
    print("Components:", best.meta.get("components", {}))
    return best

def example_run_tabu(prefix: str = "D001", data_root: Optional[str] = None):
    customers_df, depots_df, vehicles_df, roads_df = load_data(data_root)
    inst = build_instance_for_depot_prefix(prefix, customers_df, depots_df, vehicles_df, roads_df)
    init_sol = build_initial_solution(inst)
    # We'll do a small initial greedy repair to have some customers assigned
    init_sol = repair_greedy(init_sol, inst, random.Random(0))
    evaluate(init_sol, inst)
    best = tabu_search(inst, init_sol, max_iter=300, max_neighbors=80, tabu_tenure=15, rng_seed=2)
    print(f"[TABU] Best objective for {prefix}: {best.objective:.2f}")
    print("Components:", best.meta.get("components", {}))
    return best

# -------------------------
# 10. If run as script
# -------------------------

if __name__ == "__main__":
    import traceback
    print("=== START example_run_alns D001 ===")
    try:
        best_alns = example_run_alns("D001", None)
        print("ALNS done. Obj =", best_alns.objective)
    except Exception as e:
        print("Error during ALNS run:", e)
        traceback.print_exc()

    print("\n=== START example_run_tabu D001 ===")
    try:
        best_tabu = example_run_tabu("D001", None)
        print("TABU done. Obj =", best_tabu.objective)
    except Exception as e:
        print("Error during TABU run:", e)
        traceback.print_exc()
