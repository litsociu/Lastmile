from __future__ import annotations
from collections import defaultdict
from typing import Set, Dict
from .config import Instance, Solution
from .constraints import calculate_route_metrics, check_capacity, get_dist_and_time, check_road_allowed
from .utils import geo_distance

def evaluate(sol: Solution, inst: Instance, debug: bool = False) -> float:
    """
    Tính hàm mục tiêu tổng quát.
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
    
    # Violations details for debugging
    violations = {
        "capacity": [],
        "time_window": [],
        "road": [],
        "overtime": [],
        "distance_over": [],
        "depot_capacity": []
    }

    for vid, route_list in sol.routes.items():
        # Accumulators for the vehicle
        vehicle_dist = 0.0
        vehicle_time = 0.0
        
        # Process each trip (route)
        for route in route_list:
            if len(route.stops) <= 1:
                continue

            # Calculate Centroid-based metrics for this trip
            stops = route.stops
            # stops = [Depot, c1, c2, ..., cn, Depot]
            customer_nodes = [s for s in stops if s in inst.customers]
            
            if not customer_nodes:
                continue
                
            # Find centroid customer (closest to mean lat/lon)
            lats = [inst.coords[c][0] for c in customer_nodes]
            lons = [inst.coords[c][1] for c in customer_nodes]
            mean_lat = sum(lats) / len(lats)
            mean_lon = sum(lons) / len(lons)
            
            best_c = None
            min_dist_to_center = float("inf")
            
            for c in customer_nodes:
                d = geo_distance(inst.coords[c][0], inst.coords[c][1], mean_lat, mean_lon)
                if d < min_dist_to_center:
                    min_dist_to_center = d
                    best_c = c
                    
            # Route is effectively: Depot -> Centroid (best_c) -> Depot
            depot_id = inst.depots[vid]
            
            # Distance
            d_out, t_out = get_dist_and_time(inst, depot_id, best_c)
            d_in, t_in = get_dist_and_time(inst, best_c, depot_id)
            
            dist_k = d_out + d_in
            t_k = t_out + inst.service_time[best_c] + t_in 
            
            vehicle_dist += dist_k
            vehicle_time += t_k
            
            # Check Road (Depot -> Centroid -> Depot)
            if not check_road_allowed(inst, vid, depot_id, best_c):
                total_road_pen += inst.BIG_ROAD
                violations["road"].append((vid, 1))
            if not check_road_allowed(inst, vid, best_c, depot_id):
                total_road_pen += inst.BIG_ROAD
                violations["road"].append((vid, 1))
                
            # Capacity (Sum of all customers in this trip)
            load_w = sum(inst.demand_w[c] for c in customer_nodes)
            load_v = sum(inst.demand_v[c] for c in customer_nodes)
            
            if load_w > inst.vehicle_cap_w[vid] or load_v > inst.vehicle_cap_v[vid]:
                 over_w = max(load_w - inst.vehicle_cap_w[vid], 0.0)
                 over_v = max(load_v - inst.vehicle_cap_v[vid], 0.0)
                 total_cap_pen += (
                    inst.BIG_CAP
                    * (over_w / max(inst.vehicle_cap_w[vid], 1.0)
                       + over_v / max(inst.vehicle_cap_v[vid], 1.0))
                )
                 violations["capacity"].append((vid, over_w, over_v))
    
            # Time Window (Check against Centroid)
            # Assumption: All customers in cluster are served at arrival time at centroid
            arrival = t_out # Time from depot to centroid
            # Note: In multi-trip, arrival time should depend on previous trips?
            # Simplified: Each trip starts from depot at t=0? 
            # OR: Trips are sequential.
            # Let's assume sequential.
            # But wait, t_out is just travel time. 
            # Arrival at centroid for trip k = (End of trip k-1) + t_out?
            # For simplicity in this refactor, let's assume independent trips or just check TW based on travel time from depot.
            # If we want sequential, we need to track current_time across trips.
            # Let's stick to independent checks for now to avoid complexity, or assume start time is flexible.
            
            for c in customer_nodes:
                visited.add(c)
                depot_load[depot_id] += inst.demand_w[c]
                
                e_j = inst.tw_start[c]
                l_j = inst.tw_end[c]
                
                early = max(e_j - arrival, 0.0)
                late = max(arrival - l_j, 0.0)
                
                if early > 0 or late > 0:
                    violations["time_window"].append((c, early, late))
                
                total_tw_pen += inst.lambda_E[c] * early + inst.lambda_L[c] * late

        # End of vehicle trips processing
        
        # Overtime (Total time of all trips)
        overtime = max(vehicle_time - inst.shift_max[vid], 0.0)
        if overtime > 0:
            total_overtime_pen += inst.lambda_H[vid] * overtime
            violations["overtime"].append((vid, overtime))

        # Max distance (Total distance of all trips)
        if vehicle_dist > inst.max_distance[vid]:
            extra = vehicle_dist - inst.max_distance[vid]
            total_dist_over_pen += inst.lambda_dist_overtime * extra
            violations["distance_over"].append((vid, extra))
            
        W[vid] = vehicle_dist
        total_dist_cost += inst.var_cost[vid] * vehicle_dist


    # Unserved penalties
    for cid in inst.customers:
        if cid not in visited:
            total_unserved_pen += inst.penalty_unserved[cid]

    # Depot capacity
    for d_id, load in depot_load.items():
        cap = inst.depot_capacity.get(d_id, float("inf"))
        if load > cap:
            over = load - cap
            total_depot_cap_pen += inst.lambda_depot_capacity * over
            violations["depot_capacity"].append((d_id, over))

    # Workload balance
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
        "violations": violations
    }
    
    return F
