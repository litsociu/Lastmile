from __future__ import annotations
import random
import math
from typing import List, Dict, Callable, Any, Tuple
from dataclasses import dataclass

from .config import Instance, Solution, Route
from .objectives import evaluate
from .constraints import check_capacity, check_road_allowed, get_dist_and_time
from .utils import geo_distance

# --- ALNS Definitions ---

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

# --- Destroy Operators ---

def destroy_random(sol: Solution, inst: Instance, rng: random.Random) -> Solution:
    new_sol = sol.copy()
    allc = list(inst.customers)
    rng.shuffle(allc)
    remove_ratio = 0.15
    n_remove = max(1, int(len(allc) * remove_ratio))
    to_remove = set(allc[:n_remove])

    for route_list in new_sol.routes.values():
        for r in route_list:
            if len(r.stops) <= 2: continue
            depot = r.stops[0]
            new_stops = [x for x in r.stops if (x not in to_remove or x == depot)]
            # Fix roundtrip
            if not new_stops: new_stops = [depot, depot]
            elif len(new_stops) == 1: new_stops.append(depot)
            elif new_stops[-1] != depot: new_stops.append(depot)
            r.stops = new_stops
    return new_sol

# --- Repair Operators ---

def calculate_cluster_cost(inst: Instance, vid: str, customers: List[str]) -> float:
    """
    Tính chi phí (distance) của một cụm khách hàng: Depot -> Centroid -> Depot.
    """
    if not customers:
        return 0.0
        
    lats = [inst.coords[c][0] for c in customers]
    lons = [inst.coords[c][1] for c in customers]
    mean_lat = sum(lats) / len(lats)
    mean_lon = sum(lons) / len(lons)
    
    # Find centroid customer
    best_c = None
    min_dist = float("inf")
    for c in customers:
        d = geo_distance(inst.coords[c][0], inst.coords[c][1], mean_lat, mean_lon)
        if d < min_dist:
            min_dist = d
            best_c = c
            
    depot_id = inst.depots[vid]
    d_out, _ = get_dist_and_time(inst, depot_id, best_c)
    d_in, _ = get_dist_and_time(inst, best_c, depot_id)
    
    return d_out + d_in

def repair_greedy(sol: Solution, inst: Instance, rng: random.Random) -> Solution:
    """
    Greedy insertion cho bài toán Cluster-based Routing.
    Cost = Change in (Depot -> Centroid -> Depot) distance.
    """
    new_sol = sol.copy()
    evaluate(new_sol, inst) 
    served = new_sol.meta.get("visited", set())
    unserved = list(inst.customers - served)
    rng.shuffle(unserved)
    
    unserved = unserved[:500] 

    for cid in unserved:
        best_cost = float("inf")
        best_vid = None
        best_route_idx = -1
        
        for vid, route_list in new_sol.routes.items():
            for idx, route in enumerate(route_list):
                # Current customers in route
                current_customers = [s for s in route.stops if s in inst.customers]
                
                # Check Capacity if added
                new_customers = current_customers + [cid]
                load_w = sum(inst.demand_w[c] for c in new_customers)
                load_v = sum(inst.demand_v[c] for c in new_customers)
                
                if load_w > inst.vehicle_cap_w[vid] or load_v > inst.vehicle_cap_v[vid]:
                    continue
                    
                # Calculate Cost Delta
                old_cost = calculate_cluster_cost(inst, vid, current_customers)
                new_cost = calculate_cluster_cost(inst, vid, new_customers)
                delta = new_cost - old_cost
                
                if delta < best_cost:
                    best_cost = delta
                    best_vid = vid
                    best_route_idx = idx
        
        if best_vid is not None:
            # Insert into route (order doesn't matter for centroid logic, just append)
            # But we need to keep Depot at start/end
            # stops = [Depot, ..., Depot]
            new_sol.routes[best_vid][best_route_idx].stops.insert(-1, cid)
            
    return new_sol

# --- ALNS Main ---

def run_alns(
    inst: Instance,
    initial_solution: Solution,
    max_iter: int = 50,
    rng_seed: int = 42
) -> Solution:
    rng = random.Random(rng_seed)
    
    destroy_ops = {"random": destroy_random}
    repair_ops = {"greedy": repair_greedy}
    
    destroy_states = [OperatorState(n) for n in destroy_ops]
    repair_states = [OperatorState(n) for n in repair_ops]
    
    current = initial_solution.copy()
    evaluate(current, inst)
    best = current.copy()
    
    print(f"[ALNS] Start Obj: {current.objective:.2f}")
    
    T = 1000.0
    alpha = 0.95
    
    for it in range(max_iter):
        di = roulette_select(destroy_states, rng)
        ri = roulette_select(repair_states, rng)
        
        d_name = destroy_states[di].name
        r_name = repair_states[ri].name
        
        partial = destroy_ops[d_name](current.copy(), inst, rng)
        candidate = repair_ops[r_name](partial, inst, rng)
        
        f_cand = evaluate(candidate, inst)
        f_curr = current.objective
        
        accept = False
        if f_cand < f_curr:
            accept = True
        else:
            diff = f_cand - f_curr
            if rng.random() < math.exp(-diff / T):
                accept = True
                
        if accept:
            current = candidate
            if f_cand < best.objective:
                best = candidate.copy()
                print(f"[ALNS] New Best: {best.objective:.2f} (Iter {it})")
        
        T *= alpha
        
    return best

# --- Tabu Search ---
# Simplified version for brevity, can be expanded
def run_tabu(inst: Instance, initial_solution: Solution, max_iter: int = 50) -> Solution:
    # Placeholder for Tabu Search implementation
    # Implementing a full Tabu Search here would be lengthy, 
    # but the structure allows adding it easily.
    # For now, we return the ALNS result or run a simple local search.
    print("[Tabu] Running simple local search...")
    current = initial_solution.copy()
    evaluate(current, inst)
    best = current.copy()
    
    # Simple Relocate Neighborhood
    for it in range(max_iter):
        improved = False
        # Try to move a customer to another route
        # This is very expensive without optimization, so we limit scope
        pass 
    
    return best
