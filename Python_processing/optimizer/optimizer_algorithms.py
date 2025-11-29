# optimizer_algorithms.py
from __future__ import annotations
from dataclasses import dataclass
from collections import defaultdict
from typing import Dict, Set, Any, List, Tuple, Callable
import math
import random

from data_model import Instance, Route, Solution
from utils import geo_distance
from evaluation import evaluate  # nếu bạn đã tách evaluate riêng
# Nếu muốn gộp luôn evaluate vào đây, xóa dòng import trên và dùng bản evaluate dưới.

# ============================================================
# 1. (TUỲ CHỌN) HÀM EVALUATE – nếu muốn gộp hẳn vào file này
#    Nếu bạn đã có evaluation.py riêng và thích giữ, có thể bỏ hẳn phần này.
# ============================================================

# ----- BỎ COMMENT nếu bạn muốn EVALUATE nằm trong file này -----
#
# def evaluate(
#     sol: Solution,
#     inst: Instance,
#     debug: bool = False,
#     max_print_violations: int = 30,
# ) -> float:
#     """
#     Hàm mục tiêu mở rộng cho bài toán multi-depot last-mile.
#     Cập nhật sol.objective và sol.meta.
#     """
#
#     total_fixed = 0.0
#     total_dist_cost = 0.0
#     total_unserved_pen = 0.0
#     total_tw_pen = 0.0
#     total_overtime_pen = 0.0
#     total_cap_pen = 0.0
#     total_road_pen = 0.0
#     total_dist_over_pen = 0.0
#     total_depot_cap_pen = 0.0
#     total_workload_pen = 0.0
#
#     visited: Set[str] = set()
#     W: Dict[str, float] = {}
#     depot_load: Dict[str, float] = defaultdict(float)
#
#     cap_violations = []
#     tw_violations = []
#     road_violations = []
#     overtime_violations = []
#     dist_over_violations = []
#     depot_violations = []
#
#     for vid, route in sol.routes.items():
#         stops = route.stops
#         if len(stops) <= 1:
#             W[vid] = 0.0
#             continue
#
#         total_fixed += inst.fixed_cost[vid]
#
#         load_w = 0.0
#         load_v = 0.0
#         t = 0.0
#         dist_k = 0.0
#
#         depot_id = inst.depots[vid]
#         allowed_for_vid = inst.road_allowed.get(vid, {})
#
#         def get_dist_and_time(inst_: Instance, i: str, j: str) -> tuple[float, float]:
#             d = inst_.distance.get(i, {}).get(j, None)
#             tt = inst_.travel_time.get(i, {}).get(j, None)
#             if d is not None and tt is not None:
#                 return d, tt
#
#             d2 = inst_.distance.get(j, {}).get(i, None)
#             t2 = inst_.travel_time.get(j, {}).get(i, None)
#             if d2 is not None and t2 is not None:
#                 return d2, t2
#
#             if i in inst_.coords and j in inst_.coords:
#                 lat1, lon1 = inst_.coords[i]
#                 lat2, lon2 = inst_.coords[j]
#                 d_geo = geo_distance(lat1, lon1, lat2, lon2)
#                 t_geo = d_geo * 3.0
#                 return d_geo, t_geo
#
#             return 0.0, 0.0
#
#         for i, j in zip(stops[:-1], stops[1:]):
#             allow_ij = allowed_for_vid.get(i, {}).get(j, 1)
#             if allow_ij == 0:
#                 total_road_pen += inst.BIG_ROAD
#                 road_violations.append((vid, i, j))
#
#             d_ij, t_ij = get_dist_and_time(inst, i, j)
#             dist_k += d_ij
#             t += t_ij
#
#             if j in inst.customers:
#                 load_w += inst.demand_w[j]
#                 load_v += inst.demand_v[j]
#
#                 if load_w > inst.vehicle_cap_w[vid] or load_v > inst.vehicle_cap_v[vid]:
#                     over_w = max(load_w - inst.vehicle_cap_w[vid], 0.0)
#                     over_v = max(load_v - inst.vehicle_cap_v[vid], 0.0)
#                     if over_w > 0 or over_v > 0:
#                         total_cap_pen += (
#                             inst.BIG_CAP
#                             * (over_w / max(inst.vehicle_cap_w[vid], 1.0)
#                                + over_v / max(inst.vehicle_cap_v[vid], 1.0))
#                         )
#                         cap_violations.append(
#                             (vid, j, load_w, inst.vehicle_cap_w[vid],
#                              load_v, inst.vehicle_cap_v[vid])
#                         )
#
#                 a_j = t
#                 E_j = max(inst.tw_start[j] - a_j, 0.0)
#                 L_j = max(a_j - inst.tw_end[j], 0.0)
#                 if E_j > 0 or L_j > 0:
#                     tw_violations.append((j, a_j, inst.tw_start[j], inst.tw_end[j], E_j, L_j))
#                 total_tw_pen += inst.lambda_E[j] * E_j + inst.lambda_L[j] * L_j
#
#                 t += inst.service_time[j]
#
#                 visited.add(j)
#                 depot_load[depot_id] += inst.demand_w[j]
#
#         W[vid] = dist_k
#         total_dist_cost += inst.var_cost[vid] * dist_k
#
#         overtime = max(t - inst.shift_max[vid], 0.0)
#         if overtime > 0:
#             total_overtime_pen += inst.lambda_H[vid] * overtime
#             overtime_violations.append((vid, t, inst.shift_max[vid]))
#
#         if dist_k > inst.max_distance[vid]:
#             extra = dist_k - inst.max_distance[vid]
#             total_dist_over_pen += inst.lambda_dist_overtime * extra
#             dist_over_violations.append((vid, dist_k, inst.max_distance[vid]))
#
#     for cid in inst.customers:
#         if cid not in visited:
#             total_unserved_pen += inst.penalty_unserved[cid]
#
#     for d_id, load in depot_load.items():
#         cap = inst.depot_capacity.get(d_id, float("inf"))
#         if load > cap:
#             over = load - cap
#             total_depot_cap_pen += inst.lambda_depot_capacity * over
#             depot_violations.append((d_id, load, cap))
#
#     if W:
#         avgW = sum(W.values()) / len(W)
#         for vid in W:
#             total_workload_pen += inst.lambda_W * (W[vid] - avgW) ** 2
#     else:
#         avgW = 0.0
#
#     F = (
#         total_fixed
#         + total_dist_cost
#         + total_unserved_pen
#         + total_tw_pen
#         + total_overtime_pen
#         + total_cap_pen
#         + total_road_pen
#         + total_dist_over_pen
#         + total_depot_cap_pen
#         + total_workload_pen
#     )
#
#     sol.objective = F
#     sol.meta = {
#         "visited": visited,
#         "W": W,
#         "avgW": avgW,
#         "depot_load": depot_load,
#         "components": {
#             "fixed": total_fixed,
#             "distance_cost": total_dist_cost,
#             "unserved_pen": total_unserved_pen,
#             "tw_pen": total_tw_pen,
#             "overtime_pen": total_overtime_pen,
#             "capacity_pen": total_cap_pen,
#             "road_pen": total_road_pen,
#             "dist_over_pen": total_dist_over_pen,
#             "depot_cap_pen": total_depot_cap_pen,
#             "workload_pen": total_workload_pen,
#         },
#         "violations": {
#             "capacity": cap_violations,
#             "time_window": tw_violations,
#             "road": road_violations,
#             "overtime": overtime_violations,
#             "distance_over": dist_over_violations,
#             "depot_capacity": depot_violations,
#         },
#     }
#
#     if debug:
#         comps = sol.meta["components"]
#         viols = sol.meta["violations"]
#
#         print("\n===== DEBUG EVALUATE =====")
#         print(f"Objective F = {F:.2f}")
#         print("---- Components ----")
#         for k, v in comps.items():
#             print(f"  {k:15s}: {v:.2f}")
#         print("---- Violations summary ----")
#         print(f"  #capacity       = {len(viols['capacity'])}")
#         print(f"  #time_window    = {len(viols['time_window'])}")
#         print(f"  #road           = {len(viols['road'])}")
#         print(f"  #overtime       = {len(viols['overtime'])}")
#         print(f"  #dist_over      = {len(viols['distance_over'])}")
#         print(f"  #depot_capacity = {len(viols['depot_capacity'])}")
#         print("===== END DEBUG EVALUATE =====\n")
#
#     return F
#
# ----- HẾT PHẦN EVALUATE TUỲ CHỌN -----


# ============================================================
# 2. ALNS: DESTROY / REPAIR + MAIN LOOP
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


def _fix_route_roundtrip(route: Route):
    """Đảm bảo route luôn dạng [depot, ..., depot]."""
    if not route.stops:
        return
    depot = route.stops[0]
    if route.stops[-1] != depot:
        route.stops.append(depot)
    if len(route.stops) == 1:
        route.stops.append(depot)


# ---------- DESTROY OPERATORS ----------

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
    Ước lượng cost chèn cid vào route.stops tại vị trí pos (chỉ theo distance).
    """
    stops = route.stops
    if not stops:
        return float("inf")

    i = stops[pos - 1]
    j = stops[pos] if pos < len(stops) else None
    dist = inst.distance

    def get_dist(a: str, b: str) -> float:
        d = dist.get(a, {}).get(b, None)
        if d is None:
            d = dist.get(b, {}).get(a, None)
        if d is None:
            if a in inst.coords and b in inst.coords:
                lat1, lon1 = inst.coords[a]
                lat2, lon2 = inst.coords[b]
                d = geo_distance(lat1, lon1, lat2, lon2)
            else:
                d = 0.0
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
    Greedy insertion không chặn capacity cứng: vi phạm sẽ bị phạt trong evaluate().
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
            if len(route.stops) == 0:
                continue
            if len(route.stops) == 1:
                depot = route.stops[0]
                route.stops.append(depot)

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

    print(f"[repair_greedy] inserted = {inserted}")
    return new_sol


def repair_regret(sol: Solution, inst: Instance, rng: random.Random, k_regret: int = 2) -> Solution:
    new_sol = sol.copy()
    evaluate(new_sol, inst)
    served = new_sol.meta.get("visited", set())
    unserved = list(inst.customers - served)
    rng.shuffle(unserved)
    print("[repair_regret] start, #unserved =", len(unserved))

    MAX_INSERT = 200
    unserved = unserved[:MAX_INSERT]

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

    while unserved:
        best_cid = None
        best_delta_for_cid = None
        best_regret = -1.0

        for cid in list(unserved):
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
        route_load_w[vid] += inst.demand_w[best_cid]
        route_load_v[vid] += inst.demand_v[best_cid]
        unserved.remove(best_cid)

    return new_sol


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
# 3. TABU SEARCH
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

    customer_positions = []
    for vid, route in sol.routes.items():
        for pos, node in enumerate(route.stops):
            if node in inst.customers:
                customer_positions.append((vid, pos, node))

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
    max_iter: int = 50,
    max_neighbors: int = 20,
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
        best_val = float("inf")

        for mv in neighbors:
            is_tabu = mv.attr in tabu and tabu[mv.attr] > 0
            cand = apply_move(current, mv, inst)
            F_new = evaluate(cand, inst)

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
# 4. HELPER: INITIAL SOLUTION + SUMMARY + RUN ALNS/TABU
# ============================================================

def build_initial_solution(inst: Instance) -> Solution:
    routes = {}
    for vid in inst.vehicles:
        d = inst.depots[vid]
        routes[vid] = Route(vehicle_id=vid, stops=[d, d])
    return Solution(routes=routes, all_customers=inst.customers)


def print_solution_summary(sol: Solution, inst: Instance, title: str = ""):
    evaluate(sol, inst)
    comps = sol.meta["components"]
    visited = sol.meta["visited"]

    if title:
        print("\n====", title, "====")
    print("Objective:", round(sol.objective, 2))
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

    print("Một vài route mẫu:")
    for i, (vid, r) in enumerate(sol.routes.items()):
        if i >= 3:
            break
        print(f"  Route {vid}: length {len(r.stops)}")
        print("    ", r.stops[:12], "...")


def example_run_alns(inst: Instance) -> Solution:
    init_empty = build_initial_solution(inst)
    evaluate(init_empty, inst)
    print("Initial (empty) obj:", init_empty.objective)

    print("\n[DEBUG] Build initial solution with repair_greedy ...")
    rng_init = random.Random(0)
    init_sol = repair_greedy(init_empty.copy(), inst, rng_init)
    evaluate(init_sol, inst)
    comps_init = init_sol.meta["components"]
    print("[DEBUG] after greedy init: distance_cost =", comps_init["distance_cost"],
          ", unserved_pen =", comps_init["unserved_pen"])
    print("[DEBUG] #visited =", len(init_sol.meta["visited"]))

    destroy_ops = {
        "random": lambda s, i, r: destroy_random(s, i, r, remove_ratio=0.05),
        "cluster": destroy_cluster,
        "shaw": destroy_shaw_related,
    }
    repair_ops = {
        "greedy": repair_greedy,
        # "regret": repair_regret,
    }

    best = alns(
        inst=inst,
        initial_solution=init_sol,
        destroy_ops=destroy_ops,
        repair_ops=repair_ops,
        max_iter=200,
        segment_length=30,
        reaction_factor=0.2,
        start_temperature=1000,
        end_temperature=1,
        rng_seed=1,
    )
    print("[ALNS] best obj =", best.objective)
    print_solution_summary(best, inst, title="ALNS solution (final)")
    return best


def example_run_tabu(inst: Instance, sol_alns: Solution) -> Solution:
    init_sol = sol_alns.copy()
    evaluate(init_sol, inst)
    print("Initial obj TABU (from ALNS):", init_sol.objective)

    best = tabu_search(
        inst=inst,
        initial_solution=init_sol,
        max_iter=100,
        max_neighbors=80,
        tabu_tenure=20,
        rng_seed=2,
    )
    print("[TABU] best obj =", best.objective)
    print_solution_summary(best, inst, title="TABU solution (final)")
    return best
