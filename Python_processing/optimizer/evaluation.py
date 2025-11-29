# evaluation.py
from __future__ import annotations
from collections import defaultdict
from typing import Dict, Set, Any
import math

from data_model import Instance, Solution
from utils import geo_distance


def evaluate(
    sol: Solution,
    inst: Instance,
    debug: bool = False,
    max_print_violations: int = 30,
) -> float:
    """
    Hàm mục tiêu mở rộng cho bài toán multi-depot last-mile.
    Cập nhật sol.objective và sol.meta.
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

    # 1. Duyệt từng tuyến
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
            tt = inst_.travel_time.get(i, {}).get(j, None)
            if d is not None and tt is not None:
                return d, tt

            d2 = inst_.distance.get(j, {}).get(i, None)
            t2 = inst_.travel_time.get(j, {}).get(i, None)
            if d2 is not None and t2 is not None:
                return d2, t2

            if i in inst_.coords and j in inst_.coords:
                lat1, lon1 = inst_.coords[i]
                lat2, lon2 = inst_.coords[j]
                d_geo = geo_distance(lat1, lon1, lat2, lon2)
                t_geo = d_geo * 3.0
                return d_geo, t_geo

            return 0.0, 0.0

        for i, j in zip(stops[:-1], stops[1:]):
            allow_ij = allowed_for_vid.get(i, {}).get(j, 1)
            if allow_ij == 0:
                total_road_pen += inst.BIG_ROAD
                road_violations.append((vid, i, j))

            d_ij, t_ij = get_dist_and_time(inst, i, j)
            dist_k += d_ij
            t += t_ij

            if j in inst.customers:
                load_w += inst.demand_w[j]
                load_v += inst.demand_v[j]

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

                a_j = t
                E_j = max(inst.tw_start[j] - a_j, 0.0)
                L_j = max(a_j - inst.tw_end[j], 0.0)
                if E_j > 0 or L_j > 0:
                    tw_violations.append((j, a_j, inst.tw_start[j], inst.tw_end[j], E_j, L_j))
                total_tw_pen += inst.lambda_E[j] * E_j + inst.lambda_L[j] * L_j

                t += inst.service_time[j]

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

    # 2. Phạt khách không được phục vụ
    for cid in inst.customers:
        if cid not in visited:
            total_unserved_pen += inst.penalty_unserved[cid]

    # 3. Ràng buộc sức chứa depot
    for d_id, load in depot_load.items():
        cap = inst.depot_capacity.get(d_id, float("inf"))
        if load > cap:
            over = load - cap
            total_depot_cap_pen += inst.lambda_depot_capacity * over
            depot_violations.append((d_id, load, cap))

    # 4. Cân bằng workload
    if W:
        avgW = sum(W.values()) / len(W)
        for vid in W:
            total_workload_pen += inst.lambda_W * (W[vid] - avgW) ** 2
    else:
        avgW = 0.0

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
