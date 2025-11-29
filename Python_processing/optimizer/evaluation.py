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
        def get_dist_and_time(inst: Instance, i: str, j: str) -> tuple[float, float]:
            d = inst.distance.get(i, {}).get(j, None)
            t = inst.travel_time.get(i, {}).get(j, None)
            if d is not None and t is not None:
                return d, t

            # thử chiều ngược lại nếu dữ liệu là dạng một chiều
            d2 = inst.distance.get(j, {}).get(i, None)
            t2 = inst.travel_time.get(j, {}).get(i, None)
            if d2 is not None and t2 is not None:
                return d2, t2

            # nếu vẫn không có, fallback geo_distance nếu có toạ độ
            if i in inst.coords and j in inst.coords:
                lat1, lon1 = inst.coords[i]
                lat2, lon2 = inst.coords[j]
                d_geo = geo_distance(lat1, lon1, lat2, lon2)
                # giả sử tốc độ trung bình 20 km/h => 3 phút/km
                t_geo = d_geo * 3.0
                return d_geo, t_geo

            # cuối cùng, nếu hoàn toàn không có info, cho 0.0
            return 0.0, 0.0


                # Duyệt từng cung (i -> j) trên route
        for i, j in zip(stops[:-1], stops[1:]):
            allow_ij = allowed_for_vid.get(i, {}).get(j, 1)
            if allow_ij == 0:
                total_road_pen += inst.BIG_ROAD
                road_violations.append((vid, i, j))

            d_ij, t_ij = get_dist_and_time(inst, i, j)
            dist_k += d_ij
            t += t_ij  # thời điểm ARRIVAL tại node j (trước khi chờ / phục vụ)

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

                # === TIME WINDOW VỚI WAITING ĐÚNG CHUẨN VRPTW ===
                arrival = t  # thời điểm xe đến node j (sau travel)
                e_j = inst.tw_start[j]
                l_j = inst.tw_end[j]

                # Nếu đến sớm -> phải chờ
                early = max(e_j - arrival, 0.0)
                late  = max(arrival - l_j, 0.0)

                if early > 0 or late > 0:
                    tw_violations.append((j, arrival, e_j, l_j, early, late))

                total_tw_pen += inst.lambda_E[j] * early + inst.lambda_L[j] * late

                # Thời điểm bắt đầu phục vụ = max(arrival, TW start)
                start_service = max(arrival, e_j)
                t = start_service + inst.service_time[j]  # update thời gian sau khi phục vụ

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
