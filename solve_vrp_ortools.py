# solve_vrp_ortools.py
from ortools.constraint_solver import pywrapcp, routing_enums_pb2
from data import load_data, build_cost_lookup
import math
import sys

def time_to_minutes(t):
    """t is datetime.time or NaT; return minutes since midnight"""
    if t is None or (hasattr(t, 'hour') and math.isnan(t.hour if hasattr(t, 'hour') else 0)):
        return None
    if t is pd.NaT:
        return None
    try:
        return t.hour * 60 + t.minute
    except Exception:
        return None

import pandas as pd

def build_problem():
    #tải lên dữ liệu sạch 
    df_customers, df_depots, df_vehicles, df_roads_full = load_data()

    # Build both time lookup (minutes) and distance lookup (kilometers)
    time_lookup, all_nodes = build_cost_lookup(df_roads_full, df_depots, df_customers, mode="time")
    distance_lookup, _ = build_cost_lookup(df_roads_full, df_depots, df_customers, mode="distance")

    # Node index mapping (same order as all_nodes)
    node_index = {node: i for i, node in enumerate(all_nodes)}

    num_vehicles = len(df_vehicles)
    num_nodes = len(all_nodes)

    # Build starts and ends (index in all_nodes) per vehicle
    starts = [node_index.get(d, 0) for d in df_vehicles["Start_Depot_ID"]]
    ends = [node_index.get(d, 0) for d in df_vehicles["End_Depot_ID"]]
    cust_dict = df_customers.set_index("Customer_ID").to_dict("index")
    depot_dict = df_depots.set_index("Depot_ID").to_dict("index")

    demands = [0] * num_nodes
    service_times = [0] * num_nodes
    time_windows = [(0, 24*60)] * num_nodes  # default whole day

    for idx, node in enumerate(all_nodes):
        if str(node).startswith("C"):
            # customer
            c = cust_dict.get(node)
            if c is None:
                # if customer not in df_customers (shouldn't happen), leave defaults
                continue
            demands[idx] = int(round(c.get("Order_Weight", 0)))  # integer kg
            service_times[idx] = int(round(c.get("Service_Time", 0)))  # minutes
            # time windows
            tws = c.get("Time_Window_Start")
            twe = c.get("Time_Window_End")
            # pandas time -> convert
            try:
                tstart = pd.to_datetime(tws, format="%H:%M", errors="coerce").time() if not pd.isna(tws) else None
            except Exception:
                tstart = tws
            try:
                tend = pd.to_datetime(twe, format="%H:%M", errors="coerce").time() if not pd.isna(twe) else None
            except Exception:
                tend = twe
            ts_min = time_to_minutes(tstart) if tstart is not None else 0
            te_min = time_to_minutes(tend) if tend is not None else 24*60
            if ts_min is None: ts_min = 0
            if te_min is None: te_min = 24*60
            time_windows[idx] = (int(ts_min), int(te_min))
        else:
            # depot
            d = depot_dict.get(node)
            if d is not None:
                # If depots have operating hours column parsed in data.py as Open_Time/Close_Time (datetime)
                if "Open_Time" in d and pd.notna(d["Open_Time"]):
                    ot = d["Open_Time"]
                    ct = d.get("Close_Time", None)
                    if pd.notna(ot):
                        start_min = int(ot.hour*60 + ot.minute)
                    else:
                        start_min = 0
                    if ct is not None and pd.notna(ct):
                        end_min = int(ct.hour*60 + ct.minute)
                    else:
                        end_min = 24*60
                    time_windows[idx] = (start_min, end_min)
                else:
                    time_windows[idx] = (0, 24*60)

    # Vehicle properties lists
    vehicle_capacities = df_vehicles["Capacity_Weight"].fillna(0).astype(int).tolist()
    vehicle_max_distance_km = df_vehicles["Max_Distance"].fillna(1e9).astype(float).tolist()
    vehicle_max_working_hours = df_vehicles["Max_Working_Hours"].fillna(24).astype(float).tolist()
    vehicle_fixed_costs = df_vehicles["Fixed_Cost"].fillna(0).astype(float).tolist()
    vehicle_variable_costs = df_vehicles["Variable_Cost"].fillna(0).astype(float).tolist()
    vehicle_ids = df_vehicles["Vehicle_ID"].tolist()

    return {
        "num_vehicles": num_vehicles,
        "num_nodes": num_nodes,
        "starts": starts,
        "ends": ends,
        "all_nodes": all_nodes,
        "time_lookup": time_lookup,
        "distance_lookup": distance_lookup,
        "demands": demands,
        "service_times": service_times,
        "time_windows": time_windows,
        "vehicle_capacities": vehicle_capacities,
        "vehicle_max_distance_km": vehicle_max_distance_km,
        "vehicle_max_working_hours": vehicle_max_working_hours,
        "vehicle_fixed_costs": vehicle_fixed_costs,
        "vehicle_variable_costs": vehicle_variable_costs,
        "vehicle_ids": vehicle_ids,
        "df_customers": df_customers,
        "df_vehicles": df_vehicles,
        "df_depots": df_depots
    }

def solve():
    data = build_problem()
    num_nodes = data["num_nodes"]
    num_vehicles = data["num_vehicles"]
    all_nodes = data["all_nodes"]

    # Manager & Model
    manager = pywrapcp.RoutingIndexManager(num_nodes, num_vehicles, data["starts"], data["ends"])
    routing = pywrapcp.RoutingModel(manager)

    # --- Callbacks ---
    # Time callback: travel_time (minutes) + service_time at origin
    def time_callback(from_index, to_index):
        from_node = manager.IndexToNode(from_index)
        to_node = manager.IndexToNode(to_index)
        travel_time = data["time_lookup"].get((from_node, to_node), 10**7)
        service_time = data["service_times"][from_node] if from_node < len(data["service_times"]) else 0
        return int(round(travel_time + service_time))

    time_callback_index = routing.RegisterTransitCallback(time_callback)
    # Set arc cost evaluator to time (objective minimize total travel+service time)
    routing.SetArcCostEvaluatorOfAllVehicles(time_callback_index)

    # Distance callback for distance dimension (in meters to use int)
    def distance_callback(from_index, to_index):
        from_node = manager.IndexToNode(from_index)
        to_node = manager.IndexToNode(to_index)
        # distance_lookup stored in km -> convert to meters and int
        dist_km = data["distance_lookup"].get((from_node, to_node), 1e6)
        return int(round(dist_km * 1000.0))

    distance_callback_index = routing.RegisterTransitCallback(distance_callback)

    # --- Ràng buộc tải trọng (Capacity) ---
    def demand_callback(from_index):
        node = manager.IndexToNode(from_index)
        return int(data["demands"][node])
    demand_callback_index = routing.RegisterUnaryTransitCallback(demand_callback)

    routing.AddDimensionWithVehicleCapacity(
        demand_callback_index,
        0,  # không có độ trễ
        data["vehicle_capacities"],
        True,
        "Weight"
    )

    # --- Ràng buộc quãng đường (Max_Distance) ---
    routing.AddDimension(
        distance_callback_index,
        0,
        10**12,       # giới hạn toàn cục rất lớn
        True,
        "Distance"
    )
    distance_dimension = routing.GetDimensionOrDie("Distance")

    # Giới hạn quãng đường tối đa cho từng xe (km → m)
    for vid in range(num_vehicles):
        max_km = data["vehicle_max_distance_km"][vid]
        max_m = int(round(max_km * 1000.0))
        distance_dimension.CumulVar(routing.End(vid)).SetMax(max_m)

    # --- Ràng buộc thời gian (Time Windows) ---
    horizon = 24 * 60  # thời gian tối đa 24h
    routing.AddDimension(
        time_callback_index,
        horizon,  # cho phép chờ tối đa
        horizon,
        False,
        "Time"
    )
    time_dimension = routing.GetDimensionOrDie("Time")

    # Thiết lập khoảng thời gian phục vụ (Time Window) cho từng điểm
    for idx in range(routing.Size()):
        node = manager.IndexToNode(idx)
        tw = data["time_windows"][node]
        time_dimension.CumulVar(idx).SetRange(int(tw[0]), int(tw[1]))

    # Thiết lập thời gian làm việc tối đa cho từng xe
    for v in range(num_vehicles):
        start_index = routing.Start(v)
        end_index = routing.End(v)
        max_hours = data["vehicle_max_working_hours"][v]
        max_minutes = int(max_hours * 60)
        time_dimension.CumulVar(end_index).SetMax(
            time_dimension.CumulVar(end_index).Max() + max_minutes
        )

    # --- Cho phép bỏ qua khách hàng (với chi phí phạt cao) ---
    penalty = 1_000_000
    for node_idx, node in enumerate(all_nodes):
        if str(node).startswith("C"):
            idx = manager.NodeToIndex(node_idx)
            routing.AddDisjunction([idx], penalty)

    # --- Thông số tìm kiếm ---
    search_parameters = pywrapcp.DefaultRoutingSearchParameters()
    search_parameters.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
    search_parameters.local_search_metaheuristic = routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
    search_parameters.time_limit.seconds = 180
    search_parameters.log_search = False

    # --- Giải bài toán ---
    print("🔎 Đang giải bài toán VRPTW ...")
    solution = routing.SolveWithParameters(search_parameters)

    if solution is None:
        print("❌ Không tìm thấy nghiệm khả thi.")
        return

    # --- Trích xuất kết quả ---
    routes = []
    total_distance_m = 0
    total_time_min = 0
    used_vehicles = set()
    unserved = []

    for v in range(num_vehicles):
        index = routing.Start(v)
        if routing.IsEnd(solution.Value(routing.NextVar(index))):
            continue  # xe không được sử dụng

        route_nodes = []
        route_distance = 0
        route_time = 0
        used_vehicles.add(v)

        while not routing.IsEnd(index):
            node_idx = manager.IndexToNode(index)
            route_nodes.append(all_nodes[node_idx])
            next_index = solution.Value(routing.NextVar(index))
            dist_m = distance_callback(index, next_index)
            t_min = time_callback(index, next_index)
            route_distance += dist_m
            route_time += t_min
            index = next_index

        node_idx = manager.IndexToNode(index)
        route_nodes.append(all_nodes[node_idx])

        total_distance_m += route_distance
        total_time_min += route_time
        routes.append({
            "vehicle_index": v,
            "vehicle_id": data["vehicle_ids"][v],
            "route": route_nodes,
            "distance_m": route_distance,
            "time_min": route_time
        })

    # --- Xác định khách hàng chưa phục vụ ---
    served = {n for r in routes for n in r["route"] if str(n).startswith("C")}
    unserved = [node for node in all_nodes if str(node).startswith("C") and node not in served]

    # --- Tính chi phí ---
    total_cost = 0.0
    for r in routes:
        vidx = r["vehicle_index"]
        fixed = data["vehicle_fixed_costs"][vidx]
        var = data["vehicle_variable_costs"][vidx]
        dist_km = r["distance_m"] / 1000.0
        total_cost += fixed + var * dist_km

    # --- Tổng kết ---
    print("\n--- TỔNG KẾT KẾT QUẢ ---")
    print(f"Số xe được sử dụng: {len(used_vehicles)} / {num_vehicles}")
    print(f"Tổng quãng đường (km): {total_distance_m/1000.0:.2f}")
    print(f"Tổng thời gian (phút): {total_time_min:.1f}")
    print(f"Tổng chi phí ước tính: {total_cost:.2f}")
    print(f"Số khách hàng chưa phục vụ: {len(unserved)}")

    # Hiển thị lộ trình từng xe
    for r in routes:
        print(f"🚚 {r['vehicle_id']}: {' -> '.join(r['route'])}  | {r['distance_m']/1000.0:.2f} km, {r['time_min']:.1f} phút")

    return {
        "routes": routes,
        "total_distance_km": total_distance_m/1000.0,
        "total_time_min": total_time_min,
        "total_cost": total_cost,
        "unserved_customers": unserved
    }
if __name__ == "__main__": solve()