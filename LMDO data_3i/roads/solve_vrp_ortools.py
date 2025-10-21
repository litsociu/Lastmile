from ortools.constraint_solver import pywrapcp, routing_enums_pb2
from data import load_data, build_cost_lookup

# --- Load dữ liệu ---
df_customers, df_depots, df_vehicles, df_roads_full = load_data()

# --- Tạo lookup ---
cost_lookup, all_nodes = build_cost_lookup(df_roads_full, df_depots, df_customers, mode="time")

# --- Tạo mapping Node_ID → index ---
node_index = {node: i for i, node in enumerate(all_nodes)}

# --- Xác định số xe và depot ---
num_vehicles = len(df_vehicles)

# Chuyển Start_Depot_ID, End_Depot_ID sang index tương ứng trong all_nodes
starts = [node_index.get(d, 0) for d in df_vehicles["Start_Depot_ID"]]
ends = [node_index.get(d, 0) for d in df_vehicles["End_Depot_ID"]]

print(f"🚚 Tổng số xe: {num_vehicles}")
print(f"🏭 Tổng số depot: {len(df_depots)}")
print(f"🔹 Ví dụ 5 depot start: {starts[:5]}")
print(f"🔹 Ví dụ 5 depot end:   {ends[:5]}")

# --- Tạo RoutingIndexManager ---
manager = pywrapcp.RoutingIndexManager(len(all_nodes), num_vehicles, starts, ends)

routing = pywrapcp.RoutingModel(manager)

# --- Callback tính chi phí ---
def distance_callback(from_index, to_index):
    from_node = manager.IndexToNode(from_index)
    to_node = manager.IndexToNode(to_index)
    return int(cost_lookup.get((from_node, to_node), 999999))  # nếu không có cung thì cost lớn

transit_callback_index = routing.RegisterTransitCallback(distance_callback)
routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)

# --- Ràng buộc khoảng cách ---
routing.AddDimension(
    transit_callback_index,
    0,        # không cho phép chờ
    999999,    # giới hạn lớn
    True,     # start từ 0
    "Distance"
)

# --- Cấu hình tìm kiếm ---
search_parameters = pywrapcp.DefaultRoutingSearchParameters()
search_parameters.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
search_parameters.time_limit.seconds = 60  

# --- Giải ---
solution = routing.SolveWithParameters(search_parameters)
print(f"🔍 Số cung từ depot tới customer: {sum(1 for (i,j) in cost_lookup.keys() if all_nodes[i].startswith('D') and all_nodes[j].startswith('C'))}")

# --- Hiển thị kết quả ---
if solution:
    print("\n✅ Đã tìm thấy lời giải khả thi:")
    for v in range(num_vehicles):
        index = routing.Start(v)
        route_nodes = []
        while not routing.IsEnd(index):
            node = manager.IndexToNode(index)
            route_nodes.append(all_nodes[node])
            index = solution.Value(routing.NextVar(index))
        route_nodes.append(all_nodes[manager.IndexToNode(index)])
        print(f"🚚 Xe {df_vehicles.loc[v, 'Vehicle_ID']}: {' -> '.join(route_nodes)}")
else:
    print("❌ Không tìm thấy nghiệm khả thi.")
