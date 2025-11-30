from __future__ import annotations
import random
import math
import pandas as pd
from typing import List, Dict, Tuple
from sklearn.cluster import KMeans
from .config import Instance, Solution, Route
from .constraints import check_capacity, check_road_allowed, get_dist_and_time
from .utils import geo_distance

def split_cluster_by_capacity(
    cluster_nodes: List[str], 
    inst: Instance, 
    target_weight: float,
    target_volume: float
) -> List[List[str]]:
    """
    Chia một cluster lớn thành các sub-cluster nhỏ hơn dựa trên target capacity.
    Không phụ thuộc vào xe cụ thể.
    
    Args:
        cluster_nodes: Danh sách customer IDs trong cluster
        inst: Instance object
        target_weight: Target weight cho mỗi sub-cluster
        target_volume: Target volume cho mỗi sub-cluster
    
    Returns:
        List of sub-clusters
    """
    # Lấy thông tin demand
    items = []
    for cid in cluster_nodes:
        items.append({
            "id": cid,
            "w": inst.demand_w[cid],
            "v": inst.demand_v[cid]
        })
    
    # Sort giảm dần theo weight (First Fit Decreasing heuristic)
    items.sort(key=lambda x: x["w"], reverse=True)
    
    bins = []
    current_bin = []
    current_w = 0.0
    current_v = 0.0
    
    for item in items:
        w = item["w"]
        v = item["v"]
        
        # Nếu nhét vào bin hiện tại mà vượt quá -> tạo bin mới
        if current_bin and (current_w + w > target_weight or current_v + v > target_volume):
            bins.append([x["id"] for x in current_bin])
            current_bin = []
            current_w = 0.0
            current_v = 0.0
            
        current_bin.append(item)
        current_w += w
        current_v += v
        
    if current_bin:
        bins.append([x["id"] for x in current_bin])
        
    return bins

def determine_optimal_clusters(inst: Instance) -> int:
    """
    Tính số cụm tối ưu dựa trên tổng demand và capacity trung bình.
    Không phụ thuộc số xe.
    
    Returns:
        Số cụm K cho K-Means
    """
    cust_ids = list(inst.customers)
    
    # Tổng demand
    total_w = sum(inst.demand_w[c] for c in cust_ids)
    total_v = sum(inst.demand_v[c] for c in cust_ids)
    
    # Capacity trung bình của fleet
    avg_cap_w = sum(inst.vehicle_cap_w.values()) / len(inst.vehicles)
    avg_cap_v = sum(inst.vehicle_cap_v.values()) / len(inst.vehicles)
    
    # Target weight/volume cho mỗi cluster (70% capacity để an toàn)
    TARGET_FILL_RATE = 0.7
    target_w = avg_cap_w * TARGET_FILL_RATE
    target_v = avg_cap_v * TARGET_FILL_RATE
    
    # Ước lượng số cluster cần thiết
    k_from_weight = math.ceil(total_w / target_w)
    k_from_volume = math.ceil(total_v / target_v)
    
    # Lấy max để đảm bảo không quá tải
    k_estimated = max(k_from_weight, k_from_volume)
    
    # Thêm buffer 20% để tránh cluster quá đầy
    k_final = int(k_estimated * 1.2)
    
    # Giới hạn K trong khoảng hợp lý
    k_final = max(10, min(k_final, len(cust_ids) // 5))
    
    print(f"[INFO] Cluster estimation: total_weight={total_w:.0f}, total_volume={total_v:.0f}")
    print(f"[INFO] Target per cluster: weight={target_w:.0f}, volume={target_v:.0f}")
    print(f"[INFO] Optimal K determined: {k_final} clusters")
    
    return k_final, target_w, target_v

def initialize_solution(inst: Instance, rng_seed: int = 42) -> Solution:
    """
    Tạo giải pháp ban đầu với Multi-Trip support.
    
    Bước 1: K-Means clustering (không phụ thuộc số xe)
    Bước 2: Split clusters nếu quá tải
    Bước 3: Gán clusters cho xe (multi-trip: xe có thể chạy nhiều cụm)
    
    Returns:
        Solution với multi-trip routes
    """
    print(">>> [INIT] Running K-Means Clustering (vehicle-independent)...")
    rng = random.Random(rng_seed)
    
    # 1. Prepare data for K-Means
    cust_ids = list(inst.customers)
    coords = []
    for cid in cust_ids:
        coords.append(inst.coords[cid])
    
    # 2. Determine optimal K (không phụ thuộc số xe)
    n_clusters, target_w, target_v = determine_optimal_clusters(inst)
    
    # 3. Run K-Means
    kmeans = KMeans(n_clusters=n_clusters, random_state=rng_seed, n_init="auto")
    labels = kmeans.fit_predict(coords)
    
    # 4. Group customers by cluster label
    raw_clusters = {}
    for idx, label in enumerate(labels):
        if label not in raw_clusters:
            raw_clusters[label] = []
        raw_clusters[label].append(cust_ids[idx])
    
    # 5. Refine Clusters (Split by target capacity)
    final_clusters = []
    for label, nodes in raw_clusters.items():
        sub_clusters = split_cluster_by_capacity(nodes, inst, target_w, target_v)
        final_clusters.extend(sub_clusters)
    
    print(f">>> [INIT] Created {len(final_clusters)} final clusters from {len(cust_ids)} customers.")
    
    # 6. Convert clusters to cluster info với centroid
    cluster_infos = []
    for i, nodes in enumerate(final_clusters):
        w = sum(inst.demand_w[c] for c in nodes)
        v = sum(inst.demand_v[c] for c in nodes)
        
        # Tính tâm cụm
        lats = [inst.coords[c][0] for c in nodes]
        lons = [inst.coords[c][1] for c in nodes]
        center_lat = sum(lats) / len(lats)
        center_lon = sum(lons) / len(lons)
        
        # Tìm depot gần nhất
        best_depot = None
        min_dist = float("inf")
        for vid in inst.vehicles:
            depot_id = inst.depots[vid]
            d_lat, d_lon = inst.coords[depot_id]
            dist = geo_distance(center_lat, center_lon, d_lat, d_lon)
            if dist < min_dist:
                min_dist = dist
                best_depot = depot_id
        
        cluster_infos.append({
            "id": i,
            "nodes": nodes,
            "w": w,
            "v": v,
            "center": (center_lat, center_lon),
            "nearest_depot": best_depot,
            "dist_to_depot": min_dist
        })
    
    # Sort clusters theo depot gần nhất để dễ assign
    cluster_infos.sort(key=lambda x: (x["nearest_depot"], x["dist_to_depot"]))
    
    # 7. MULTI-TRIP ASSIGNMENT: Gán clusters cho xe
    print(f">>> [INIT] Assigning {len(cluster_infos)} clusters to {len(inst.vehicles)} vehicles (Multi-Trip)...")
    
    # Khởi tạo trips cho mỗi xe
    vehicle_trips = {vid: [] for vid in inst.vehicles}  # List of trips (routes) per vehicle
    vehicle_total_time = {vid: 0.0 for vid in inst.vehicles}
    
    unassigned_clusters = []
    
    for clus in cluster_infos:
        nodes = clus["nodes"]
        w_clus = clus["w"]
        v_clus = clus["v"]
        nearest_depot = clus["nearest_depot"]
        
        # Tìm xe phù hợp: xe cùng depot, còn thời gian, đủ capacity
        assigned = False
        
        # Ưu tiên xe thuộc depot gần nhất
        candidate_vehicles = []
        for vid in inst.vehicles:
            if inst.depots[vid] == nearest_depot:
                candidate_vehicles.append(vid)
        
        # Nếu không có xe ở depot đó, thử tất cả xe
        if not candidate_vehicles:
            candidate_vehicles = list(inst.vehicles)
        
        # Shuffle để phân bố đều
        rng.shuffle(candidate_vehicles)
        
        for vid in candidate_vehicles:
            # Check capacity
            if w_clus > inst.vehicle_cap_w[vid] or v_clus > inst.vehicle_cap_v[vid]:
                continue
            
            # Estimate time cho trip này: depot -> cluster center -> depot
            depot_id = inst.depots[vid]
            
            # Tìm centroid customer
            lats = [inst.coords[c][0] for c in nodes]
            lons = [inst.coords[c][1] for c in nodes]
            mean_lat = sum(lats) / len(lats)
            mean_lon = sum(lons) / len(lons)
            
            best_c = None
            min_dist_to_center = float("inf")
            for c in nodes:
                d = geo_distance(inst.coords[c][0], inst.coords[c][1], mean_lat, mean_lon)
                if d < min_dist_to_center:
                    min_dist_to_center = d
                    best_c = c
            
            # Estimate time
            _, t_out = get_dist_and_time(inst, depot_id, best_c)
            _, t_in = get_dist_and_time(inst, best_c, depot_id)
            trip_time = t_out + inst.service_time[best_c] + t_in
            
            # Check nếu xe còn đủ thời gian
            if vehicle_total_time[vid] + trip_time <= inst.shift_max[vid]:
                # Gán cluster cho xe này (tạo trip mới)
                # Route stops vẫn giữ full nodes để tính toán load trong evaluate
                trip_route = Route(vehicle_id=vid, stops=[depot_id] + nodes + [depot_id])
                
                # Store cluster info in meta for Output & Visualization
                trip_route.meta = {
                    "cluster_id": clus["id"],
                    "centroid_customer_id": best_c,
                    "cluster_customers": nodes,
                    "centroid_lat": inst.coords[best_c][0],
                    "centroid_lon": inst.coords[best_c][1]
                }
                
                vehicle_trips[vid].append(trip_route)
                vehicle_total_time[vid] += trip_time
                assigned = True
                break
        
        if not assigned:
            unassigned_clusters.append(clus)
            print(f"[WARN] Cluster {clus['id']} (w={w_clus:.1f}, v={v_clus:.1f}) không thể gán cho xe nào (capacity hoặc time)")
    
    print(f">>> [INIT] Assignment complete: {len(cluster_infos) - len(unassigned_clusters)}/{len(cluster_infos)} clusters assigned")
    print(f">>> [INIT] Multi-trip stats:")
    trips_per_vehicle = [len(trips) for trips in vehicle_trips.values() if trips]
    if trips_per_vehicle:
        print(f"    - Avg trips/vehicle: {sum(trips_per_vehicle)/len(trips_per_vehicle):.1f}")
        print(f"    - Max trips/vehicle: {max(trips_per_vehicle)}")
        print(f"    - Vehicles used: {len([v for v in vehicle_trips.values() if v])}/{len(inst.vehicles)}")
    
    # 8. Convert to Solution format
    routes = {}
    for vid in inst.vehicles:
        if vehicle_trips[vid]:
            routes[vid] = vehicle_trips[vid]
        else:
            # Route rỗng
            depot = inst.depots[vid]
            routes[vid] = [Route(vehicle_id=vid, stops=[depot, depot])]
    
    # Store thông tin multi-trip trong meta để thuật toán có thể dùng
    sol = Solution(routes=routes, all_customers=inst.customers)
    sol.meta["vehicle_trips"] = vehicle_trips  # Full multi-trip info
    sol.meta["unassigned_clusters"] = unassigned_clusters
    
    return sol
