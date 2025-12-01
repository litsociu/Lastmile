from __future__ import annotations
import pandas as pd
import os
from .config import Solution, Instance

def save_solution(sol: Solution, inst: Instance, output_dir: str = "OUTPUT", prefix: str = "result"):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    # 1. Summary Routes (Depot -> Centroid)
    route_rows = []
    cluster_rows = []
    
    for vid, route_list in sol.routes.items():
        for trip_idx, route in enumerate(route_list):
            if len(route.stops) <= 2: continue
            
            # Get Cluster Info from Meta
            meta = route.meta
            if not meta:
                # Fallback if meta missing (should not happen with new init)
                continue
                
            cluster_id = meta.get("cluster_id")
            centroid_id = meta.get("centroid_customer_id")
            cluster_customers = meta.get("cluster_customers", [])
            
            # Calculate Load
            load_w = sum(inst.demand_w[c] for c in cluster_customers)
            load_v = sum(inst.demand_v[c] for c in cluster_customers)
            
            # Get Depot
            depot_id = route.stops[0]
            
            # Distance (Depot -> Centroid -> Depot)
            # Note: This is approximate if not using exact calculated metrics
            # But consistent with our logic
            
            route_rows.append({
                "Vehicle_ID": vid,
                "Trip_ID": trip_idx + 1,
                "Depot_ID": depot_id,
                "Cluster_ID": cluster_id,
                "Centroid_Customer_ID": centroid_id,
                "Num_Customers": len(cluster_customers),
                "Load_Weight": load_w,
                "Load_Volume": load_v,
                "Route_Sequence": f"{depot_id} -> {centroid_id} (Cluster {cluster_id}) -> {depot_id}"
            })
            
            # 2. Cluster Details
            for cid in cluster_customers:
                cluster_rows.append({
                    "Cluster_ID": cluster_id,
                    "Vehicle_ID": vid,
                    "Trip_ID": trip_idx + 1,
                    "Depot_ID": depot_id,
                    "Centroid_ID": centroid_id,
                    "Customer_ID": cid,
                    "Weight": inst.demand_w[cid],
                    "Volume": inst.demand_v[cid]
                })
        
    # Save Routes
    df_routes = pd.DataFrame(route_rows)
    out_path_routes = os.path.join(output_dir, f"{prefix}_routes.xlsx")
    df_routes.to_excel(out_path_routes, index=False)
    print(f"[OUTPUT] Saved routes summary to {out_path_routes}")
    
    # Save Clusters
    df_clusters = pd.DataFrame(cluster_rows)
    out_path_clusters = os.path.join(output_dir, f"{prefix}_clusters.xlsx")
    df_clusters.to_excel(out_path_clusters, index=False)
    print(f"[OUTPUT] Saved cluster details to {out_path_clusters}")

def print_solution_stats(sol: Solution):
    print("\n=== SOLUTION STATS ===")
    print(f"Objective: {sol.objective:.2f}")
    
    comps = sol.meta.get("components", {})
    if comps:
        print("Components:")
        for k, v in comps.items():
            print(f"  {k}: {v:.2f}")
            
    viols = sol.meta.get("violations", {})
    if viols:
        print("Violations:")
        for k, v in viols.items():
            if v:
                print(f"  {k}: {len(v)} violations")
