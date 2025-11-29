# instance_builder.py
from __future__ import annotations
from collections import defaultdict
from typing import Dict, Set, Tuple
import pandas as pd

from data_model import Instance
from utils import time_str_to_min, geo_distance


def build_instance_for_depot_prefix(
    depot_prefix: str,
    customers_df: pd.DataFrame,
    depots_df: pd.DataFrame,
    vehicles_df: pd.DataFrame,
    roads_df: pd.DataFrame,
) -> Instance:
    """
    Xây dựng một Instance cho một depot_prefix (ví dụ 'D001').
    Giữ nguyên logic từ code gốc, chỉ tách ra thành module riêng.
    """

    # VEHICLES
    veh_sub = vehicles_df[vehicles_df["Start_Depot_ID"].str.startswith(depot_prefix)].copy()
    vehicle_ids = veh_sub["Vehicle_ID"].tolist()
    if not vehicle_ids:
        raise ValueError(f"Không có xe cho prefix {depot_prefix}")

    depots_map = {row["Vehicle_ID"]: row["Start_Depot_ID"] for _, row in veh_sub.iterrows()}
    max_distance = {row["Vehicle_ID"]: float(row["Max_Distance"]) for _, row in veh_sub.iterrows()}

    # ROADS
    roads_sub = roads_df[roads_df["Origin_Node_ID"].str.startswith(depot_prefix)].copy()
    if roads_sub.empty:
        raise ValueError(f"Không có roads cho prefix {depot_prefix}")

    dest_nodes: Set[str] = set(roads_sub["Destination_Node_ID"].unique())
    all_customer_ids: Set[str] = set(customers_df["Customer_ID"].unique())

    intersection = dest_nodes & all_customer_ids
    if intersection:
        customers_in_instance = intersection
    else:
        print("[WARN] roads không chứa Customer_ID nào; dùng toàn bộ customers từ file customers.")
        customers_in_instance = all_customer_ids

    cust_sub = customers_df[customers_df["Customer_ID"].isin(customers_in_instance)].copy()

    # CUSTOMER PARAMS
    demand_w: Dict[str, float] = {}
    demand_v: Dict[str, float] = {}
    service_time: Dict[str, float] = {}
    tw_start: Dict[str, float] = {}
    tw_end: Dict[str, float] = {}
    priority_map: Dict[str, int] = {}
    delivery_type: Dict[str, str] = {}
    coords: Dict[str, Tuple[float, float]] = {}

    for _, r in cust_sub.iterrows():
        cid = r["Customer_ID"]
        demand_w[cid] = float(r["Order_Weight"])
        demand_v[cid] = float(r["Order_Volume"])
        service_time[cid] = float(r["Service_Time"])
        tw_start[cid] = float(time_str_to_min(r["Time_Window_Start"]))
        tw_end[cid] = float(time_str_to_min(r["Time_Window_End"]))
        priority_map[cid] = int(r["Priority_Level"])
        delivery_type[cid] = str(r["Delivery_Type"])
        coords[cid] = (float(r["Latitude"]), float(r["Longitude"]))

    # DEPOTS
    depots_sub = depots_df[depots_df["Depot_ID"].str.startswith(depot_prefix)].copy()
    depot_capacity = {r["Depot_ID"]: float(r["Capacity_Storage"]) for _, r in depots_sub.iterrows()}

    depot_coords: Dict[str, Tuple[float, float]] = {}
    for _, r in depots_sub.iterrows():
        d_id = r["Depot_ID"]
        lat = float(r["Latitude"])
        lon = float(r["Longitude"])
        depot_coords[d_id] = (lat, lon)
        coords[d_id] = (lat, lon)

    # VEHICLE PARAMS
    vehicle_cap_w = {r["Vehicle_ID"]: float(r["Capacity_Weight"]) for _, r in veh_sub.iterrows()}
    vehicle_cap_v = {r["Vehicle_ID"]: float(r["Capacity_Volume"]) for _, r in veh_sub.iterrows()}
    fixed_cost = {r["Vehicle_ID"]: float(r["Fixed_Cost"]) for _, r in veh_sub.iterrows()}
    var_cost = {r["Vehicle_ID"]: float(r["Variable_Cost"]) for _, r in veh_sub.iterrows()}
    shift_max = {r["Vehicle_ID"]: float(r["Max_Working_Hours"]) * 60 for _, r in veh_sub.iterrows()}
    vehicle_type = {r["Vehicle_ID"]: str(r["Vehicle_Type"]) for _, r in veh_sub.iterrows()}

    # DISTANCE / TIME MATRIX
    distance = defaultdict(dict)
    travel_time = defaultdict(dict)
    for _, r in roads_sub.iterrows():
        i = r["Origin_Node_ID"]
        j = r["Destination_Node_ID"]
        distance[i][j] = float(r["Distance_km"])
        travel_time[i][j] = float(r["Travel_Time_min"])

    # ROAD RESTRICTIONS
    HEAVY = {"Truck", "Van", "Heavy Truck"}
    roads_sub["Road_Restrictions"] = roads_sub["Road_Restrictions"].fillna("None").astype(str)

    road_allowed = {vid: defaultdict(dict) for vid in vehicle_ids}
    for _, r in roads_sub.iterrows():
        i = r["Origin_Node_ID"]
        j = r["Destination_Node_ID"]
        rest = r["Road_Restrictions"]

        for vid in vehicle_ids:
            allow = 1
            if rest == "No Heavy Trucks" and vehicle_type[vid] in HEAVY:
                allow = 0
            road_allowed[vid][i][j] = allow

    # CLUSTERING: gán khách về depot gần nhất
    customer_cluster: Dict[str, str] = {}
    for cid in customers_in_instance:
        clat, clon = coords[cid]
        best_d = None
        best_dis = float("inf")
        for did, (dlat, dlon) in depot_coords.items():
            d = geo_distance(clat, clon, dlat, dlon)
            if d < best_dis:
                best_dis = d
                best_d = did
        customer_cluster[cid] = best_d

    # PENALTIES
    penalty_unserved: Dict[str, float] = {}
    lambda_E: Dict[str, float] = {}
    lambda_L: Dict[str, float] = {}
    for cid in customers_in_instance:
        phi = priority_map[cid]
        penalty_unserved[cid] = 2e5 * phi * max(demand_w[cid], 1)
        lambda_E[cid] = 0.1 * phi
        lambda_L[cid] = 2.0 * phi

    lambda_H = {vid: 0.1 * fixed_cost[vid] for vid in vehicle_ids}
    lambda_W = 1e-4
    lambda_dist_overtime = 5.0
    lambda_depot_capacity = 1.0

    BIG_CAP = 1e4
    BIG_ROAD = 1e4

    return Instance(
        customers=customers_in_instance,
        vehicles=vehicle_ids,
        depots=depots_map,
        depot_capacity=depot_capacity,
        distance=dict(distance),
        travel_time=dict(travel_time),
        road_allowed=road_allowed,
        demand_w=demand_w,
        demand_v=demand_v,
        service_time=service_time,
        tw_start=tw_start,
        tw_end=tw_end,
        priority=priority_map,
        delivery_type=delivery_type,
        coords=coords,
        customer_cluster=customer_cluster,
        vehicle_cap_w=vehicle_cap_w,
        vehicle_cap_v=vehicle_cap_v,
        shift_max=shift_max,
        max_distance=max_distance,
        fixed_cost=fixed_cost,
        var_cost=var_cost,
        penalty_unserved=penalty_unserved,
        lambda_E=lambda_E,
        lambda_L=lambda_L,
        lambda_H=lambda_H,
        lambda_W=lambda_W,
        lambda_dist_overtime=lambda_dist_overtime,
        lambda_depot_capacity=lambda_depot_capacity,
        BIG_CAP=BIG_CAP,
        BIG_ROAD=BIG_ROAD,
    )
