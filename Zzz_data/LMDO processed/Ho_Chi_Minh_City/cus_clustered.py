import pandas as pd
from sklearn.cluster import KMeans
import numpy as np
import math
import os

# ========= CONFIG =========
BASE_DIR = "/Users/alicecin/Documents/Lastmile/Zzz_data/LMDO processed/Ho_Chi_Minh_City"

INPUT_CUSTOMERS = os.path.join(BASE_DIR, "customers.xlsx")          # file gốc
INPUT_VEHICLES = os.path.join(BASE_DIR, "vehicles.xlsx")            # để đọc capacity
OUTPUT_CUSTOMERS = os.path.join(BASE_DIR, "customers_clustered.xlsx")  # file mới

N_CLUSTERS = 200   # số cluster ban đầu (có thể tăng nếu muốn)
PICKUP_TW_START = "08:00"
PICKUP_TW_END = "21:00"
PICKUP_SERVICE_TIME = 10.0
# ==========================

print("Đọc customers.xlsx ...")
df = pd.read_excel(INPUT_CUSTOMERS)
veh = pd.read_excel(INPUT_VEHICLES)

needed_cols = [
    "Customer_ID",
    "Order_Weight",
    "Order_Volume",
    "Service_Time",
    "Time_Window_Start",
    "Time_Window_End",
    "Priority_Level",
    "Delivery_Type",
    "Latitude",
    "Longitude",
]
missing = [c for c in needed_cols if c not in df.columns]
if missing:
    raise ValueError(f"Thiếu cột trong customers.xlsx: {missing}")

max_cap = veh["Capacity_Weight"].max()
# Ngưỡng mỗi pickup sau khi chia – dùng 80% capacity để còn room cho nhiều node trên 1 xe
TARGET_CAP = 0.8 * max_cap

print(f"Max vehicle capacity: {max_cap}")
print(f"TARGET_CAP (mỗi pickup <=): {TARGET_CAP}")

coords = df[["Latitude", "Longitude"]].values

print(f"Chạy KMeans với K={N_CLUSTERS} ...")
kmeans = KMeans(n_clusters=N_CLUSTERS, random_state=0, n_init="auto")
labels = kmeans.fit_predict(coords)
df["cluster_id"] = labels

pickup_rows = []
mapping_rows = []  # lưu mapping khách gốc -> pickup

pickup_counter = 0

def make_pickup_id(idx: int) -> str:
    # Đủ rộng nếu sau này số pickup > 1000
    return f"P{idx:04d}"

def split_cluster_by_capacity(group: pd.DataFrame, target_cap: float):
    """
    Nhận 1 group (cùng cluster_id), chia thành nhiều 'bin' sao cho
    mỗi bin có tổng Order_Weight <= target_cap (greedy first-fit decreasing).
    Trả về list DataFrame, mỗi df_sub là 1 pickup.
    """
    g = group.copy()
    g = g.sort_values("Order_Weight", ascending=False)

    bins = []
    current_bin = []
    current_weight = 0.0

    for _, row in g.iterrows():
        w = float(row["Order_Weight"])
        # nếu thêm row này vào bin hiện tại mà vượt target_cap,
        # ta đóng bin hiện tại lại (nếu không rỗng) và mở bin mới
        if current_bin and current_weight + w > target_cap:
            bins.append(pd.DataFrame(current_bin))
            current_bin = []
            current_weight = 0.0

        current_bin.append(row)
        current_weight += w

    if current_bin:
        bins.append(pd.DataFrame(current_bin))

    return bins

print("Bắt đầu aggregate + split theo capacity ...")

for c_id, group in df.groupby("cluster_id"):
    # Nếu cụm này nhỏ hơn TARGET_CAP thì khỏi split, cho thành 1 pickup luôn
    total_weight = group["Order_Weight"].sum()

    if total_weight <= TARGET_CAP:
        bins = [group]
    else:
        # Cụm quá to -> chia thành nhiều bin theo capacity
        bins = split_cluster_by_capacity(group, TARGET_CAP)

    for bin_df in bins:
        pickup_id = make_pickup_id(pickup_counter)
        pickup_counter += 1

        total_weight = bin_df["Order_Weight"].sum()
        total_volume = bin_df["Order_Volume"].sum()
        max_priority = bin_df["Priority_Level"].max()
        lat_mean = bin_df["Latitude"].mean()
        lon_mean = bin_df["Longitude"].mean()

        pickup_rows.append({
            "Customer_ID": pickup_id,
            "Order_Weight": total_weight,
            "Order_Volume": total_volume,
            "Service_Time": PICKUP_SERVICE_TIME,
            "Time_Window_Start": PICKUP_TW_START,
            "Time_Window_End": PICKUP_TW_END,
            "Priority_Level": max_priority,
            "Delivery_Type": "Pickup",
            "Latitude": lat_mean,
            "Longitude": lon_mean,
        })

        # mapping khách gốc -> pickup_id
        for orig_id in bin_df["Customer_ID"].tolist():
            mapping_rows.append({
                "Customer_ID": orig_id,
                "Pickup_ID": pickup_id,
            })

pickup_df = pd.DataFrame(pickup_rows)
mapping_df = pd.DataFrame(mapping_rows)

print("Số pickup nodes sau khi split:", len(pickup_df))
print(pickup_df[["Customer_ID", "Order_Weight"]].describe())

print(f"Ghi ra file {OUTPUT_CUSTOMERS} ...")
pickup_df.to_excel(OUTPUT_CUSTOMERS, index=False)

mapping_path = os.path.join(BASE_DIR, "customer_to_pickup_mapping.xlsx")
mapping_df.to_excel(mapping_path, index=False)
print("Ghi mapping gốc -> pickup:", mapping_path)

print("DONE.")
