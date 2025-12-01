import pandas as pd
from sklearn.cluster import KMeans
import numpy as np
import math
import os
import matplotlib.pyplot as plt

# ========= CONFIG =========
BASE_DIR = "/Users/alicecin/Documents/Lastmile/Zzz_data/LMDO processed/Ho_Chi_Minh_City"
INPUT_CUSTOMERS = os.path.join(BASE_DIR, "customers.xlsx")            # file gốc
INPUT_VEHICLES = os.path.join(BASE_DIR, "vehicles.xlsx")              # để đọc capacity
INPUT_DEPOTS   = os.path.join(BASE_DIR, "depots.xlsx")                # để gán depot gần nhất
OUTPUT_CUSTOMERS = os.path.join(BASE_DIR, "customers_clustered1.xlsx") # file pickup mới

N_CLUSTERS = 200   # số cluster ban đầu (có thể tăng nếu muốn)
PICKUP_TW_START = "08:00"
PICKUP_TW_END = "21:00"
PICKUP_SERVICE_TIME = 10.0
# ==========================

print("Đọc customers.xlsx ...")
df = pd.read_excel(INPUT_CUSTOMERS)
veh = pd.read_excel(INPUT_VEHICLES)
depots = pd.read_excel(INPUT_DEPOTS)

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

# ==== CHỖ ĐỔI SO VỚI CODE CŨ ====
cap_series = veh["Capacity_Weight"]
min_cap = cap_series.min()
median_cap = cap_series.median()
max_cap = cap_series.max()
TARGET_CAP = 0.8 * median_cap   # dùng median thay vì max

print(f"Vehicle capacity (min/median/max): {min_cap} / {median_cap} / {max_cap}")
print(f"TARGET_CAP (mỗi pickup <=): {TARGET_CAP}")
# ================================

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

# =====================================================================
#  GÁN DEPOT CHO MỖI PICKUP (kho nào phụ trách cụm nào?)
# =====================================================================

# Tính max capacity theo từng depot (dựa trên các xe xuất phát ở depot đó)
if "Start_Depot_ID" not in veh.columns:
    raise ValueError("vehicles.xlsx thiếu cột 'Start_Depot_ID' – kiểm tra lại file input.")

cap_per_depot = (
    veh.groupby("Start_Depot_ID")["Capacity_Weight"]
    .max()
    .to_dict()
)

# Index depots theo Depot_ID để truy cập nhanh
if "Depot_ID" not in depots.columns:
    raise ValueError("depots.xlsx thiếu cột 'Depot_ID' – kiểm tra lại file input.")

depots_idx = depots.set_index("Depot_ID")

def assign_depot_for_pickup(row):
    lat = row["Latitude"]
    lon = row["Longitude"]
    w   = row["Order_Weight"]

    # Những depot có ít nhất 1 xe đủ tải cho pickup này
    candidate_depots = [d for d, cap in cap_per_depot.items() if cap >= w]

    # Nếu không có depot nào đủ tải (lý thuyết hiếm khi xảy ra) -> cho phép tất cả
    if not candidate_depots:
        candidate_depots = list(cap_per_depot.keys())

    best_depot = None
    best_dist2 = None

    for depot_id in candidate_depots:
        if depot_id not in depots_idx.index:
            continue
        dep = depots_idx.loc[depot_id]
        dlat = dep["Latitude"] - lat
        dlon = dep["Longitude"] - lon
        dist2 = dlat * dlat + dlon * dlon
        if best_dist2 is None or dist2 < best_dist2:
            best_dist2 = dist2
            best_depot = depot_id

    return best_depot

print("Gán depot gần nhất (theo vị trí + đủ tải) cho từng pickup ...")
pickup_df["Assigned_Depot_ID"] = pickup_df.apply(assign_depot_for_pickup, axis=1)

# =====================================================================
#  GÁN XE CHO MỖI PICKUP (xe nào phục vụ cụm đó?)
#  -> đây là lời giải khởi tạo đơn giản, để sau này ALNS/Tabu tối ưu thêm
# =====================================================================

pickup_df["Assigned_Vehicle_ID"] = None

# Khởi tạo remaining capacity cho từng xe
remaining_cap = veh.set_index("Vehicle_ID")["Capacity_Weight"].to_dict()

for depot_id, group in pickup_df.groupby("Assigned_Depot_ID"):
    veh_ids = veh.loc[veh["Start_Depot_ID"] == depot_id, "Vehicle_ID"].tolist()
    if not veh_ids:
        # depot này không có xe xuất phát -> bỏ qua (hoặc xử lý tùy bạn)
        continue

    # Sort pickup theo trọng lượng giảm dần để pack tốt hơn
    group_sorted = group.sort_values("Order_Weight", ascending=False)

    for idx, row in group_sorted.iterrows():
        w = row["Order_Weight"]

        best_vid = None
        best_rem = None

        # tìm xe còn đủ tải cho pickup này và khi gán xong thì dư ít nhất (best-fit)
        for vid in veh_ids:
            cap_left = remaining_cap.get(vid, 0.0)
            if cap_left >= w:
                rem = cap_left - w
                if best_rem is None or rem < best_rem:
                    best_rem = rem
                    best_vid = vid

        # nếu không xe nào còn đủ tải: gán tạm cho xe có remaining lớn nhất
        # (coi như trong thực tế xe đó sẽ chạy nhiều lượt, sau này ALNS sẽ sửa)
        if best_vid is None:
            best_vid = max(veh_ids, key=lambda v: remaining_cap.get(v, 0.0))

        pickup_df.at[idx, "Assigned_Vehicle_ID"] = best_vid
        remaining_cap[best_vid] = remaining_cap.get(best_vid, 0.0) - w

print("Một vài dòng ví dụ sau khi gán depot + xe:")
print(pickup_df[["Customer_ID", "Order_Weight", "Assigned_Depot_ID", "Assigned_Vehicle_ID"]].head())

# =====================================================================
#  Ghi file output
#  - customers_clustered.xlsx: danh sách pickup + depot + xe + time window + service_time
#  - customer_to_pickup_mapping.xlsx: mapping khách gốc -> pickup
# =====================================================================

print(f"Ghi ra file {OUTPUT_CUSTOMERS} ...")
pickup_df.to_excel(OUTPUT_CUSTOMERS, index=False)

mapping_path = os.path.join(BASE_DIR, "customer_to_pickup_mapping.xlsx")
mapping_df.to_excel(mapping_path, index=False)
print("Ghi mapping gốc -> pickup:", mapping_path)

# === IN SƠ ĐỒ PHÂN CỤM DẠNG TEXT ===
print("\n=== Sơ đồ phân cụm (Pickup node -> các Customer gốc) ===")
for pickup_id, group in mapping_df.groupby("Pickup_ID"):
    customers_in_pickup = group["Customer_ID"].tolist()
    print(f"{pickup_id}: {len(customers_in_pickup)} customers -> {customers_in_pickup[:10]}", end="")
    if len(customers_in_pickup) > 10:
        print(f" ... (+{len(customers_in_pickup) - 10} nữa)")
    else:
        print("")

# === VẼ SƠ ĐỒ PHÂN CỤM (HÌNH) ===
plt.figure(figsize=(10, 8))
plt.scatter(df["Longitude"], df["Latitude"],
            c=df["cluster_id"], s=5, alpha=0.5, label="Original customers")
plt.scatter(pickup_df["Longitude"], pickup_df["Latitude"],
            s=50, marker="x", c="red", label="Pickup nodes")

plt.xlabel("Longitude")
plt.ylabel("Latitude")
plt.title(f"KMeans clustering + capacity split (K={N_CLUSTERS})")
plt.legend()
plt.tight_layout()

plot_path = os.path.join(BASE_DIR, "cluster_map.png")
plt.savefig(plot_path, dpi=200)
plt.close()
print("Đã vẽ sơ đồ phân cụm lưu tại:", plot_path)

print("DONE.")