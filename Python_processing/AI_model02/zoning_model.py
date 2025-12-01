import math
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt

try:
    from scipy.spatial import ConvexHull
    _HAS_SCIPY = True
except ImportError:
    _HAS_SCIPY = False


# ========================
# Haversine: tính khoảng cách km từ lat/lon
# ========================
def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0  # bán kính Trái Đất km
    phi1 = np.radians(lat1)
    phi2 = np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlambda = np.radians(lon2 - lon1)
    a = np.sin(dphi/2.0)**2 + np.cos(phi1)*np.cos(phi2)*np.sin(dlambda/2.0)**2
    c = 2*np.arctan2(np.sqrt(a), np.sqrt(1-a))
    return R*c


class DepotAwareZoningV2:
    """
    Mô hình AI phân vùng khách hàng theo depot + chọn center + gán vehicle ban đầu.

    Thiết kế để PHÙ HỢP tối đa với pipeline:
      KMeans (zoning spatial-first) -> ALNS -> Tabu Search

    - Spatial-first: cluster ưu tiên (dx, dy, r_km).
    - Time-window & demand: chỉ là feature phụ (soft constraints).
    - Mỗi zone:
        + thuộc 1 depot (Nearest_Depot_ID),
        + có Center_Customer_ID,
        + được gán 1 Assigned_Vehicle_ID (ban đầu).
    - Mỗi customer:
        + Nearest_Depot_ID
        + Zone_ID
        + Init_Vehicle_ID
    """

    def __init__(
        self,
        target_utilization=0.7,
        min_clusters_per_depot=3,
        max_clusters_per_depot=50,
        random_state=42,
        alpha_center=0.5,
        beta_center=0.4,
        gamma_center=0.1,
        w_space=3.0,
        w_time=1.0,
        w_demand=0.7,
        w_other=0.5,
    ):
        """
        target_utilization : hệ số sử dụng tải để ước lượng số cluster/zone.
        min/max_clusters_per_depot : giới hạn số zone cho mỗi depot.
        alpha/beta/gamma : trọng số hàm score chọn center.
        w_space/w_time/w_demand/w_other : trọng số nhóm feature cho KMeans.
        """
        self.target_utilization = target_utilization
        self.min_clusters_per_depot = min_clusters_per_depot
        self.max_clusters_per_depot = max_clusters_per_depot
        self.random_state = random_state
        self.alpha_center = alpha_center
        self.beta_center = beta_center
        self.gamma_center = gamma_center

        self.w_space = w_space
        self.w_time = w_time
        self.w_demand = w_demand
        self.w_other = w_other

        self.scalers_ = {}        # scaler feature cho từng depot
        self.kmeans_ = {}         # KMeans per depot
        self.zone_centers_ = None # DataFrame center của từng zone
        self._fitted = False

    # ------------------------
    # Helper: parse time 'HH:MM'
    # ------------------------
    @staticmethod
    def _parse_time_to_min(t):
        """Chuyển 'HH:MM' -> phút trong ngày."""
        if pd.isna(t):
            return np.nan
        if isinstance(t, (int, float)):
            return float(t)
        s = str(t)
        parts = s.split(':')
        if len(parts) >= 2 and parts[0].isdigit() and parts[1].isdigit():
            return int(parts[0]) * 60 + int(parts[1])
        return np.nan

    # ------------------------
    # Gán depot gần nhất (dùng roads.xlsx)
    # ------------------------
    def _compute_nearest_depot(self, customers, roads):
        """
        Với mỗi Customer_ID, chọn depot có Travel_Time_min nhỏ nhất.
        roads: Origin_Node_ID (depot), Destination_Node_ID (customer).
        """
        idx = roads.groupby("Destination_Node_ID")["Travel_Time_min"].idxmin()
        nearest = roads.loc[idx, ["Destination_Node_ID", "Origin_Node_ID", "Travel_Time_min"]].copy()
        nearest.rename(
            columns={
                "Destination_Node_ID": "Customer_ID",
                "Origin_Node_ID": "Nearest_Depot_ID",
                "Travel_Time_min": "Time_To_Nearest_Depot",
            },
            inplace=True,
        )
        merged = customers.merge(nearest, on="Customer_ID", how="left")
        return merged

    # ------------------------
    # Enrich: thêm feature từ depots + customers
    # ------------------------
    def _build_enriched_customers(self, customers_with_depot, depots):
        """
        Thêm:
          - Depot_Latitude/Longitude
          - dx, dy, r_km (vị trí tương đối vs depot)
          - tw_start_min, tw_end_min, tw_mid, tw_width
          - Return_Flag_Num, one-hot Delivery_Type
        """
        depots_small = depots[["Depot_ID", "Latitude", "Longitude"]].rename(
            columns={"Latitude": "Depot_Latitude", "Longitude": "Depot_Longitude"}
        )
        df = customers_with_depot.merge(
            depots_small, left_on="Nearest_Depot_ID", right_on="Depot_ID", how="left"
        )

        # toạ độ tương đối & khoảng cách tới depot
        df["dx"] = df["Latitude"] - df["Depot_Latitude"]
        df["dy"] = df["Longitude"] - df["Depot_Longitude"]
        df["r_km"] = haversine_km(
            df["Latitude"].values,
            df["Longitude"].values,
            df["Depot_Latitude"].values,
            df["Depot_Longitude"].values,
        )

        # time windows
        df["tw_start_min"] = df["Time_Window_Start"].apply(self._parse_time_to_min)
        df["tw_end_min"]   = df["Time_Window_End"].apply(self._parse_time_to_min)
        df["tw_mid"]   = (df["tw_start_min"] + df["tw_end_min"]) / 2.0
        df["tw_width"] = df["tw_end_min"] - df["tw_start_min"]

        # Return_Flag -> 0/1
        if df["Return_Flag"].dtype == bool:
            df["Return_Flag_Num"] = df["Return_Flag"].astype(int)
        else:
            df["Return_Flag_Num"] = (
                df["Return_Flag"].astype(str).str.lower().eq("true").astype(int)
            )

        # One-hot Delivery_Type
        dummies = pd.get_dummies(df["Delivery_Type"], prefix="Type")
        df = pd.concat([df, dummies], axis=1)

        return df

    def _build_feature_matrix(self, df):
        """
        Tạo ma trận feature X cho KMeans + list feature_cols.
        Chú ý: thứ tự này dùng để apply trọng số.
        """
        feature_cols = [
            "dx", "dy", "r_km",             # SPACE
            "tw_mid", "tw_width",          # TIME
            "Order_Weight", "Order_Volume",# DEMAND
            "Priority_Level",
            "Time_To_Nearest_Depot",
            "Return_Flag_Num",
        ]
        type_cols = [c for c in df.columns if c.startswith("Type_")]
        feature_cols.extend(type_cols)

        X = df[feature_cols].astype(float).fillna(0.0).values
        return X, feature_cols

    def _weight_features(self, X_scaled, feature_cols):
        """
        Sau khi StandardScaler, nhân trọng số cho nhóm feature:
          - SPACE   (dx, dy, r_km)      -> w_space
          - TIME    (tw_mid, tw_width)  -> w_time
          - DEMAND  (Order_Weight, Order_Volume) -> w_demand
          - OTHERS  -> w_other
        """
        weights = np.ones(len(feature_cols))
        for i, col in enumerate(feature_cols):
            if col in ("dx", "dy", "r_km"):
                weights[i] = self.w_space
            elif col in ("tw_mid", "tw_width"):
                weights[i] = self.w_time
            elif col in ("Order_Weight", "Order_Volume"):
                weights[i] = self.w_demand
            else:
                weights[i] = self.w_other
        return X_scaled * weights

    # ------------------------
    # Ước lượng số clusters K cho depot (để dùng cho KMeans)
    # ------------------------
    def _estimate_num_clusters_for_depot(self, depot_id, customers_df, vehicles_df):
        """
        Heuristic: số zone ~ số route dựa trên tổng demand / capacity trung bình.
        Mỗi zone ~ 1 route (ở mức utilization target).
        """
        cust_dep = customers_df[customers_df["Nearest_Depot_ID"] == depot_id]
        veh_dep  = vehicles_df[vehicles_df["Start_Depot_ID"] == depot_id]

        if cust_dep.empty or veh_dep.empty:
            return max(self.min_clusters_per_depot, 1)

        total_demand = cust_dep["Order_Weight"].sum()
        avg_capacity = veh_dep["Capacity_Weight"].mean()

        if pd.isna(avg_capacity) or avg_capacity <= 0:
            avg_capacity = total_demand / max(len(veh_dep), 1)

        expected_routes = total_demand / (avg_capacity * self.target_utilization + 1e-6)
        k = int(math.ceil(expected_routes))
        k = max(self.min_clusters_per_depot, min(self.max_clusters_per_depot, k))
        k = min(k, len(cust_dep))  # không nhiều cluster hơn số khách
        if k <= 0:
            k = 1
        return k

    # ------------------------
    # Chọn customer trung tâm 1 zone
    # ------------------------
    def _select_zone_center(self, zone_df):
        """
        Chọn center trong 1 zone theo score:
          score = alpha * dist_to_depot
                + beta  * avg_dist_to_other_customers
                + gamma * TW_deviation

        - dist_to_depot: Time_To_Nearest_Depot (phút)
        - avg_dist_to_other_customers: khoảng cách hình học (dx, dy)
        - TW_deviation: độ lệch tw_mid so với median của cụm
        """
        if len(zone_df) == 1:
            return zone_df.iloc[0]

        dist_depot = zone_df["Time_To_Nearest_Depot"].astype(float).values

        dx = zone_df["dx"].astype(float).values
        dy = zone_df["dy"].astype(float).values
        coords = np.vstack([dx, dy]).T
        diff = coords[:, None, :] - coords[None, :, :]
        dist_matrix = np.sqrt((diff ** 2).sum(axis=2))
        avg_dist = dist_matrix.mean(axis=1)

        tw_mid = zone_df["tw_mid"].astype(float).values
        median_tw = np.nanmedian(tw_mid)
        tw_dev = np.abs(tw_mid - median_tw)

        def norm(x):
            x = np.asarray(x, float)
            if np.all(~np.isfinite(x)):
                return np.zeros_like(x)
            x = np.where(np.isfinite(x), x, np.nan)
            x_min = np.nanmin(x)
            x_max = np.nanmax(x)
            if not np.isfinite(x_min) or not np.isfinite(x_max) or x_max == x_min:
                return np.zeros_like(x)
            return (x - x_min) / (x_max - x_min)

        nd  = norm(dist_depot)
        nc  = norm(avg_dist)
        ntw = norm(tw_dev)

        score = (
            self.alpha_center * nd
            + self.beta_center  * nc
            + self.gamma_center * ntw
        )
        best_idx = np.argmin(score)
        return zone_df.iloc[best_idx]

    # ------------------------
    # Gán vehicle cho mỗi zone (theo depot + capacity)
    # ------------------------
    def _assign_vehicles_to_zones(self, zone_centers, vehicles):
        """
        Gán 1 vehicle thuộc depot đó cho mỗi zone (heuristic):

        - vehicles depot d: Start_Depot_ID == d
        - sort zones d theo Total_Order_Weight (giảm dần)
        - sort vehicles theo Remaining_Capacity (giảm dần)
        - assign zone nặng nhất cho xe còn capacity lớn nhất
        - Trả về zone_centers có Assigned_Vehicle_ID + Capacity_Remaining_After_Assign.
        """
        zone_centers = zone_centers.copy()
        zone_centers["Assigned_Vehicle_ID"] = None
        zone_centers["Capacity_Remaining_After_Assign"] = np.nan

        out_records = []

        for depot_id, zones_dep in zone_centers.groupby("Depot_ID"):
            zones_dep = zones_dep.copy()
            veh_dep   = vehicles[vehicles["Start_Depot_ID"] == depot_id].copy()

            if veh_dep.empty:
                out_records.append(zones_dep)
                continue

            veh_dep["Remaining_Capacity"] = veh_dep["Capacity_Weight"].astype(float).values

            zones_dep = zones_dep.sort_values("Total_Order_Weight", ascending=False).reset_index(drop=True)

            for i, zrow in zones_dep.iterrows():
                veh_dep = veh_dep.sort_values("Remaining_Capacity", ascending=False).reset_index(drop=True)
                vrow = veh_dep.iloc[0]
                vid  = vrow["Vehicle_ID"]
                remaining  = float(vrow["Remaining_Capacity"])
                demand_z   = float(zrow["Total_Order_Weight"])
                new_remain = remaining - demand_z

                veh_dep.at[0, "Remaining_Capacity"] = new_remain
                zones_dep.at[i, "Assigned_Vehicle_ID"] = vid
                zones_dep.at[i, "Capacity_Remaining_After_Assign"] = new_remain

            out_records.append(zones_dep)

        return pd.concat(out_records, ignore_index=True)

    # ------------------------
    # Fit zoning model: KMeans + chọn center + gán vehicle
    # ------------------------
    def fit(self, customers, depots, roads, vehicles):
        """
        Fit mô hình zoning & center & vehicle từ 4 file.

        Trả về:
          - customers_with_zones: customers + Nearest_Depot_ID, Zone_ID, Init_Vehicle_ID
          - zone_centers:         thông tin center + Assigned_Vehicle_ID mỗi zone.
        """
        # 1) gán depot
        cust = self._compute_nearest_depot(customers, roads)

        # 2) enrich feature
        cust = self._build_enriched_customers(cust, depots)

        all_zone_records = []
        all_customers_out = []

        # 3) KMeans per depot (spatial-first)
        for depot_id, group in cust.groupby("Nearest_Depot_ID"):
            group = group.copy()
            if group.empty:
                continue

            X, feature_cols = self._build_feature_matrix(group)

            scaler = StandardScaler()
            X_scaled = scaler.fit_transform(X)
            X_weighted = self._weight_features(X_scaled, feature_cols)

            self.scalers_[depot_id] = scaler

            k = self._estimate_num_clusters_for_depot(depot_id, cust, vehicles)

            if k <= 0 or len(group) <= k:
                if len(group) <= k:
                    labels = np.arange(len(group))
                else:
                    labels = np.zeros(len(group), dtype=int)
            else:
                km = KMeans(n_clusters=k, random_state=self.random_state, n_init=10)
                labels = km.fit_predict(X_weighted)
                self.kmeans_[depot_id] = km

            group["Cluster_Local"] = labels
            group["Zone_ID"] = [
                f"{depot_id}_Z{int(lbl):03d}" for lbl in group["Cluster_Local"].values
            ]

            zone_ids = group["Zone_ID"].unique()
            for zid in zone_ids:
                sub = group[group["Zone_ID"] == zid].copy()
                center_row = self._select_zone_center(sub)

                zone_record = {
                    "Zone_ID": zid,
                    "Depot_ID": depot_id,
                    "Center_Customer_ID": center_row["Customer_ID"],
                    "Center_Latitude": center_row["Latitude"],
                    "Center_Longitude": center_row["Longitude"],
                    "Num_Customers": len(sub),
                    "Total_Order_Weight": sub["Order_Weight"].sum(),
                    "Total_Order_Volume": sub["Order_Volume"].sum(),
                    "TW_Start_Min_Min": sub["tw_start_min"].min(),
                    "TW_End_Min_Max": sub["tw_end_min"].max(),
                }
                all_zone_records.append(zone_record)

            all_customers_out.append(group)

        customers_with_zones = pd.concat(all_customers_out, ignore_index=True)
        zone_centers = pd.DataFrame(all_zone_records)

        # 4) assign vehicles cho zone
        zone_centers = self._assign_vehicles_to_zones(zone_centers, vehicles)

        # 5) propagate Init_Vehicle_ID xuống customer
        customers_with_zones = customers_with_zones.merge(
            zone_centers[["Zone_ID", "Assigned_Vehicle_ID"]],
            on="Zone_ID",
            how="left"
        )
        customers_with_zones.rename(columns={"Assigned_Vehicle_ID": "Init_Vehicle_ID"}, inplace=True)

        self.zone_centers_ = zone_centers
        self._fitted = True
        return customers_with_zones, zone_centers

    # ------------------------
    # Nơi sau này nhét output từ ALNS + Tabu để refine AI
    # ------------------------
    def refine_with_routes(self, routes_df):
        """
        routes_df: output từ ALNS + Tabu, ví dụ:

            Route_ID | Vehicle_ID | Depot_ID | Customer_ID | Seq | Arrival_Time | ...

        TODO (sau này):
          - Dùng Route_ID như "cluster thực" để học lại zone.
          - Dùng vị trí Seq để đánh giá center tốt hơn.
          - Dùng cost / violation trong từng zone để split/merge/refine.
        """
        raise NotImplementedError("refine_with_routes: integrate ALNS/Tabu output here.")

    # ------------------------
    # Vẽ graph phân vùng (zone + depot + center + optional hull)
    # ------------------------
    def plot_zones(
        self,
        customers_with_zones,
        depots,
        zone_centers,
        sample_zones=None,
        figsize=(8, 8),
    ):
        """
        Vẽ:
        - Customers màu theo Zone_ID
        - Depots: hình vuông
        - Zone centers: hình sao
        - ĐƯỜNG NỐI depot gần nhất ↔ từng zone center (rõ ràng)

        sample_zones: list Zone_ID nếu chỉ muốn vẽ 1 vài zone cho đỡ rối.
        """
        if not self._fitted:
            raise RuntimeError("Model chưa fit, không thể plot. Hãy gọi fit() trước.")

        df    = customers_with_zones
        dep   = depots
        zones = zone_centers

        if sample_zones is not None:
            df    = df[df["Zone_ID"].isin(sample_zones)]
            zones = zones[zones["Zone_ID"].isin(sample_zones)]

        fig, ax = plt.subplots(figsize=figsize)

        # ===== 1. vẽ customers, đồng thời lưu màu cho từng Zone_ID =====
        cmap = plt.cm.get_cmap("tab10")
        zone_ids = list(df["Zone_ID"].unique())
        zone_color = {}

        for idx, zid in enumerate(zone_ids):
            group = df[df["Zone_ID"] == zid]
            color = cmap(idx / max(len(zone_ids), 1))
            zone_color[zid] = color

            ax.scatter(
                group["Longitude"],
                group["Latitude"],
                s=6,
                alpha=0.6,
                color=color,
                label=zid,
            )

        # ===== 2. vẽ depot =====
        ax.scatter(
            dep["Longitude"],
            dep["Latitude"],
            s=100,
            marker="s",
            edgecolors="black",
            linewidths=1.0,
            facecolors="none",
            label="Depots",
        )

        # ===== 3. vẽ zone centers =====
        ax.scatter(
            zones["Center_Longitude"],
            zones["Center_Latitude"],
            s=80,
            marker="*",
            edgecolors="black",
            linewidths=0.9,
            facecolors="yellow",
            label="Zone centers",
        )

        # ===== 4. vẽ đường nối depot ↔ center cho từng zone =====
        for _, zrow in zones.iterrows():
            zid = zrow["Zone_ID"]
            color = zone_color.get(zid, "gray")   # trùng màu với zone

            # ưu tiên Center_Depot_ID nếu có, fallback Depot_ID
            depot_id = zrow.get("Center_Depot_ID", zrow["Depot_ID"])
            dmatch = dep[dep["Depot_ID"] == depot_id]
            if dmatch.empty:
                continue
            drow = dmatch.iloc[0]

            ax.plot(
                [drow["Longitude"], zrow["Center_Longitude"]],
                [drow["Latitude"],  zrow["Center_Latitude"]],
                linestyle="-.",
                linewidth=1.2,
                color=color,
                alpha=0.9,
            )

        ax.set_xlabel("Longitude")
        ax.set_ylabel("Latitude")
        ax.set_title("Customer zoning by AI model (depots & zone centers linked)")
        # legend gọn hơn: chỉ lấy unique labels
        handles, labels = ax.get_legend_handles_labels()
        # giữ lại 3 loại: 1 zone sample, Depots, Zone centers
        new_handles = []
        new_labels = []
        seen = set()
        for h, l in zip(handles, labels):
            if l in seen:
                continue
            if l not in ["Depots", "Zone centers"] and len(new_handles) > 0:
                # chỉ giữ 1 zone mẫu cho legend (tránh 100 label)
                continue
            seen.add(l)
            new_handles.append(h)
            new_labels.append(l)
        ax.legend(new_handles, new_labels, fontsize=7, loc="best")
        ax.grid(True)

        plt.tight_layout()
        plt.show()



# ============================
# MAIN: ví dụ chạy với 4 file thật
# ============================
if __name__ == "__main__":
    # 1. Load 4 file đúng tên & cấu trúc
    import os
    base_dir = os.path.dirname(os.path.abspath(__file__))

    customers = pd.read_excel(os.path.join(base_dir, "customers.xlsx"))
    depots    = pd.read_excel(os.path.join(base_dir, "depots.xlsx"))
    vehicles  = pd.read_excel(os.path.join(base_dir, "vehicles.xlsx"))
    roads     = pd.read_excel(os.path.join(base_dir, "roads.xlsx"))

    zm = DepotAwareZoningV2(
        min_clusters_per_depot=3,
        max_clusters_per_depot=25,
        target_utilization=0.7,
        random_state=42,
        w_space=3.0,   # ưu tiên không gian
        w_time=1.0,
        w_demand=0.7,
        w_other=0.5,
    )

    customers_zoned, zone_centers = zm.fit(customers, depots, roads, vehicles)

    print("Số zone:", customers_zoned["Zone_ID"].nunique())
    print(zone_centers.head())

    customers_zoned.to_csv("customers_with_zones_v2.csv", index=False)
    zone_centers.to_csv("zone_centers_v2.csv", index=False)

    # vẽ mỗi lần vài zone cho dễ nhìn
    #some_zones = zone_centers["Zone_ID"].unique()[:5]
    zm.plot_zones(
        customers_zoned,
        depots,
        zone_centers,
        #sample_zones=some_zones,
        #show_hulls=True,  # nếu chưa cài scipy thì chuyển False
    )
