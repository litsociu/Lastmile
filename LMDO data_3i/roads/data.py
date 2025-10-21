import pandas as pd
from pathlib import Path
import glob


def load_data():
    """
    Đọc và chuẩn hóa dữ liệu từ các file gốc:
    - customers_vietnam.xlsx
    - depots_vietnam.xlsx
    - vehicles_vietnam.xlsx
    - roads_*/roads_*.csv
    Trả về 4 DataFrame đã sẵn sàng cho solver.
    """
    BASE_DIR = Path(__file__).resolve().parent.parent
    DATA_DIR = BASE_DIR / "roads"

    # --- Đọc dữ liệu chính ---
    df_customers = pd.read_excel(BASE_DIR / "customers_vietnam.xlsx")
    df_depots = pd.read_excel(BASE_DIR / "depots_vietnam.xlsx")
    df_vehicles = pd.read_excel(BASE_DIR / "vehicles_vietnam.xlsx")

    print("✅ Đã đọc xong file customers, depots, và vehicles.")

    # --- Chuẩn hóa VEHICLES ---
    for col in ["Capacity_Volume", "Fixed_Cost", "Variable_Cost"]:
        df_vehicles[col] = (
            df_vehicles[col]
            .astype(str)
            .str.replace(",", ".", regex=False)
            .astype(float)
        )

    numeric_cols = ["Capacity_Weight", "Max_Distance", "Max_Working_Hours"]
    df_vehicles[numeric_cols] = df_vehicles[numeric_cols].apply(pd.to_numeric, errors="coerce")

    # Chuẩn hóa depot ID
    df_vehicles["Start_Depot_ID"] = df_vehicles["Start_Depot_ID"].astype(str).str.strip().str.upper()
    df_vehicles["End_Depot_ID"] = df_vehicles["End_Depot_ID"].astype(str).str.strip().str.upper()

    # --- Chuẩn hóa DEPOTS ---
    df_depots["Depot_ID"] = df_depots["Depot_ID"].astype(str).str.strip().str.upper()
    if "Operating_Hours" in df_depots.columns:
        df_depots[["Open_Time", "Close_Time"]] = df_depots["Operating_Hours"].str.split("-", expand=True)
        df_depots["Open_Time"] = pd.to_datetime(df_depots["Open_Time"], format="%H:%M", errors="coerce")
        df_depots["Close_Time"] = pd.to_datetime(df_depots["Close_Time"], format="%H:%M", errors="coerce")

    # --- Chuẩn hóa CUSTOMERS ---
    df_customers["Customer_ID"] = df_customers["Customer_ID"].astype(str).str.strip().str.upper()
    for col in ["Demand_Weight", "Demand_Volume"]:
        if col in df_customers.columns:
            df_customers[col] = pd.to_numeric(df_customers[col], errors="coerce")

    if "Delivery_Time_Window" in df_customers.columns:
        df_customers[["Time_Start", "Time_End"]] = df_customers["Delivery_Time_Window"].str.split("-", expand=True)
        df_customers["Time_Start"] = pd.to_datetime(df_customers["Time_Start"], format="%H:%M", errors="coerce")
        df_customers["Time_End"] = pd.to_datetime(df_customers["Time_End"], format="%H:%M", errors="coerce")

    # --- Đọc & gộp roads ---
    road_files = glob.glob(str(DATA_DIR / "roads_*/*.csv"))
    if not road_files:
        raise FileNotFoundError("❌ Không tìm thấy bất kỳ file road nào trong thư mục /roads/")
    
    list_of_road_dfs = []
    for f in road_files:
        df = pd.read_csv(f)
        df["Origin_Node_ID"] = df["Origin_Node_ID"].astype(str).str.strip().str.upper()
        df["Destination_Node_ID"] = df["Destination_Node_ID"].astype(str).str.strip().str.upper()
        df["Distance_km"] = pd.to_numeric(df["Distance_km"], errors="coerce")
        df["Travel_Time_min"] = pd.to_numeric(df["Travel_Time_min"], errors="coerce")
        df = df.dropna(subset=["Origin_Node_ID", "Destination_Node_ID", "Distance_km"])
        list_of_road_dfs.append(df)

    df_roads_full = pd.concat(list_of_road_dfs, ignore_index=True)
    print(f"✅ Đã gộp xong {len(road_files)} file roads ({len(df_roads_full):,} cung đường).")

    # --- Kiểm tra tính toàn vẹn ---
    missing_starts = set(df_vehicles["Start_Depot_ID"]) - set(df_depots["Depot_ID"])
    if missing_starts:
        print(f"⚠️ Depot không tồn tại trong depots_vietnam.xlsx: {missing_starts}")

    missing_customers = set(df_customers["Customer_ID"]) - set(df_roads_full["Destination_Node_ID"])
    if missing_customers:
        print(f"⚠️ Một số khách hàng không có cung đường: {list(missing_customers)[:10]} ...")

    return df_customers, df_depots, df_vehicles, df_roads_full



def build_cost_lookup(df_roads_full, df_depots, df_customers, mode="time"):
    """
    Tạo từ điển lookup chi phí (hoặc thời gian) giữa các node.
    mode = 'time' hoặc 'distance'
    """
    traffic_multipliers = {"LOW": 1.0, "MEDIUM": 1.5, "HIGH": 2.0}

    # Làm sạch dữ liệu roads
    df_roads_full["Traffic_Level"] = df_roads_full["Traffic_Level"].astype(str).str.strip().str.title()
    df_roads_full["Distance_km"] = pd.to_numeric(df_roads_full["Distance_km"], errors="coerce")
    df_roads_full["Travel_Time_min"] = pd.to_numeric(df_roads_full["Travel_Time_min"], errors="coerce")
    df_roads_full = df_roads_full.dropna(subset=["Distance_km", "Travel_Time_min"])

    all_nodes = list(df_depots["Depot_ID"]) + list(df_customers["Customer_ID"])
    node_index = {node: i for i, node in enumerate(all_nodes)}

    cost_lookup = {}
    for row in df_roads_full.itertuples(index=False):
        o, d = row.Origin_Node_ID, row.Destination_Node_ID
        if o not in node_index or d not in node_index:
            continue

        if mode == "time":
            base_value = row.Travel_Time_min
            factor = traffic_multipliers.get(row.Traffic_Level.title(), 1.0)
            cost = base_value * factor
        else:
            cost = row.Distance_km

        i, j = node_index[o], node_index[d]
        cost_lookup[(i, j)] = cost
        cost_lookup[(j, i)] = cost  # thêm 2 chiều

    print(f"🔹 Đã tạo cost lookup cho {len(cost_lookup):,} cung đường (2 chiều).")
    return cost_lookup, all_nodes
