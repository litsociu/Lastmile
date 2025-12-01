import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import matplotlib.patches as mpatches


# Thư mục chứa file .py và file Excel
BASE_DIR = Path(__file__).resolve().parent
CLUSTERED_FILE = BASE_DIR / "customers_clustered.xlsx"


def main():
    # 1. Đọc dữ liệu gốc các pickup cluster
    df = pd.read_excel(CLUSTERED_FILE)

    # Kỳ vọng có các cột:
    # Customer_ID (P0000...), Latitude, Longitude, Assigned_Depot_ID, ...
    required_cols = ["Customer_ID", "Latitude", "Longitude", "Assigned_Depot_ID"]
    for c in required_cols:
        if c not in df.columns:
            raise ValueError(f"Thiếu cột {c} trong {CLUSTERED_FILE.name}")

    # 2. Loại bỏ bản ghi thiếu thông tin
    df = df.dropna(subset=["Latitude", "Longitude", "Assigned_Depot_ID"])

    # 3. Mã hóa depot thành số để vẽ màu
    depots = sorted(df["Assigned_Depot_ID"].unique())
    depot_codes = {d: i for i, d in enumerate(depots)}
    df["Depot_Code"] = df["Assigned_Depot_ID"].map(depot_codes)

    # 4. Vẽ scatter: mỗi điểm là 1 pickup cluster
    plt.figure(figsize=(8, 8))
    sc = plt.scatter(
        df["Longitude"],
        df["Latitude"],
        s=20,                     # mỗi cluster là 1 chấm to hơn
        c=df["Depot_Code"]        # màu theo depot
    )
    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    plt.title("Pickup clusters theo depot (dữ liệu gốc)")

    # 5. Legend cho từng depot
    cmap = sc.cmap
    norm = sc.norm
    handles = []
    for depot, code in depot_codes.items():
        color = cmap(norm(code))
        handles.append(mpatches.Patch(color=color, label=depot))
    plt.legend(handles=handles, title="Depot", loc="best")

    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    main()
