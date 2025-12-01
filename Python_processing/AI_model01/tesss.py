import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import matplotlib.patches as mpatches


# Thư mục chứa file .py và các file Excel
BASE_DIR = Path(__file__).resolve().parent

CUSTOMERS_FILE = BASE_DIR / "customers.xlsx"
CLUSTERED_FILE = BASE_DIR / "customers_clustered.xlsx"
MAPPING_FILE   = BASE_DIR / "customer_to_pickup_mapping.xlsx"


def main():
    # 1. Đọc dữ liệu
    customers = pd.read_excel(CUSTOMERS_FILE)
    clustered = pd.read_excel(CLUSTERED_FILE)
    mapping   = pd.read_excel(MAPPING_FILE)

    # 2. Gán khách hàng -> Pickup cluster
    df = customers.merge(mapping, on="Customer_ID", how="left")

    # 3. Gán Pickup cluster -> Depot
    clustered_renamed = clustered.rename(columns={"Customer_ID": "Pickup_ID"})
    df = df.merge(
        clustered_renamed[["Pickup_ID", "Assigned_Depot_ID"]],
        on="Pickup_ID",
        how="left"
    )

    # 4. Bỏ bản ghi thiếu thông tin
    df = df.dropna(subset=["Latitude", "Longitude", "Assigned_Depot_ID"])

    # 5. Mã hóa depot thành số để vẽ màu
    depot_codes = {d: i for i, d in enumerate(sorted(df["Assigned_Depot_ID"].unique()))}
    df["Depot_Code"] = df["Assigned_Depot_ID"].map(depot_codes)

    # 6. Vẽ scatter: khách hàng màu theo kho
    plt.figure(figsize=(8, 8))
    sc = plt.scatter(
        df["Longitude"],
        df["Latitude"],
        s=4,                       # kích thước điểm
        c=df["Depot_Code"]         # màu theo kho
    )
    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    plt.title("Phân bố khách hàng theo kho (cluster → depot)")

    # 7. Thêm legend cho từng depot
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
