import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from pathlib import Path

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score


# =========================
#  CẤU HÌNH ĐƯỜNG DẪN
# =========================
BASE_DIR = Path(__file__).resolve().parent
CUSTOMERS_FILE = BASE_DIR / "customers.xlsx"
CLUSTERED_FILE = BASE_DIR / "customers_clustered.xlsx"
MAPPING_FILE   = BASE_DIR / "customer_to_pickup_mapping.xlsx"


def load_and_merge():
    """Đọc 3 file Excel và gộp lại thành 1 DataFrame cấp độ customer."""

    customers = pd.read_excel(CUSTOMERS_FILE)
    clustered = pd.read_excel(CLUSTERED_FILE)
    mapping   = pd.read_excel(MAPPING_FILE)

    # 1) map Customer_ID -> Pickup_ID (cluster)
    df = customers.merge(mapping, on="Customer_ID", how="left")

    # 2) map Pickup_ID -> Assigned_Depot_ID từ bảng clustered
    clustered_renamed = clustered.rename(columns={"Customer_ID": "Pickup_ID"})
    df = df.merge(
        clustered_renamed[["Pickup_ID", "Assigned_Depot_ID"]],
        on="Pickup_ID",
        how="left"
    )

    # Bỏ các bản ghi thiếu key chính
    df = df.dropna(
        subset=["Pickup_ID", "Assigned_Depot_ID", "Latitude", "Longitude",
                "Order_Weight", "Order_Volume"]
    )

    return df, clustered_renamed


def train_cluster_model(df):
    """
    Học mô hình dự đoán cụm (Pickup_ID) từ feature customer:
    - numeric: lat, lon, weight, volume
    - categorical: city, delivery_type, return_flag
    """

    numeric_features = ["Latitude", "Longitude", "Order_Weight", "Order_Volume"]
    categorical_features = ["City", "Delivery_Type", "Return_Flag"]

    feature_cols = numeric_features + categorical_features

    X = df[feature_cols]
    y = df["Pickup_ID"]

    preprocess = ColumnTransformer(
        transformers=[
            ("num", "passthrough", numeric_features),
            ("cat", OneHotEncoder(handle_unknown="ignore"), categorical_features),
        ]
    )

    model = RandomForestClassifier(
        n_estimators=100,        # có thể tăng nếu máy khỏe
        random_state=42,
        n_jobs=-1
    )

    clf = Pipeline([
        ("preprocess", preprocess),
        ("model", model),
    ])

    X_train, X_test, y_train, y_test = train_test_split(
        X, y,
        test_size=0.2,
        random_state=42,
        stratify=y
    )

    clf.fit(X_train, y_train)

    y_pred = clf.predict(X_test)
    acc = accuracy_score(y_test, y_pred)
    print(f"[INFO] Cluster prediction accuracy (Pickup_ID): {acc:.4f}")

    return clf, feature_cols


def predict_cluster_and_depot(df, clustered_renamed, clf, feature_cols):
    """Dùng model để dự đoán Pickup_ID và suy ra Depot."""

    X_all = df[feature_cols]
    df["Pickup_Pred"] = clf.predict(X_all)

    # Map Pickup_ID -> Depot
    pickup_to_depot = clustered_renamed.set_index("Pickup_ID")["Assigned_Depot_ID"].to_dict()
    df["Depot_Pred"] = df["Pickup_Pred"].map(pickup_to_depot)

    return df


def plot_predicted_clusters(df):
    """Vẽ scatter dựa trên cluster dự đoán (Pickup_Pred) -> lưu predicted_clusters.png"""

    # Mã hóa Pickup_Pred thành số để tô màu
    clusters = sorted(df["Pickup_Pred"].unique())
    cluster_codes = {cid: i for i, cid in enumerate(clusters)}
    df["Cluster_Code_Pred"] = df["Pickup_Pred"].map(cluster_codes)

    plt.figure(figsize=(8, 8))
    sc = plt.scatter(
        df["Longitude"],
        df["Latitude"],
        s=2,
        c=df["Cluster_Code_Pred"]
    )
    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    plt.title("Predicted clusters (Pickup_Pred)")

    plt.tight_layout()
    out_file = BASE_DIR / "predicted_clusters.png"
    plt.savefig(out_file, dpi=300)
    print(f"[INFO] Saved cluster prediction figure to: {out_file}")
    plt.close()


def plot_predicted_depots(df):
    """Vẽ scatter dựa trên depot dự đoán (Depot_Pred) -> lưu predicted_depots.png"""

    df = df.dropna(subset=["Depot_Pred"]).copy()

    depots = sorted(df["Depot_Pred"].unique())
    depot_codes = {d: i for i, d in enumerate(depots)}
    df["Depot_Code_Pred"] = df["Depot_Pred"].map(depot_codes)

    plt.figure(figsize=(8, 8))
    sc = plt.scatter(
        df["Longitude"],
        df["Latitude"],
        s=2,
        c=df["Depot_Code_Pred"]
    )
    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    plt.title("Predicted depots (from predicted clusters)")

    # Thêm legend depot
    cmap = sc.cmap
    norm = sc.norm
    handles = []
    for depot, code in depot_codes.items():
        color = cmap(norm(code))
        handles.append(mpatches.Patch(color=color, label=depot))
    plt.legend(handles=handles, title="Depot", loc="best")

    plt.tight_layout()
    out_file = BASE_DIR / "predicted_depots.png"
    plt.savefig(out_file, dpi=300)
    print(f"[INFO] Saved depot prediction figure to: {out_file}")
    plt.close()


def main():
    df, clustered_renamed = load_and_merge()
    print(f"[INFO] Data shape: {df.shape}")

    clf, feature_cols = train_cluster_model(df)
    df = predict_cluster_and_depot(df, clustered_renamed, clf, feature_cols)

    # 2 hình:
    #   - predicted_clusters.png
    #   - predicted_depots.png
    plot_predicted_clusters(df)
    plot_predicted_depots(df)


if __name__ == "__main__":
    main()
