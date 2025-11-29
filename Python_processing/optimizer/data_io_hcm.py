# data_io_hcm.py
from __future__ import annotations
import os
import pandas as pd


def load_data():
    """
    Load bộ dữ liệu Hồ Chí Minh (D001) từ cùng thư mục với file này.
    """
    this_dir = os.path.dirname(os.path.abspath(__file__))

    customers_path = os.path.join(this_dir, "customers_clustered.xlsx")
    depots_path = os.path.join(this_dir, "depots.xlsx")
    vehicles_path = os.path.join(this_dir, "vehicles.xlsx")
    roads_path = os.path.join(this_dir, "roads.xlsx")

    customers_df = pd.read_excel(customers_path)
    depots_df = pd.read_excel(depots_path)
    vehicles_df = pd.read_excel(vehicles_path)
    roads_df = pd.read_excel(roads_path)

    print("=== LOAD DATA HCMC (D001) ===")
    print(customers_path)
    print(depots_path)
    print(vehicles_path)
    print(roads_path)

    print(
        f"[DATA] customers={len(customers_df)}, depots={len(depots_df)}, "
        f"vehicles={len(vehicles_df)}, roads rows={len(roads_df)}"
    )

    return customers_df, depots_df, vehicles_df, roads_df
