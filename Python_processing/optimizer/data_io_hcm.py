# data_io_hcm.py
from __future__ import annotations
import os
import pandas as pd


def load_data():
    """
    Load bộ dữ liệu Hồ Chí Minh (D001) nằm cùng thư mục với hcm.py.
    """
    BASE_DIR = "/Users/alicecin/Documents/Lastmile/Zzz_data/LMDO processed/Ho_Chi_Minh_City"

    customers_path = os.path.join(BASE_DIR, "customers_clustered.xlsx")
    depots_path    = os.path.join(BASE_DIR, "depots.xlsx")
    vehicles_path  = os.path.join(BASE_DIR, "vehicles.xlsx")
    roads_path     = os.path.join(BASE_DIR, "roads.xlsx")

    customers_df = pd.read_excel(customers_path)
    depots_df    = pd.read_excel(depots_path)
    vehicles_df  = pd.read_excel(vehicles_path)
    roads_df     = pd.read_excel(roads_path)

    print("=== LOAD DATA HCMC (D001) ===")
    print(customers_path)
    print(depots_path)
    print(vehicles_path)
    print(roads_path)

    print(f"[DATA] customers={len(customers_df)}, depots={len(depots_df)}, "
          f"vehicles={len(vehicles_df)}, roads rows={len(roads_df)}")

    return customers_df, depots_df, vehicles_df, roads_df