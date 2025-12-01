import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from phong.config import load_data
import pandas as pd
import time

def debug():
    print(">>> DEBUG: Loading data...")
    start = time.time()
    try:
        base_dir = r"D:\A UEH_UNIVERSITY\UEH_Subjects\operation reseach\LMDO\Lastmile\Zzz_data\LMDO processed\Ho_Chi_Minh_City"
        
        print(f"Loading customers from {base_dir}...")
        customers = pd.read_excel(os.path.join(base_dir, "customers.xlsx"))
        print(f"Loaded customers: {customers.shape}")
        
        print("Loading depots...")
        depots = pd.read_excel(os.path.join(base_dir, "depots.xlsx"))
        print(f"Loaded depots: {depots.shape}")
        
        print("Loading vehicles...")
        vehicles = pd.read_excel(os.path.join(base_dir, "vehicles.xlsx"))
        print(f"Loaded vehicles: {vehicles.shape}")
        
        print("Loading roads...")
        roads = pd.read_excel(os.path.join(base_dir, "roads.xlsx"))
        print(f"Loaded roads: {roads.shape}")
        
        print(f"Data loaded in {time.time() - start:.2f}s")
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug()
