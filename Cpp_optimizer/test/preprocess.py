import pandas as pd
import os
import sys

def convert_data(base_dir, output_dir):
    print(f"Converting data from {base_dir} to {output_dir}")
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Define paths
    files = {
        "customers": os.path.join(base_dir, "customers.xlsx"),
        "depots": os.path.join(base_dir, "depots.xlsx"),
        "vehicles": os.path.join(base_dir, "vehicles.xlsx"),
        "roads": os.path.join(base_dir, "roads.xlsx")
    }
    
    # Check existence
    for name, path in files.items():
        if not os.path.exists(path):
            print(f"[ERROR] File not found: {path}")
            return

    # 1. Customers
    print("Processing customers...")
    df_cust = pd.read_excel(files["customers"])
    # Select and rename columns for C++
    df_cust_out = df_cust[[
        "Customer_ID", "Latitude", "Longitude", 
        "Order_Weight", "Order_Volume", 
        "Service_Time", "Time_Window_Start", "Time_Window_End"
    ]].copy()
    
    # Convert time strings to minutes
    def time_to_min(t_str):
        try:
            h, m = map(int, str(t_str).split(':'))
            return h * 60 + m
        except:
            return 0
            
    df_cust_out["Time_Window_Start"] = df_cust_out["Time_Window_Start"].apply(time_to_min)
    df_cust_out["Time_Window_End"] = df_cust_out["Time_Window_End"].apply(time_to_min)
    
    df_cust_out.to_csv(os.path.join(output_dir, "customers.csv"), index=False)

    # 2. Depots
    print("Processing depots...")
    df_depot = pd.read_excel(files["depots"])
    df_depot_out = df_depot[["Depot_ID", "Latitude", "Longitude", "Capacity_Storage"]].copy()
    df_depot_out.to_csv(os.path.join(output_dir, "depots.csv"), index=False)

    # 3. Vehicles
    print("Processing vehicles...")
    df_veh = pd.read_excel(files["vehicles"])
    df_veh_out = df_veh[[
        "Vehicle_ID", "Start_Depot_ID", 
        "Capacity_Weight", "Capacity_Volume", 
        "Max_Working_Hours", "Fixed_Cost", "Variable_Cost", "Max_Distance"
    ]].copy()
    # Convert hours to minutes
    df_veh_out["Max_Working_Hours"] = df_veh_out["Max_Working_Hours"] * 60
    df_veh_out.to_csv(os.path.join(output_dir, "vehicles.csv"), index=False)

    # 4. Roads (Distance Matrix)
    print("Processing roads...")
    df_road = pd.read_excel(files["roads"])
    # We only need distance and time
    df_road_out = df_road[[
        "Origin_Node_ID", "Destination_Node_ID", "Distance_km", "Travel_Time_min"
    ]].copy()
    df_road_out.to_csv(os.path.join(output_dir, "roads.csv"), index=False)
    
    print("Data conversion complete.")

if __name__ == "__main__":
    # Default paths
    # Adjust these to match your actual data location
    data_dir = r"D:\A UEH_UNIVERSITY\UEH_Subjects\operation reseach\LMDO\Lastmile\Zzz_data\LMDO processed\Ho_Chi_Minh_City"
    output_dir = r"D:\A UEH_UNIVERSITY\UEH_Subjects\operation reseach\LMDO\Lastmile\Cpp_optimizer\test\data"
    
    convert_data(data_dir, output_dir)
