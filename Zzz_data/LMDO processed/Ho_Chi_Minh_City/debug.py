import pandas as pd, os

BASE_DIR = "/Users/alicecin/Documents/Lastmile/Zzz_data/LMDO processed/Ho_Chi_Minh_City"
cus = pd.read_excel(os.path.join(BASE_DIR, "customers_clustered.xlsx"))
veh = pd.read_excel(os.path.join(BASE_DIR, "vehicles.xlsx"))

max_cap = veh["Capacity_Weight"].max()
g = cus.groupby("Customer_ID")["Order_Weight"].sum()

print("Max vehicle capacity:", max_cap)
print(g.describe())
print("Số pickup có weight > max_cap:", (g > max_cap).sum(), "/", len(g))
print("Số pickup có weight > 0.8*max_cap:", (g > 0.8*max_cap).sum(), "/", len(g))
