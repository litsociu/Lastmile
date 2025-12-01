import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import contextily as cx

# =======================
# CONFIG
# =======================
DRAW_ROUTES = True        # nếu thấy rối quá thì chỉnh thành False
OUTPUT_FIG = "vn_zones_routes_customers.png"

# =======================
# 1. Đọc dữ liệu
# =======================

depots = pd.read_csv("depots.csv")
inter = pd.read_csv("intermediate_customers.csv")
routes = pd.read_csv("routes_best_objective_cpp.csv")
customers = pd.read_csv("customers.csv")

# depots.csv:  Depot_ID, City, Latitude, Longitude, ...
# intermediate_customers.csv:
#   Inter_Index,Inter_ID,Zone_Index,Depot_ID,Medoid_Customer_ID,
#   Latitude,Longitude,Weight,Volume,Service
# routes_best_objective_cpp.csv:
#   Route_Index,Depot_ID,Vehicle_ID,Seq,Customer_ID,Latitude,Longitude
# customers.csv:
#   Customer_ID, Latitude, Longitude, ...

# =======================
# 2. Tạo GeoDataFrame
# =======================

# Depots
gdf_depots = gpd.GeoDataFrame(
    depots.copy(),
    geometry=gpd.points_from_xy(depots["Longitude"], depots["Latitude"]),
    crs="EPSG:4326"
)

# Intermediate customers (zone medoids)
gdf_inter = gpd.GeoDataFrame(
    inter.copy(),
    geometry=gpd.points_from_xy(inter["Longitude"], inter["Latitude"]),
    crs="EPSG:4326"
)

# Raw customers
gdf_customers = gpd.GeoDataFrame(
    customers.copy(),
    geometry=gpd.points_from_xy(customers["Longitude"], customers["Latitude"]),
    crs="EPSG:4326"
)

# Route points (intermediate customers trên từng route)
gdf_routes_points = gpd.GeoDataFrame(
    routes.copy(),
    geometry=gpd.points_from_xy(routes["Longitude"], routes["Latitude"]),
    crs="EPSG:4326"
)

# =======================
# 3. Chuyển CRS sang WebMercator để add basemap
# =======================

gdf_depots = gdf_depots.to_crs(epsg=3857)
gdf_inter = gdf_inter.to_crs(epsg=3857)
gdf_customers = gdf_customers.to_crs(epsg=3857)
gdf_routes_points = gdf_routes_points.to_crs(epsg=3857)

# =======================
# 4. Vẽ bản đồ
# =======================

fig, ax = plt.subplots(figsize=(14, 14), dpi=200)

# 4.1. Raw customers: nền xám, nhỏ, hơi mờ
gdf_customers.plot(
    ax=ax,
    markersize=5,
    color="lightgrey",
    alpha=0.5,
    label="Raw customers"
)

# 4.2. Zone medoids (intermediate customers), tô màu theo Depot_ID
gdf_inter.plot(
    ax=ax,
    column="Depot_ID",
    markersize=40,
    alpha=0.9,
    legend=True,
    cmap="tab10",
    edgecolor="black",
    linewidth=0.3
)

# 4.3. Depots: tam giác đỏ, to
gdf_depots.plot(
    ax=ax,
    marker="^",
    color="red",
    markersize=160,
    edgecolor="black",
    linewidth=0.8,
    label="Depot"
)

# 4.4. Routes: depot -> zone -> depot (màu xám mờ cho đỡ rối)
if DRAW_ROUTES:
    for rid, group in gdf_routes_points.groupby("Route_Index"):
        group_sorted = group.sort_values("Seq")
        xs = group_sorted.geometry.x.values
        ys = group_sorted.geometry.y.values

        depot_id = group_sorted["Depot_ID"].iloc[0]
        dep_row = gdf_depots[gdf_depots["Depot_ID"] == depot_id]
        if dep_row.empty:
            continue

        dep_x = dep_row.geometry.x.values[0]
        dep_y = dep_row.geometry.y.values[0]

        # depot -> điểm đầu
        ax.plot([dep_x, xs[0]], [dep_y, ys[0]],
                linewidth=0.6, alpha=0.25, color="gray")
        # (sau này có nhiều điểm trong trip thì nối thêm giữa các điểm)
        ax.plot(xs, ys, linewidth=0.6, alpha=0.25, color="gray")
        # điểm cuối -> depot
        ax.plot([xs[-1], dep_x], [ys[-1], dep_y],
                linewidth=0.6, alpha=0.25, color="gray")

# 4.5. Thêm bản đồ nền OpenStreetMap
cx.add_basemap(
    ax,
    crs=gdf_depots.crs,
    source=cx.providers.OpenStreetMap.Mapnik
)

# 4.6. Zoom vừa khít (dùng cả depot + inter + customers)
all_bounds = gpd.GeoSeries(
    list(gdf_depots.geometry) +
    list(gdf_inter.geometry) +
    list(gdf_customers.geometry),
    crs=gdf_depots.crs
).total_bounds  # [minx, miny, maxx, maxy]

pad_x = (all_bounds[2] - all_bounds[0]) * 0.05
pad_y = (all_bounds[3] - all_bounds[1]) * 0.05

ax.set_xlim(all_bounds[0] - pad_x, all_bounds[2] + pad_x)
ax.set_ylim(all_bounds[1] - pad_y, all_bounds[3] + pad_y)

ax.set_axis_off()
ax.set_title("Depots, Zones & Raw Customers on Vietnam Map", fontsize=16)

# Do gdf_inter đã có legend theo Depot_ID, thêm legend cho depot + raw customers
ax.legend(loc="lower left")

plt.tight_layout()
plt.savefig(OUTPUT_FIG, dpi=300, bbox_inches="tight")
plt.show()

print(f"Saved figure to {OUTPUT_FIG}")
