import folium
import pandas as pd
import os
import random

def visualize_solution(
    routes_path: str, 
    clusters_path: str, 
    depots_path: str, 
    customers_path: str,
    output_html: str = "map_visualization.html"
):
    """
    Visualize solution from output Excel files.
    """
    print(f"[VIS] Loading data for visualization...")
    
    # Load data
    try:
        df_routes = pd.read_excel(routes_path)
        df_clusters = pd.read_excel(clusters_path)
        df_depots = pd.read_excel(depots_path)
        df_customers = pd.read_excel(customers_path)
    except Exception as e:
        print(f"[ERROR] Could not load data for visualization: {e}")
        return

    # Create Map centered on Ho Chi Minh City (approx)
    m = folium.Map(location=[10.762622, 106.660172], zoom_start=12)
    
    # 1. Plot Depots
    depot_coords = {}
    for _, row in df_depots.iterrows():
        did = row['Depot_ID']
        lat, lon = row['Latitude'], row['Longitude']
        depot_coords[did] = (lat, lon)
        
        folium.Marker(
            location=[lat, lon],
            popup=f"Depot: {did}",
            icon=folium.Icon(color='black', icon='home', prefix='fa')
        ).add_to(m)
        
    # 2. Plot Centroids & Routes
    # Generate colors for vehicles
    vehicles = df_routes['Vehicle_ID'].unique()
    colors = ["red", "blue", "green", "purple", "orange", "darkred", "lightred", "beige", "darkblue", "darkgreen", "cadetblue", "darkpurple", "white", "pink", "lightblue", "lightgreen", "gray", "black", "lightgray"]
    veh_color_map = {v: colors[i % len(colors)] for i, v in enumerate(vehicles)}
    
    # Helper to get customer coords
    cust_coords = {}
    for _, row in df_customers.iterrows():
        cust_coords[row['Customer_ID']] = (row['Latitude'], row['Longitude'])
        
    for _, row in df_routes.iterrows():
        vid = row['Vehicle_ID']
        did = row['Depot_ID']
        cid = row['Centroid_Customer_ID']
        cluster_id = row['Cluster_ID']
        
        if cid not in cust_coords: continue
        
        c_lat, c_lon = cust_coords[cid]
        d_lat, d_lon = depot_coords.get(did, (None, None))
        
        color = veh_color_map.get(vid, 'blue')
        
        # Plot Centroid
        folium.CircleMarker(
            location=[c_lat, c_lon],
            radius=6,
            popup=f"Cluster: {cluster_id}<br>Centroid: {cid}<br>Vehicle: {vid}<br>Load: {row['Load_Weight']:.1f}kg",
            color=color,
            fill=True,
            fill_color=color
        ).add_to(m)
        
        # Draw Line Depot -> Centroid
        if d_lat is not None:
            folium.PolyLine(
                locations=[[d_lat, d_lon], [c_lat, c_lon]],
                color=color,
                weight=2,
                opacity=0.7
            ).add_to(m)
            
    # 3. (Optional) Plot Cluster Members as small dots
    # This might be too heavy if too many customers, but let's try for small scale
    # Or just skip to keep map clean. 
    # Let's add them but very small and transparent
    
    print(f"[VIS] Plotting {len(df_clusters)} customers...")
    for _, row in df_clusters.iterrows():
        cid = row['Customer_ID']
        vid = row['Vehicle_ID']
        
        if cid not in cust_coords: continue
        lat, lon = cust_coords[cid]
        color = veh_color_map.get(vid, 'blue')
        
        folium.CircleMarker(
            location=[lat, lon],
            radius=2,
            color=color,
            fill=True,
            fill_opacity=0.4,
            popup=f"Customer: {cid}<br>Cluster: {row['Cluster_ID']}"
        ).add_to(m)

    m.save(output_html)
    print(f"[VIS] Map saved to {output_html}")

if __name__ == "__main__":
    # Example usage
    project_dir = r"d:\A UEH_UNIVERSITY\UEH_Subjects\operation reseach\LMDO\Lastmile\Python_processing\optimizer"
    data_dir = r"D:\A UEH_UNIVERSITY\UEH_Subjects\operation reseach\LMDO\Lastmile\Zzz_data\LMDO processed\Ho_Chi_Minh_City"
    
    visualize_solution(
        routes_path=os.path.join(project_dir, "OUTPUT_PHONG", "result_routes.xlsx"),
        clusters_path=os.path.join(project_dir, "OUTPUT_PHONG", "result_clusters.xlsx"),
        depots_path=os.path.join(data_dir, "depots.xlsx"),
        customers_path=os.path.join(data_dir, "customers.xlsx"), # Note: customers.xlsx, not customers_clustered.xlsx based on config.py
        output_html=os.path.join(project_dir, "OUTPUT_PHONG", "visualization.html")
    )
