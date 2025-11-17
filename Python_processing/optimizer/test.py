#!/usr/bin/env python3
"""
cluster_leader_road_full_fixed_for_user.py

Improved, robust, and scalable version of your pipeline suitable for large customer sets.
Key changes (compared with your original):
 - Automatically infers customer columns (Customer_ID/Latitude/Longitude) but allows explicit override.
 - Uses full shortest-path graph if provided and small enough; otherwise falls back to a scalable KMeans+medoid approach.
 - For large N (>2000) uses MiniBatchKMeans and selects medoid as the point nearest the cluster centroid (fast and stable).
 - For small N (<=2000) preserves PAM k-medoids exact routine.
 - Vectorized haversine implementations for speed.
 - Safe handling of missing graph nodes, unreachable pairs, and deterministic seeding.
 - CLI friendly and writes outputs to files (clusters xlsx, objective csv, folium map html).

Usage:
    python cluster_leader_road_full_fixed_for_user.py --customers customers.xlsx --depot-lat 10.78 --depot-lon 106.69 --pmin 2 --pmax 8

Author: Assistant (adapted for user's dataset)
Fixed: 2025-11-16
"""

import os
import sys
import math
import argparse
import time
from collections import defaultdict

import pandas as pd
import numpy as np
from sklearn.cluster import MiniBatchKMeans, KMeans
import networkx as nx
import folium

# ---------- Utilities ----------

def haversine_km_pair(a, b):
    lat1, lon1 = a
    lat2, lon2 = b
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    s = math.sin(dlat / 2.0)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2.0)**2
    return 2 * R * math.asin(min(1.0, math.sqrt(s)))


def haversine_matrix(A, B):
    """Vectorized haversine. A: (m,2), B: (n,2) -> (m,n)"""
    lat1 = np.radians(A[:, 0])[:, None]
    lon1 = np.radians(A[:, 1])[:, None]
    lat2 = np.radians(B[:, 0])[None, :]
    lon2 = np.radians(B[:, 1])[None, :]
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    sin_dlat = np.sin(dlat / 2.0) ** 2
    sin_dlon = np.sin(dlon / 2.0) ** 2
    a = sin_dlat + np.cos(lat1) * np.cos(lat2) * sin_dlon
    a = np.minimum(1.0, a)
    R = 6371.0
    return 2 * R * np.arcsin(np.sqrt(a))


# ---------- PAM k-medoids (exact, small N) ----------

def pam_medoids(distance_matrix, k, initial_medoids=None, max_iter=200, random_state=0):
    rng = np.random.RandomState(random_state)
    N = distance_matrix.shape[0]
    if k <= 0 or k > N:
        raise ValueError("k must be in 1..N")
    if initial_medoids is None:
        medoids = list(rng.choice(N, k, replace=False))
    else:
        medoids = list(initial_medoids)
    medoids = list(dict.fromkeys(medoids))
    while len(medoids) < k:
        cand = int(rng.choice(N))
        if cand not in medoids:
            medoids.append(cand)
    for _ in range(max_iter):
        dists_to_meds = distance_matrix[:, medoids]
        assign = np.argmin(dists_to_meds, axis=1)
        current_cost = distance_matrix[np.arange(N), np.array(medoids)[assign]].sum()
        improved = False
        for i_med_idx in range(len(medoids)):
            for cand in range(N):
                if cand in medoids:
                    continue
                new_medoids = medoids.copy()
                new_medoids[i_med_idx] = cand
                new_dists = distance_matrix[:, new_medoids]
                new_assign = np.argmin(new_dists, axis=1)
                new_cost = new_dists[np.arange(N), new_assign].sum()
                if new_cost + 1e-9 < current_cost:
                    medoids = new_medoids
                    current_cost = new_cost
                    improved = True
                    break
            if improved:
                break
        if not improved:
            break
    dists_to_meds = distance_matrix[:, medoids]
    assign = np.argmin(dists_to_meds, axis=1)
    return medoids, assign


# ---------- TSP helper (NN + 2-opt) ----------

def tsp_length_from_distance_matrix(D):
    M = D.shape[0]
    if M <= 1:
        return 0.0
    visited = [0]
    unvis = set(range(1, M))
    cur = 0
    while unvis:
        nxt = min(unvis, key=lambda j: D[cur, j])
        visited.append(nxt)
        unvis.remove(nxt)
        cur = nxt
    improved = True
    max_2opt_iters = 5000
    it_count = 0
    while improved and it_count < max_2opt_iters:
        improved = False
        it_count += 1
        for i in range(1, M - 2):
            for j in range(i + 1, M):
                if j - i == 1:
                    continue
                a, b = visited[i - 1], visited[i]
                c, d = visited[j - 1], visited[j % M]
                if D[a, c] + D[b, d] < D[a, b] + D[c, d] - 1e-9:
                    visited[i:j] = list(reversed(visited[i:j]))
                    improved = True
    length = 0.0
    for i in range(M - 1):
        length += D[visited[i], visited[i + 1]]
    length += D[visited[-1], visited[0]]
    return float(length)


# ---------- Graph helpers (optional) ----------

def read_roads_excel(path):
    if path.lower().endswith(('.xlsx', '.xls')):
        df = pd.read_excel(path)
    else:
        df = pd.read_csv(path)
    origin_col = None
    dest_col = None
    dist_col = None
    time_col = None
    for c in df.columns:
        lc = str(c).lower()
        if origin_col is None and any(k in lc for k in ['origin', 'from', 'u', 'source', 'start', 'node']):
            origin_col = c
        if dest_col is None and any(k in lc for k in ['dest', 'to', 'v', 'target', 'end']):
            dest_col = c
        if dist_col is None and any(k in lc for k in ['dist', 'distance', 'length', 'km']):
            dist_col = c
        if time_col is None and any(k in lc for k in ['time', 'travel', 'duration', 'min']):
            time_col = c
    if origin_col is None or dest_col is None:
        origin_col = df.columns[0]
        dest_col = df.columns[1]
        print("Warning: Could not reliably infer origin/destination columns; using first two columns.", file=sys.stderr)
    edges = []
    for _, row in df.iterrows():
        u = str(row[origin_col])
        v = str(row[dest_col])
        d = None
        t = None
        if dist_col is not None and not pd.isna(row[dist_col]):
            try:
                d = float(row[dist_col])
            except:
                d = None
        if time_col is not None and not pd.isna(row[time_col]):
            try:
                t = float(row[time_col])
            except:
                t = None
        edges.append((u, v, d, t))
    return edges


def build_graph_from_edges(edges, use_distance=True):
    G = nx.DiGraph()
    for u, v, d, t in edges:
        if use_distance and d is not None:
            w = float(d)
        elif t is not None:
            w = float(t)
        else:
            w = 1.0
        try:
            G.add_edge(str(u), str(v), weight=float(w))
        except:
            G.add_edge(str(u), str(v), weight=w)
    return G


def compute_shortest_paths_for_sources(G, sources, weight='weight'):
    sp = {}
    for s in sources:
        s_s = str(s)
        if s_s not in G:
            sp[s_s] = {}
            continue
        lengths = nx.single_source_dijkstra_path_length(G, s_s, weight=weight)
        sp[s_s] = lengths
    return sp


# ---------- Main pipeline ----------

def run_pipeline(customers_path,
                 roads_path=None,
                 depot_node_id=None,
                 depot_coord=None,
                 P_min=2,
                 P_max=12,
                 alpha=1.0,
                 use_graph=False,
                 max_exact_n=2000,
                 out_prefix='results',
                 random_seed=0):
    t0 = time.time()
    np.random.seed(random_seed)

    # Read customers
    if customers_path.lower().endswith(('.xlsx', '.xls')):
        customers = pd.read_excel(customers_path)
    else:
        customers = pd.read_csv(customers_path)
    # infer columns
    cols = list(customers.columns)
    id_col = next((c for c in cols if 'id' in c.lower() or 'customer' in c.lower()), cols[0])
    lat_col = next((c for c in cols if 'lat' in c.lower()), None)
    lon_col = next((c for c in cols if 'lon' in c.lower() or 'lng' in c.lower() or 'long' in c.lower()), None)
    if lat_col is None or lon_col is None:
        raise ValueError('Could not find lat/lon columns in customers file')
    customers[id_col] = customers[id_col].astype(str)
    customers[lat_col] = customers[lat_col].astype(float)
    customers[lon_col] = customers[lon_col].astype(float)

    cust_ids = customers[id_col].tolist()
    coords = list(zip(customers[lat_col].tolist(), customers[lon_col].tolist()))
    N = len(cust_ids)
    print(f"Loaded {N} customers.")

    # Graph handling (optional)
    sp = None
    G = None
    graph_nodes = set()
    if use_graph and roads_path is not None:
        edges = read_roads_excel(roads_path)
        G = build_graph_from_edges(edges, use_distance=True)
        graph_nodes = set(G.nodes())
        missing_nodes = [cid for cid in cust_ids if cid not in graph_nodes]
        if depot_node_id is not None and depot_node_id not in graph_nodes:
            print(f"Warning: depot_node_id '{depot_node_id}' not found in graph nodes.", file=sys.stderr)
        if len(missing_nodes) == 0 and N <= max_exact_n and (depot_node_id is None or depot_node_id in graph_nodes):
            sources = cust_ids.copy()
            if depot_node_id is not None and depot_node_id not in sources:
                sources.append(depot_node_id)
            print("Computing shortest paths from every customer and depot (may take time)...")
            sp = compute_shortest_paths_for_sources(G, sources, weight='weight')
            print("  -> Shortest-path computation done.")
        else:
            print("Skipping full SP computation; will fallback to haversine for missing pairs or use scalable method.")
            sp = None

    # sanitize P
    P_min = max(1, int(P_min))
    P_max = min(int(P_max), N-1) if N>1 else 1
    if P_min > P_max:
        raise ValueError("Invalid P_min/P_max")

    results = []
    best = None

    # Decide strategy: exact PAM if N small, else scalable KMeans approach
    use_exact = (N <= max_exact_n)
    print(f"Using exact PAM? {use_exact} (N={N}, max_exact_n={max_exact_n})")

    for P in range(P_min, P_max+1):
        print(f"Trying P={P} ...")
        if use_exact:
            # Build full distance matrix, prefer SP when present
            D = np.zeros((N, N), dtype=float)
            if sp is not None:
                for i,u in enumerate(cust_ids):
                    for j,v in enumerate(cust_ids):
                        if u==v:
                            D[i,j]=0.0
                        else:
                            D[i,j] = sp.get(u, {}).get(v, np.inf)
                # replace inf by haversine
                mask = ~np.isfinite(D)
                if mask.any():
                    for i in range(N):
                        for j in range(N):
                            if not np.isfinite(D[i,j]):
                                D[i,j] = haversine_km_pair(coords[i], coords[j])
            else:
                for i in range(N):
                    D[i,i] = 0.0
                for i in range(N):
                    for j in range(i+1,N):
                        d = haversine_km_pair(coords[i], coords[j])
                        D[i,j]=d; D[j,i]=d
            medoids, assign = pam_medoids(D, P, random_state=random_seed)
            medoid_indices = medoids
            intra = float(np.sum(np.min(D[:, medoid_indices], axis=1)))
            # compute route cost between depot + medoids using haversine on coords
            if depot_coord is None:
                depot_coord = (np.mean([c[0] for c in coords]), np.mean([c[1] for c in coords]))
            nodes = [depot_coord] + [coords[m] for m in medoid_indices]
            nodes_arr = np.array(nodes)
            Dm = haversine_matrix(nodes_arr, nodes_arr)
            route_cost = tsp_length_from_distance_matrix(Dm)
            obj = float(intra + alpha * route_cost)
        else:
            # scalable: MiniBatchKMeans and medoid = nearest point to centroid (fast)
            mbk = MiniBatchKMeans(n_clusters=P, random_state=random_seed, batch_size=2048, max_iter=200, n_init=1)
            labels = mbk.fit_predict(np.array(coords))
            centroids = mbk.cluster_centers_
            # choose medoid per cluster as point nearest centroid (haversine)
            all_to_centroids = haversine_matrix(np.array(coords), centroids)
            medoid_indices = []
            for k in range(P):
                idxs = np.where(labels==k)[0]
                if len(idxs)==0:
                    medoid_indices.append(int(np.random.choice(N)))
                    continue
                sub = all_to_centroids[idxs, k]
                best_local = idxs[int(np.argmin(sub))]
                medoid_indices.append(int(best_local))
            med_coords = np.array([coords[i] for i in medoid_indices])
            all_to_meds = haversine_matrix(np.array(coords), med_coords)
            assign = np.argmin(all_to_meds, axis=1)
            intra = float(all_to_meds[np.arange(N), assign].sum())
            if depot_coord is None:
                depot_coord = (np.mean([c[0] for c in coords]), np.mean([c[1] for c in coords]))
            nodes = np.vstack([np.array(depot_coord), med_coords])
            Dm = haversine_matrix(nodes, nodes)
            route_cost = tsp_length_from_distance_matrix(Dm)
            obj = float(intra + alpha * route_cost)

        results.append({'P': P, 'obj': obj, 'intra': float(intra), 'route': float(route_cost), 'medoids': medoid_indices})
        print(f"  P={P}: obj={obj:.3f}, intra={intra:.3f}, route={route_cost:.3f}")
        if best is None or obj < best['obj']:
            best = results[-1]

    if best is None:
        raise RuntimeError("No result obtained")

    print("Best P=", best['P'], "objective=", best['obj'])

    final_medoids = best['medoids']
    # final assignments
    med_coords = np.array([coords[i] for i in final_medoids])
    all_to_meds = haversine_matrix(np.array(coords), med_coords)
    final_assign = np.argmin(all_to_meds, axis=1)
    dist_to_med = np.min(all_to_meds, axis=1)

    out_rows = []
    for i in range(N):
        cluster_id = int(final_assign[i])
        medoid_global_idx = int(final_medoids[cluster_id])
        out_rows.append({
            'Customer_ID': cust_ids[i],
            'Cluster_ID': cluster_id,
            'Assigned_Medoid_Index': medoid_global_idx,
            'Assigned_Medoid_ID': cust_ids[medoid_global_idx],
            'Distance_km': float(dist_to_med[i])
        })
    out_df = pd.DataFrame(out_rows)
    out_file = f"{out_prefix}_clusters.xlsx"
    out_df.to_excel(out_file, index=False)
    print("Saved clusters to", out_file)

    resdf = pd.DataFrame([{'P': r['P'], 'obj': r['obj'], 'intra': r['intra'], 'route': r['route']} for r in results])
    res_csv = f"{out_prefix}_obj_vs_P.csv"
    resdf.to_csv(res_csv, index=False)
    print("Saved objective curve to", res_csv)

    map_file = f"{out_prefix}_map.html"
    try:
        m = folium.Map(location=depot_coord, zoom_start=12)
        folium.Marker(location=depot_coord, tooltip='Depot', icon=folium.Icon(color='red')).add_to(m)
        for med_idx in final_medoids:
            lat = customers.loc[med_idx, lat_col]
            lon = customers.loc[med_idx, lon_col]
            folium.CircleMarker(location=[lat, lon], radius=6, color='blue', fill=True).add_to(m)
        m.save(map_file)
        print("Saved map to", map_file)
    except Exception as e:
        print("Warning: failed to create map:", e, file=sys.stderr)
        map_file = None

    elapsed = time.time() - t0
    print(f"Done. Elapsed {elapsed:.1f}s")
    return {'best': best, 'results': results, 'clusters_file': out_file, 'map_file': map_file}


# ---------- CLI ----------

def parse_args_and_run():
    parser = argparse.ArgumentParser(description="Cluster customers and choose medoids using road graph distances.")
    parser.add_argument('--customers', required=True, help='Path to customers csv/xlsx with Customer_ID, Latitude, Longitude')
    parser.add_argument('--roads', required=False, help='Path to roads file (edge list) - xlsx or csv')
    parser.add_argument('--depot-id', required=False, help='Depot node id string present in the roads graph (if available)')
    parser.add_argument('--depot-lat', required=False, type=float, help='Depot latitude (for fallback / map)')
    parser.add_argument('--depot-lon', required=False, type=float, help='Depot longitude (for fallback / map)')
    parser.add_argument('--pmin', type=int, default=2, help='Minimum P to try (default 2)')
    parser.add_argument('--pmax', type=int, default=8, help='Maximum P to try (default 8)')
    parser.add_argument('--alpha', type=float, default=1.0, help='Weight for route length in objective')
    parser.add_argument('--use-graph', type=int, default=0, help='Whether to use road graph (1) or not (0)')
    parser.add_argument('--max-exact-n', type=int, default=2000, help='Max N to attempt full SP/PAM computation (default 2000)')
    parser.add_argument('--out-prefix', type=str, default='results', help='Output file prefix')
    parser.add_argument('--seed', type=int, default=0, help='Random seed (default 0)')
    args = parser.parse_args()

    depot_coord = None
    if args.depot_lat is not None and args.depot_lon is not None:
        depot_coord = (args.depot_lat, args.depot_lon)

    run_pipeline(customers_path=args.customers,
                 roads_path=args.roads,
                 depot_node_id=args.depot_id,
                 depot_coord=depot_coord,
                 P_min=args.pmin,
                 P_max=args.pmax,
                 alpha=args.alpha,
                 use_graph=bool(args.use_graph),
                 max_exact_n=args.max_exact_n,
                 out_prefix=args.out_prefix,
                 random_seed=args.seed)


if __name__ == '__main__':
    parse_args_and_run()
