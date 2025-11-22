import pandas as pd
import numpy as np
import random
import math
import folium
import os

# -------------------------
# PARAMETERS
# -------------------------
INPUT_XLSX = "/Users/alicecin/Documents/Lastmile/data/process/Ho_Chi_Minh_City/customers.xlsx"
OUTPUT_XLSX = "/Users/alicecin/Documents/Lastmile/data/process/Ho_Chi_Minh_City/alns_results.xlsx"
OUTPUT_MAP = "/Users/alicecin/Documents/Lastmile/data/process/Ho_Chi_Minh_City/alns_map.html"

def main():
    # -------------------------
    # ALNS PARAMETERS
    # -------------------------
    P = 4  # number of meeting points
    DEPOT_LAT = 10.776889
    DEPOT_LON = 106.700806
    ALPHA = 1.0
    RANDOM_SEED = 42
    MAX_ITER = 2000
    NO_IMPROVE_LIMIT = 300
    WEIGHT_DECAY = 0.8
    REWARD = 5

    random.seed(RANDOM_SEED)
    np.random.seed(RANDOM_SEED)

    # làm chắc folder tồn tại
    os.makedirs(os.path.dirname(OUTPUT_XLSX), exist_ok=True)
    os.makedirs(os.path.dirname(OUTPUT_MAP), exist_ok=True)

    # -------------------------
    # UTILITIES
    # -------------------------
    def haversine(lat1, lon1, lat2, lon2):
        R = 6371.0
        phi1 = math.radians(lat1)
        phi2 = math.radians(lat2)
        dphi = math.radians(lat2 - lat1)
        dlambda = math.radians(lon2 - lon1)
        a = math.sin(dphi/2.0)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2.0)**2
        return 2*R*math.asin(math.sqrt(a))

    def pairwise_haversine_matrix(lats1, lons1, lats2, lons2):
        a_lat = np.radians(lats1)[:,None]
        a_lon = np.radians(lons1)[:,None]
        b_lat = np.radians(lats2)[None,:]
        b_lon = np.radians(lons2)[None,:]
        dlat = b_lat - a_lat
        dlon = b_lon - a_lon
        R = 6371.0
        A = np.sin(dlat/2.0)**2 + np.cos(a_lat)*np.cos(b_lat)*np.sin(dlon/2.0)**2
        return 2*R*np.arcsin(np.sqrt(A))

    def tsp_length(seq_coords):
        n = len(seq_coords)
        if n <= 1:
            return 0.0
        lats = [c[0] for c in seq_coords]
        lons = [c[1] for c in seq_coords]
        D = pairwise_haversine_matrix(lats, lons, lats, lons)

        visited = [0]
        unvisited = set(range(1,n))
        cur = 0
        while unvisited:
            nxt = min(unvisited, key=lambda j: D[cur,j])
            visited.append(nxt)
            unvisited.remove(nxt)
            cur = nxt

        improved = True
        while improved:
            improved = False
            for i in range(1,n-2):
                for j in range(i+1,n):
                    if j-i == 1:
                        continue
                    a,b = visited[i-1], visited[i]
                    c,d = visited[j-1], visited[j % n]
                    if D[a,c] + D[b,d] < D[a,b] + D[c,d]:
                        visited[i:j] = list(reversed(visited[i:j]))
                        improved = True

        length = sum(D[visited[i], visited[i+1]] for i in range(n-1)) + D[visited[-1], visited[0]]
        return float(length)

    # -------------------------
    # READ DATA
    # -------------------------
    df = pd.read_excel(INPUT_XLSX)
    if not {'Customer_ID','Latitude','Longitude'}.issubset(df.columns):
        raise ValueError("Excel must have columns: Customer_ID, Latitude, Longitude")

    cust_ids = df['Customer_ID'].astype(str).tolist()
    lats = df['Latitude'].astype(float).tolist()
    lons = df['Longitude'].astype(float).tolist()
    N = len(cust_ids)

    depot_coord = (DEPOT_LAT, DEPOT_LON)

    # -------------------------
    # INITIAL SOLUTION
    # -------------------------
    def initial_solution_random(P):
        P_eff = min(P, N)
        if P_eff < 1:
            raise ValueError("P must be >= 1")
        medoids = set(random.sample(range(N), P_eff))
        med_list = list(medoids)
        D = pairwise_haversine_matrix(lats, lons, [lats[m] for m in med_list], [lons[m] for m in med_list])
        nearest_idx = np.argmin(D, axis=1)
        assignment = np.array([med_list[idx] for idx in nearest_idx])
        return medoids, assignment

    # -------------------------
    # EVALUATION
    # -------------------------
    def evaluate(medoids, assignment):
        med_list = list(medoids)
        pos = {m:i for i,m in enumerate(med_list)}
        Dcust = pairwise_haversine_matrix(lats, lons, [lats[m] for m in med_list], [lons[m] for m in med_list])
        cust_pos = np.array([pos[a] for a in assignment])
        cust_distances = Dcust[np.arange(N), cust_pos]
        sum_cust = np.sum(cust_distances)

        coords = [depot_coord] + [(lats[m], lons[m]) for m in med_list]
        tour_len = tsp_length(coords)

        return float(sum_cust + ALPHA*tour_len), float(sum_cust), float(tour_len)

    # -------------------------
    # DESTROY OPERATORS
    # -------------------------
    def destroy_random(medoids, assignment, remove_k):
        if len(medoids) <= 1:
            return medoids
        max_remove = max(1, min(remove_k, len(medoids)-1))
        to_remove = set(random.sample(list(medoids), max_remove))
        return medoids - to_remove

    def destroy_worst(medoids, assignment, remove_k):
        med_list = list(medoids)
        if len(med_list) <= 1:
            return medoids

        pos = {m:i for i,m in enumerate(med_list)}
        D = pairwise_haversine_matrix(lats, lons,
                                      [lats[m] for m in med_list],
                                      [lons[m] for m in med_list])

        sums = []
        for m in med_list:
            idxs = [i for i,a in enumerate(assignment) if a == m]
            s = D[idxs, pos[m]].sum() if idxs else 0.0
            sums.append(s)

        remove_cnt = min(remove_k, len(med_list)-1)
        ranked = sorted(zip(med_list, sums), key=lambda x: x[1], reverse=True)
        to_remove = set([m for m,_ in ranked[:remove_cnt]])
        return medoids - to_remove

    def destroy_cluster(medoids, assignment, remove_k):
        med_list = list(medoids)
        if len(med_list) <= remove_k or len(med_list) <= 1:
            return medoids

        chosen = random.choice(med_list)
        Dmed = pairwise_haversine_matrix(
            [lats[m] for m in med_list], [lons[m] for m in med_list],
            [lats[m] for m in med_list], [lons[m] for m in med_list]
        )

        idx_map = {m:i for i,m in enumerate(med_list)}
        dists = Dmed[idx_map[chosen]]
        sorted_idx = np.argsort(dists)
        to_remove = set()

        for idx in sorted_idx:
            if len(to_remove) >= remove_k:
                break
            to_remove.add(med_list[idx])

        if len(med_list) - len(to_remove) < 1:
            return medoids

        return medoids - to_remove

    # -------------------------
    # REPAIR OPERATORS
    # -------------------------
    def repair_assign_nearest(medoids_partial, assignment_partial, P):
        medoids = set(medoids_partial)
        if len(medoids) == 0:
            medoids.add(random.randrange(N))

        med_list = list(medoids)
        D = pairwise_haversine_matrix(lats, lons,
                                      [lats[m] for m in med_list],
                                      [lons[m] for m in med_list])
        nearest_idx = np.argmin(D, axis=1)
        assignment = np.array([med_list[idx] for idx in nearest_idx])
        return medoids, assignment

    def repair_greedy(medoids_partial, assignment_partial, P):
        medoids = set(medoids_partial)
        remaining = [i for i in range(N) if i not in medoids]

        while len(medoids) < P and remaining:
            cand = random.choice(remaining)
            medoids.add(cand)
            remaining.remove(cand)

        med_list = list(medoids)
        D = pairwise_haversine_matrix(lats, lons,
                                      [lats[m] for m in med_list],
                                      [lons[m] for m in med_list])
        nearest_idx = np.argmin(D, axis=1)
        assignment = np.array([med_list[idx] for idx in nearest_idx])
        return medoids, assignment

    # -------------------------
    # LOCAL SEARCH SWAP
    # -------------------------
    def local_search_swap(medoids, assignment):
        best_meds = set(medoids)
        best_assign = assignment.copy()
        best_val,_,_ = evaluate(best_meds, best_assign)
        improved = True

        while improved:
            improved = False
            for m in list(best_meds):
                for cand in range(N):
                    if cand in best_meds:
                        continue
                    new_meds = set(best_meds)
                    new_meds.remove(m)
                    new_meds.add(cand)
                    _, new_assign = repair_assign_nearest(new_meds, None, P)
                    val,_,_ = evaluate(new_meds, new_assign)
                    if val < best_val:
                        best_val = val
                        best_meds = set(new_meds)
                        best_assign = new_assign.copy()
                        improved = True
                        break
                if improved:
                    break
        return best_meds, best_assign

    # -------------------------
    # ALNS MAIN LOOP
    # -------------------------
    destroy_ops = [("random", destroy_random),
                   ("worst", destroy_worst),
                   ("cluster", destroy_cluster)]
    repair_ops = [("greedy", repair_greedy),
                  ("assign_nearest", repair_assign_nearest)]

    d_weights = {name:1.0 for name,_ in destroy_ops}
    r_weights = {name:1.0 for name,_ in repair_ops}
    d_scores = {name:0.0 for name,_ in destroy_ops}
    r_scores = {name:0.0 for name,_ in repair_ops}
    d_uses = {name:0 for name,_ in destroy_ops}
    r_uses = {name:0 for name,_ in repair_ops}

    curr_medoids, curr_assign = initial_solution_random(P)
    curr_val,_,_ = evaluate(curr_medoids, curr_assign)
    best_medoids, best_assign = set(curr_medoids), curr_assign.copy()
    best_val = curr_val
    no_improve = 0

    print(f"Initial objective: {curr_val:.4f}")

    iteration = 0
    while iteration < MAX_ITER and no_improve < NO_IMPROVE_LIMIT:
        iteration += 1

        d_probs = np.array([d_weights[n] for n,_ in destroy_ops])
        d_probs /= d_probs.sum()
        r_probs = np.array([r_weights[n] for n,_ in repair_ops])
        r_probs /= r_probs.sum()

        d_choice = np.random.choice([name for name,_ in destroy_ops], p=d_probs)
        r_choice = np.random.choice([name for name,_ in repair_ops], p=r_probs)

        remove_k = max(1, int(0.2 * len(curr_medoids)))

        d_func = dict(destroy_ops)[d_choice]
        r_func = dict(repair_ops)[r_choice]

        med_after = d_func(curr_medoids, curr_assign, remove_k)
        new_meds, new_assign = r_func(med_after, curr_assign, P)
        new_val,_,_ = evaluate(new_meds, new_assign)

        if new_val < curr_val or random.random() < 0.01:
            curr_medoids = set(new_meds)
            curr_assign = new_assign
            curr_val = new_val

            if new_val < best_val:
                best_val = new_val
                best_medoids = set(new_meds)
                best_assign = new_assign.copy()
                d_scores[d_choice] += REWARD
                r_scores[r_choice] += REWARD
                no_improve = 0
            else:
                d_scores[d_choice] += 1
                r_scores[r_choice] += 1
                no_improve += 1
        else:
            no_improve += 1

        d_uses[d_choice] += 1
        r_uses[r_choice] += 1

        if iteration % 200 == 0:
            ls_m, ls_a = local_search_swap(curr_medoids, curr_assign)
            ls_val,_,_ = evaluate(ls_m, ls_a)
            if ls_val < curr_val:
                curr_medoids = set(ls_m)
                curr_assign = ls_a
                curr_val = ls_val
                if ls_val < best_val:
                    best_val = ls_val
                    best_medoids = set(ls_m)
                    best_assign = ls_a.copy()
                    print(f"Iter {iteration}: local-search improved global best → {best_val:.4f}")

        if iteration % 100 == 0:
            for name in d_weights:
                if d_uses[name] > 0:
                    d_weights[name] = WEIGHT_DECAY*d_weights[name] + d_scores[name]/d_uses[name]
                    d_scores[name] = 0
                    d_uses[name] = 0

            for name in r_weights:
                if r_uses[name] > 0:
                    r_weights[name] = WEIGHT_DECAY*r_weights[name] + r_scores[name]/r_uses[name]
                    r_scores[name] = 0
                    r_uses[name] = 0

        if iteration % 200 == 0:
            print(f"Iter {iteration}: curr {curr_val:.3f}  best {best_val:.3f}  (no improve = {no_improve})")

    print("ALNS finished. Best objective:", best_val)

    # -------------------------
    # SAVE RESULTS
    # -------------------------
    best_med_list = list(best_medoids)
    Dmed = pairwise_haversine_matrix(
        lats, lons,
        [lats[m] for m in best_med_list],
        [lons[m] for m in best_med_list]
    )

    nearest_idx = np.argmin(Dmed, axis=1)
    final_assign = np.array([best_med_list[idx] for idx in nearest_idx])
    final_distances = Dmed[np.arange(N), nearest_idx]

    out_rows = []
    for i in range(N):
        out_rows.append({
            "Customer_ID": cust_ids[i],
            "Assigned_MP": cust_ids[int(final_assign[i])],
            "Distance_km": final_distances[i]
        })

    out_df = pd.DataFrame(out_rows)
    out_df.to_excel(OUTPUT_XLSX, index=False)
    print("Saved results to", OUTPUT_XLSX)

    # MAP
    m = folium.Map(location=[DEPOT_LAT, DEPOT_LON], zoom_start=12)
    folium.Marker([DEPOT_LAT, DEPOT_LON], tooltip="Depot", icon=folium.Icon(color='red')).add_to(m)

    for m_id in best_med_list:
        folium.CircleMarker([lats[m_id], lons[m_id]], radius=6, color='blue', fill=True).add_to(m)

    m.save(OUTPUT_MAP)
    print("Saved map to", OUTPUT_MAP)

if __name__ == "__main__":
    main()
