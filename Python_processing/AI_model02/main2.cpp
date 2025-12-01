#include <bits/stdc++.h>
using namespace std;

// ============================================================
// 0. Hằng số & Utils
// ============================================================

const double PI = 3.14159265358979323846;
const double AVERAGE_SPEED_KMPH = 30.0;   // dùng để ước lượng travel time

// Haversine distance (km) giữa 2 điểm lat/lon (độ)
double haversine_km(double lat1, double lon1, double lat2, double lon2) {
    const double R = 6371.0;
    double rlat1 = lat1 * PI / 180.0;
    double rlon1 = lon1 * PI / 180.0;
    double rlat2 = lat2 * PI / 180.0;
    double rlon2 = lon2 * PI / 180.0;
    double dlat = rlat2 - rlat1;
    double dlon = rlon2 - rlon1;
    double a = sin(dlat / 2.0) * sin(dlat / 2.0) +
               cos(rlat1) * cos(rlat2) * sin(dlon / 2.0) * sin(dlon / 2.0);
    double c = 2.0 * atan2(sqrt(a), sqrt(1.0 - a));
    return R * c;
}

string trim(const string &s) {
    size_t l = 0, r = s.size();
    while (l < r && isspace((unsigned char)s[l])) l++;
    while (r > l && isspace((unsigned char)s[r - 1])) r--;
    return s.substr(l, r - l);
}

// parse double an toàn bằng strtod
double parse_double(const string &raw, double default_val = 0.0) {
    string s = trim(raw);
    if (s.empty()) return default_val;

    string t = s;
    for (char &ch : t) {
        if (ch == ',') ch = '.';
    }

    const char *cstr = t.c_str();
    char *endptr = nullptr;
    errno = 0;
    double val = strtod(cstr, &endptr);

    if (endptr == cstr || errno == ERANGE) {
        cerr << "[WARN] Cannot parse double from '" << s
             << "' -> use default " << default_val << "\n";
        return default_val;
    }
    return val;
}

// split CSV đơn giản
vector<string> split_csv_line(const string &line) {
    vector<string> result;
    string cur;
    bool in_quote = false;
    for (char c : line) {
        if (c == '"') {
            in_quote = !in_quote;
        } else if (c == ',' && !in_quote) {
            result.push_back(cur);
            cur.clear();
        } else {
            cur.push_back(c);
        }
    }
    result.push_back(cur);
    return result;
}

// ============================================================
// 1. Cấu trúc dữ liệu
// ============================================================

struct Customer {
    string id;
    double lat = 0.0, lon = 0.0;
    double weight = 0.0, volume = 0.0;
    double service_time = 0.0;

    string nearest_depot_id;
};

struct Depot {
    string id;
    double lat = 0.0, lon = 0.0;
};

struct Vehicle {
    string id;
    string start_depot_id;
    double cap_weight = 0.0, cap_volume = 0.0;
    double fixed_cost = 0.0;
    double variable_cost = 0.0;
    double max_distance = 0.0;
    double max_hours = 0.0;
};

struct RoadArc {
    string origin_id;   // Depot_ID
    string dest_id;     // Customer_ID
    double distance_km = 0.0;
    double travel_time_min = 0.0;
};

struct Zone {
    string depot_id;
    vector<int> members;  // indices vào vector raw_customers
    int medoid_idx = -1;  // index vào raw_customers

    double total_weight = 0.0;
    double total_volume = 0.0;
    double total_service_time = 0.0;
};

struct Route {
    string depot_id;
    string vehicle_id;
    vector<int> cust_idx;  // indices vào vector inter_customers
};

struct Solution {
    vector<Route> routes;
    double total_distance = 0.0;
    double total_cost = 0.0;
};

struct CostSummary {
    double total_distance = 0.0;
    double total_fixed_cost = 0.0;
    double total_variable_cost = 0.0;
    double total_cost = 0.0;
};

// ============================================================
// 2. Load CSV
// ============================================================

vector<Customer> load_customers(const string &path) {
    ifstream fin(path);
    if (!fin) {
        cerr << "Cannot open " << path << endl;
        exit(1);
    }
    string line;
    getline(fin, line); // header
    vector<Customer> customers;
    while (getline(fin, line)) {
        if (line.empty()) continue;
        auto cols = split_csv_line(line);
        // Customer_ID, Latitude, Longitude, City,
        // Order_Weight, Order_Volume,
        // Time_Window_Start, Time_Window_End,
        // Service_Time, Priority_Level, Delivery_Type, Return_Flag
        if (cols.size() < 9) continue;

        Customer c;
        c.id           = trim(cols[0]);
        c.lat          = parse_double(cols[1]);
        c.lon          = parse_double(cols[2]);
        c.weight       = parse_double(cols[4]);
        c.volume       = parse_double(cols[5]);
        c.service_time = parse_double(cols[8]);
        customers.push_back(c);
    }
    return customers;
}

vector<Depot> load_depots(const string &path) {
    ifstream fin(path);
    if (!fin) {
        cerr << "Cannot open " << path << endl;
        exit(1);
    }
    string line;
    getline(fin, line); // header
    vector<Depot> depots;
    while (getline(fin, line)) {
        if (line.empty()) continue;
        auto cols = split_csv_line(line);
        // Depot_ID, City, Latitude, Longitude, Capacity_Storage, Operating_Hours
        if (cols.size() < 4) continue;
        Depot d;
        d.id  = trim(cols[0]);
        d.lat = parse_double(cols[2]);
        d.lon = parse_double(cols[3]);
        depots.push_back(d);
    }
    return depots;
}

vector<Vehicle> load_vehicles(const string &path) {
    ifstream fin(path);
    if (!fin) {
        cerr << "Cannot open " << path << endl;
        exit(1);
    }
    string line;
    getline(fin, line); // header
    vector<Vehicle> vehicles;
    while (getline(fin, line)) {
        if (line.empty()) continue;
        auto cols = split_csv_line(line);
        // Vehicle_ID, Vehicle_Type,
        // Capacity_Weight, Capacity_Volume,
        // Fixed_Cost, Variable_Cost,
        // Max_Distance, Max_Working_Hours,
        // Start_Depot_ID, End_Depot_ID
        if (cols.size() < 9) continue;
        Vehicle v;
        v.id             = trim(cols[0]);
        v.cap_weight     = parse_double(cols[2]);
        v.cap_volume     = parse_double(cols[3]);
        v.fixed_cost     = parse_double(cols[4]);
        v.variable_cost  = parse_double(cols[5]);
        v.max_distance   = parse_double(cols[6]);
        v.max_hours      = parse_double(cols[7]);
        v.start_depot_id = trim(cols[8]);
        vehicles.push_back(v);
    }
    return vehicles;
}

vector<RoadArc> load_roads(const string &path) {
    ifstream fin(path);
    if (!fin) {
        cerr << "Cannot open " << path << endl;
        exit(1);
    }
    string line;
    getline(fin, line); // header
    vector<RoadArc> arcs;
    while (getline(fin, line)) {
        if (line.empty()) continue;
        auto cols = split_csv_line(line);
        // Origin_Node_ID, Destination_Node_ID, Distance_km, Travel_Time_min, ...
        if (cols.size() < 4) continue;
        RoadArc r;
        r.origin_id       = trim(cols[0]);
        r.dest_id         = trim(cols[1]);
        r.distance_km     = parse_double(cols[2]);
        r.travel_time_min = parse_double(cols[3]);
        arcs.push_back(r);
    }
    return arcs;
}

// ============================================================
// 3. Helper: compute cost cho cả solution
// ============================================================

CostSummary compute_cost(
    const Solution &sol,
    const vector<Customer> &customers,
    const vector<Depot> &depots,
    const vector<Vehicle> &vehicles
) {
    CostSummary cs;
    if (sol.routes.empty()) return cs;

    unordered_map<string, int> idx_depot;
    for (int i = 0; i < (int)depots.size(); ++i) {
        idx_depot[depots[i].id] = i;
    }
    unordered_map<string, const Vehicle*> veh_by_id;
    for (const auto &v : vehicles) {
        veh_by_id[v.id] = &v;
    }

    unordered_map<string, double> dist_by_vehicle;
    unordered_set<string> used_vehicles;

    for (const auto &r : sol.routes) {
        if (r.cust_idx.empty()) continue;
        auto it_dep = idx_depot.find(r.depot_id);
        if (it_dep == idx_depot.end()) continue;
        const auto &dep = depots[it_dep->second];

        double dist = 0.0;
        const auto &c0 = customers[r.cust_idx[0]];
        dist += haversine_km(dep.lat, dep.lon, c0.lat, c0.lon);
        for (int i = 0; i + 1 < (int)r.cust_idx.size(); ++i) {
            const auto &ca = customers[r.cust_idx[i]];
            const auto &cb = customers[r.cust_idx[i + 1]];
            dist += haversine_km(ca.lat, ca.lon, cb.lat, cb.lon);
        }
        const auto &cl = customers[r.cust_idx.back()];
        dist += haversine_km(cl.lat, cl.lon, dep.lat, dep.lon);

        cs.total_distance += dist;
        dist_by_vehicle[r.vehicle_id] += dist;
        used_vehicles.insert(r.vehicle_id);
    }

    for (const auto &vid : used_vehicles) {
        auto it = veh_by_id.find(vid);
        if (it == veh_by_id.end()) continue;
        const Vehicle *v = it->second;
        cs.total_fixed_cost += v->fixed_cost;
        auto itd = dist_by_vehicle.find(vid);
        double vd = (itd != dist_by_vehicle.end()) ? itd->second : 0.0;
        cs.total_variable_cost += vd * v->variable_cost;
    }

    cs.total_cost = cs.total_fixed_cost + cs.total_variable_cost;
    return cs;
}

// ============================================================
// 4. Assign nearest depot cho customers
// ============================================================

void assign_nearest_depot(
    vector<Customer> &customers,
    const vector<RoadArc> &roads
) {
    unordered_map<string, int> idx_cust;
    for (int i = 0; i < (int)customers.size(); ++i) {
        idx_cust[customers[i].id] = i;
    }

    unordered_map<string, pair<string, double>> best; // Customer_ID -> (best_depot, best_time)

    for (const auto &arc : roads) {
        auto it = idx_cust.find(arc.dest_id);
        if (it == idx_cust.end()) continue;
        auto itb = best.find(arc.dest_id);
        if (itb == best.end() || arc.travel_time_min < itb->second.second) {
            best[arc.dest_id] = {arc.origin_id, arc.travel_time_min};
        }
    }

    for (auto &c : customers) {
        auto itb = best.find(c.id);
        if (itb != best.end()) {
            c.nearest_depot_id = itb->second.first;
        } else {
            c.nearest_depot_id = "";
        }
    }
}

// ============================================================
// 5. Zone utils: stats, medoid, split, merge
// ============================================================

void compute_zone_stats(Zone &z, const vector<Customer> &customers) {
    z.total_weight = 0.0;
    z.total_volume = 0.0;
    z.total_service_time = 0.0;
    for (int idx : z.members) {
        z.total_weight += customers[idx].weight;
        z.total_volume += customers[idx].volume;
        z.total_service_time += customers[idx].service_time;
    }
}

int compute_medoid(const Zone &z, const vector<Customer> &customers) {
    if (z.members.empty()) return -1;
    double best_sum = 1e18;
    int best_idx = z.members[0];
    for (int i : z.members) {
        double sumd = 0.0;
        for (int j : z.members) {
            sumd += haversine_km(customers[i].lat, customers[i].lon,
                                 customers[j].lat, customers[j].lon);
        }
        if (sumd < best_sum) {
            best_sum = sumd;
            best_idx = i;
        }
    }
    return best_idx;
}

void update_zone(Zone &z, const vector<Customer> &customers) {
    compute_zone_stats(z, customers);
    z.medoid_idx = compute_medoid(z, customers);
}

pair<Zone, Zone> split_zone(
    const Zone &z,
    const vector<Customer> &customers,
    mt19937 &rng
) {
    Zone z1, z2;
    z1.depot_id = z.depot_id;
    z2.depot_id = z.depot_id;

    if (z.members.size() <= 1) {
        z1 = z;
        z2.members.clear();
        update_zone(z1, customers);
        update_zone(z2, customers);
        return {z1, z2};
    }

    int seed1 = z.members[rng() % z.members.size()];
    int seed2 = z.members[rng() % z.members.size()];
    if (seed1 == seed2 && z.members.size() >= 2) {
        seed2 = z.members[(rng() + 1) % z.members.size()];
    }

    vector<int> A, B;
    for (int idx : z.members) {
        double d1 = haversine_km(customers[idx].lat, customers[idx].lon,
                                 customers[seed1].lat, customers[seed1].lon);
        double d2 = haversine_km(customers[idx].lat, customers[idx].lon,
                                 customers[seed2].lat, customers[seed2].lon);
        if (d1 < d2) A.push_back(idx);
        else B.push_back(idx);
    }

    if (A.empty()) {
        A.push_back(B.back());
        B.pop_back();
    } else if (B.empty()) {
        B.push_back(A.back());
        A.pop_back();
    }

    z1.members = A;
    z2.members = B;
    update_zone(z1, customers);
    update_zone(z2, customers);
    return {z1, z2};
}

// ============================================================
// 6. KMeans zoning per depot trên customers gốc
// ============================================================

int estimate_k(
    const vector<int> &cust_idx,
    const vector<Customer> &customers,
    const vector<Vehicle> &vehicles_for_depot,
    double target_util = 0.8,
    int min_clusters = 3,
    int max_clusters = 40
) {
    if (cust_idx.size() <= 1) return 1;
    double total_w = 0.0;
    for (int i : cust_idx) total_w += customers[i].weight;
    if (vehicles_for_depot.empty()) {
        return max(1, min_clusters);
    }
    double avg_cap = 0.0;
    for (auto &v : vehicles_for_depot) avg_cap += v.cap_weight;
    avg_cap /= vehicles_for_depot.size();
    if (avg_cap <= 0) avg_cap = total_w / max((int)vehicles_for_depot.size(), 1);

    double base_loads = total_w / (avg_cap * target_util + 1e-6);
    int k0 = (int)ceil(base_loads);
    k0 = max(min_clusters, min(max_clusters, k0));
    k0 = min(k0, (int)cust_idx.size());
    return max(1, k0);
}

// KMeans đơn giản
vector<int> kmeans_assign(
    const vector<vector<double>> &X,
    int K,
    mt19937 &rng,
    int max_iters = 50
) {
    int N = (int)X.size();
    int D = (int)X[0].size();
    vector<int> labels(N, 0);

    vector<int> idx(N);
    iota(idx.begin(), idx.end(), 0);
    shuffle(idx.begin(), idx.end(), rng);

    vector<vector<double>> centers(K, vector<double>(D, 0.0));
    for (int k = 0; k < K; ++k) {
        centers[k] = X[idx[k]];
    }

    for (int it = 0; it < max_iters; ++it) {
        bool changed = false;

        for (int i = 0; i < N; ++i) {
            double best_dist = numeric_limits<double>::infinity();
            int best_k = 0;
            for (int k = 0; k < K; ++k) {
                double d2 = 0.0;
                for (int j = 0; j < D; ++j) {
                    double diff = X[i][j] - centers[k][j];
                    d2 += diff * diff;
                }
                if (d2 < best_dist) {
                    best_dist = d2;
                    best_k = k;
                }
            }
            if (labels[i] != best_k) {
                labels[i] = best_k;
                changed = true;
            }
        }

        vector<vector<double>> new_centers(K, vector<double>(D, 0.0));
        vector<int> counts(K, 0);
        for (int i = 0; i < N; ++i) {
            int k = labels[i];
            counts[k]++;
            for (int j = 0; j < D; ++j) {
                new_centers[k][j] += X[i][j];
            }
        }
        for (int k = 0; k < K; ++k) {
            if (counts[k] > 0) {
                for (int j = 0; j < D; ++j) new_centers[k][j] /= counts[k];
            } else {
                new_centers[k] = X[rng() % N];
            }
        }
        centers.swap(new_centers);

        if (!changed) break;
    }
    return labels;
}

vector<Zone> zoning_kmeans(
    vector<Customer> &customers,
    const vector<Depot> &depots,
    const vector<Vehicle> &vehicles,
    double target_utilization = 0.8,
    int min_clusters = 3,
    int max_clusters = 40,
    unsigned seed = 42
) {
    cout << "\n================ KMEANS ZONING (start) ================\n";
    mt19937 rng(seed);

    unordered_map<string, int> idx_depot;
    for (int i = 0; i < (int)depots.size(); ++i) {
        idx_depot[depots[i].id] = i;
    }

    unordered_map<string, vector<int>> cust_by_depot;
    for (int i = 0; i < (int)customers.size(); ++i) {
        if (customers[i].nearest_depot_id.empty()) continue;
        cust_by_depot[customers[i].nearest_depot_id].push_back(i);
    }

    unordered_map<string, vector<Vehicle>> veh_by_depot;
    for (auto &v : vehicles) {
        veh_by_depot[v.start_depot_id].push_back(v);
    }

    vector<Zone> zones_all;

    for (auto &kv : cust_by_depot) {
        const string &depot_id = kv.first;
        auto &cust_idx = kv.second;
        auto itveh = veh_by_depot.find(depot_id);

        cout << "[KMeans] Depot " << depot_id << ": "
             << cust_idx.size() << " customers, "
             << (itveh != veh_by_depot.end() ? itveh->second.size() : 0)
             << " vehicles\n";

        if (cust_idx.empty()) continue;

        auto veh_dep = (itveh != veh_by_depot.end()) ? itveh->second : vector<Vehicle>{};
        int K = estimate_k(cust_idx, customers, veh_dep, target_utilization, min_clusters, max_clusters);
        cout << "[KMeans] Depot " << depot_id << ": estimated K = " << K << "\n";

        int N = (int)cust_idx.size();
        int D = 4;
        vector<vector<double>> rawX(N, vector<double>(D, 0.0));
        for (int i = 0; i < N; ++i) {
            const auto &c = customers[cust_idx[i]];
            rawX[i][0] = c.lat;
            rawX[i][1] = c.lon;
            rawX[i][2] = c.weight;
            rawX[i][3] = c.volume;
        }

        vector<double> mean(D, 0.0), stddev(D, 0.0);
        for (int j = 0; j < D; ++j) {
            for (int i = 0; i < N; ++i) mean[j] += rawX[i][j];
            mean[j] /= N;
            for (int i = 0; i < N; ++i) {
                double diff = rawX[i][j] - mean[j];
                stddev[j] += diff * diff;
            }
            stddev[j] = sqrt(stddev[j] / max(1, N - 1));
            if (stddev[j] < 1e-9) stddev[j] = 1.0;
        }

        vector<double> weight(D);
        weight[0] = 3.0;
        weight[1] = 3.0;
        weight[2] = 0.7;
        weight[3] = 0.7;

        vector<vector<double>> X(N, vector<double>(D, 0.0));
        for (int i = 0; i < N; ++i) {
            for (int j = 0; j < D; ++j) {
                X[i][j] = ((rawX[i][j] - mean[j]) / stddev[j]) * weight[j];
            }
        }

        vector<int> labels;
        if (K == 1) {
            labels.assign(N, 0);
        } else {
            labels = kmeans_assign(X, K, rng);
        }

        unordered_map<int, vector<int>> members;
        for (int i = 0; i < N; ++i) {
            members[labels[i]].push_back(cust_idx[i]);
        }

        for (auto &zpair : members) {
            auto &members_idx = zpair.second;
            if (members_idx.empty()) continue;
            Zone z;
            z.depot_id = depot_id;
            z.members = members_idx;
            update_zone(z, customers);
            zones_all.push_back(z);
        }

        cout << "[KMeans] Depot " << depot_id
             << ": created " << members.size() << " zones\n\n";
    }

    cout << "[KMeans] Total zones = " << zones_all.size() << "\n";
    return zones_all;
}

// ============================================================
// 7. Đảm bảo Zone feasible theo capacity (split nếu cần)
// ============================================================

vector<Zone> ensure_zone_feasibility(
    const vector<Zone> &zones_init,
    const vector<Customer> &customers,
    const vector<Vehicle> &vehicles,
    double util = 0.9,
    unsigned seed = 2025
) {
    mt19937 rng(seed);

    unordered_map<string, vector<Vehicle>> veh_by_depot;
    for (auto v : vehicles) {
        veh_by_depot[v.start_depot_id].push_back(v);
    }

    vector<Zone> result;

    for (const Zone &z0 : zones_init) {
        auto itv = veh_by_depot.find(z0.depot_id);
        if (itv == veh_by_depot.end() || itv->second.empty()) {
            Zone z = z0;
            update_zone(z, customers);
            result.push_back(z);
            continue;
        }

        double max_cap_w = 0.0, max_cap_v = 0.0;
        for (auto &v : itv->second) {
            if (v.cap_weight > max_cap_w) max_cap_w = v.cap_weight;
            if (v.cap_volume > max_cap_v) max_cap_v = v.cap_volume;
        }

        vector<Zone> stack;
        Zone zstart = z0;
        update_zone(zstart, customers);
        stack.push_back(zstart);

        int guard = 0;
        while (!stack.empty() && guard < 10000) {
            guard++;
            Zone z = stack.back();
            stack.pop_back();

            bool overweight = (max_cap_w > 0.0 && z.total_weight > max_cap_w * util);
            bool overvolume = (max_cap_v > 0.0 && z.total_volume > max_cap_v * util);

            if ((overweight || overvolume) && z.members.size() > 1) {
                auto [z1, z2] = split_zone(z, customers, rng);
                if (!z1.members.empty()) stack.push_back(z1);
                if (!z2.members.empty()) stack.push_back(z2);
            } else {
                result.push_back(z);
            }
        }
    }

    cout << "[Zone] After feasibility split, total zones = " << result.size() << "\n";
    return result;
}

// ============================================================
// 8. Build intermediate customers từ Zone (medoid)
// ============================================================

vector<Customer> build_intermediate_customers_from_zones(
    const vector<Zone> &zones,
    const vector<Customer> &orig_customers
) {
    vector<Customer> inter;
    inter.reserve(zones.size());
    for (size_t i = 0; i < zones.size(); ++i) {
        const auto &z = zones[i];
        const auto &center = orig_customers[z.medoid_idx];

        Customer c;
        // id = "Z<index>_<CustomerID>" để tránh trùng
        c.id = "Z" + to_string(i) + "_" + center.id;
        c.lat = center.lat;
        c.lon = center.lon;
        c.weight = z.total_weight;
        c.volume = z.total_volume;
        c.service_time = z.total_service_time;
        c.nearest_depot_id = z.depot_id;

        inter.push_back(c);
    }
    return inter;
}

// ============================================================
// 9. Build initial solution trên intermediate customers
// ============================================================

double compute_route_distance_for_trip(
    const vector<int> &trip,
    const vector<Customer> &customers,
    const Depot &dep
) {
    if (trip.empty()) return 0.0;
    double dist = 0.0;
    const auto &c0 = customers[trip[0]];
    dist += haversine_km(dep.lat, dep.lon, c0.lat, c0.lon);
    for (int i = 0; i + 1 < (int)trip.size(); ++i) {
        const auto &ca = customers[trip[i]];
        const auto &cb = customers[trip[i + 1]];
        dist += haversine_km(ca.lat, ca.lon, cb.lat, cb.lon);
    }
    const auto &cl = customers[trip.back()];
    dist += haversine_km(cl.lat, cl.lon, dep.lat, dep.lon);
    return dist;
}

Solution build_initial_solution_stage1(
    vector<Customer> &inter_customers,
    const vector<Depot> &depots,
    const vector<Vehicle> &vehicles,
    double capacity_util = 0.9
) {
    cout << "\n[MAIN] Step 4: Building initial solution on intermediate customers...\n";

    // Map depot_id -> index trong depots
    unordered_map<string, int> idx_depot;
    for (int i = 0; i < (int)depots.size(); ++i) {
        idx_depot[depots[i].id] = i;
    }

    // Map vehicle_id -> Vehicle
    unordered_map<string, Vehicle> veh_by_id;
    for (auto &v : vehicles) {
        veh_by_id[v.id] = v;
    }

    // Gom intermediate customers theo depot
    unordered_map<string, vector<int>> cust_by_depot;
    for (int i = 0; i < (int)inter_customers.size(); ++i) {
        if (inter_customers[i].nearest_depot_id.empty()) continue;
        cust_by_depot[inter_customers[i].nearest_depot_id].push_back(i);
    }

    // Gom vehicle_id theo depot
    unordered_map<string, vector<string>> veh_ids_by_depot;
    for (auto &v : vehicles) {
        veh_ids_by_depot[v.start_depot_id].push_back(v.id);
    }

    vector<Route> routes;
    double total_dist_est = 0.0;

    // Xử lý từng depot
    for (auto &kv : cust_by_depot) {
        const string &depot_id = kv.first;
        auto &cust_idx = kv.second;

        auto it_dep = idx_depot.find(depot_id);
        if (it_dep == idx_depot.end()) {
            cerr << "[MAIN] WARNING: depot " << depot_id << " not found in depots list\n";
            continue;
        }
        const auto &dep = depots[it_dep->second];

        auto itveh = veh_ids_by_depot.find(depot_id);
        if (itveh == veh_ids_by_depot.end() || itveh->second.empty()) {
            cerr << "[MAIN] WARNING: no vehicle for depot " << depot_id << "\n";
            continue;
        }
        auto &veh_ids = itveh->second;

        // Đếm số trip mỗi xe đã chạy tại depot này
        unordered_map<string,int> veh_usage;
        for (auto &vid : veh_ids) {
            veh_usage[vid] = 0;
        }

        // Mỗi intermediate customer = 1 zone cần 1 trip
        vector<int> unassigned = cust_idx;

        while (!unassigned.empty()) {
            int z_idx = unassigned.front();
            unassigned.erase(unassigned.begin());

            const auto &zc = inter_customers[z_idx];

            double demand_w = zc.weight;
            double demand_v = zc.volume;

            // Trip chỉ đi 1 zone
            vector<int> tmp_trip = {z_idx};
            double dist_single = compute_route_distance_for_trip(tmp_trip, inter_customers, dep);
            double travel_min_single =
                (AVERAGE_SPEED_KMPH > 0.0)
                    ? (dist_single / AVERAGE_SPEED_KMPH * 60.0)
                    : 0.0;
            double svc_single = zc.service_time;
            double time_single_min = travel_min_single + svc_single;

            // Chọn vehicle khả thi & ít được dùng nhất
            string chosen_vid;
            int best_used = INT_MAX;
            double best_cap = -1.0;

            for (const auto &vid_candidate : veh_ids) {
                auto itv = veh_by_id.find(vid_candidate);
                if (itv == veh_by_id.end()) continue;
                const auto &vrow = itv->second;

                bool feasible_for_v = true;
                if (vrow.cap_weight > 0.0 &&
                    demand_w > vrow.cap_weight * capacity_util) {
                    feasible_for_v = false;
                }
                if (vrow.cap_volume > 0.0 &&
                    demand_v > vrow.cap_volume * capacity_util) {
                    feasible_for_v = false;
                }
                if (vrow.max_distance > 0.0 &&
                    dist_single > vrow.max_distance) {
                    feasible_for_v = false;
                }
                if (vrow.max_hours > 0.0 &&
                    time_single_min > vrow.max_hours * 60.0) {
                    feasible_for_v = false;
                }

                if (!feasible_for_v) continue;

                int used = veh_usage[vid_candidate];
                double cap = vrow.cap_weight;

                // Ưu tiên: used ít hơn, nếu bằng thì cap lớn hơn
                if (used < best_used ||
                    (used == best_used && cap > best_cap)) {
                    best_used = used;
                    best_cap = cap;
                    chosen_vid = vid_candidate;
                }
            }

            if (chosen_vid.empty()) {
                cerr << "[MAIN] ERROR: no feasible vehicle for zone "
                     << zc.id << " at depot " << depot_id << "\n";
                continue;
            }

            // 1 trip = 1 zone
            Route r;
            r.depot_id = depot_id;
            r.vehicle_id = chosen_vid;
            r.cust_idx.push_back(z_idx);

            routes.push_back(r);
            total_dist_est += dist_single;
            veh_usage[chosen_vid]++;
        }
    }

    Solution sol;
    sol.routes = routes;
    sol.total_distance = total_dist_est;
    sol.total_cost = 0.0;

    cout << "[MAIN] Initial solution (intermediate, 1 zone/trip): "
         << sol.routes.size() << " routes\n";
    cout << "[MAIN] Initial total distance (approx, km): "
         << total_dist_est << "\n";

    return sol;
}


// ============================================================
// 10. ALNS Solver (destroy/repair, objective = cost) trên customer trung gian
// ============================================================

class ALNSSolver {
public:
    vector<Customer> &customers;
    const vector<Depot> &depots;
    const vector<Vehicle> &vehicles;
    unordered_map<string, int> idx_depot;
    unordered_map<string, Vehicle> veh_by_id;

    double capacity_util;
    mt19937 rng;

    ALNSSolver(
        vector<Customer> &cust,
        const vector<Depot> &dep,
        const vector<Vehicle> &veh,
        double cap_util = 0.98,
        unsigned seed = 42
    ) : customers(cust),
        depots(dep),
        vehicles(veh),
        capacity_util(cap_util),
        rng(seed)
    {
        for (int i = 0; i < (int)depots.size(); ++i) {
            idx_depot[depots[i].id] = i;
        }
        for (auto &v : vehicles) {
            veh_by_id[v.id] = v;
        }
    }

    double route_distance(const Route &r) const {
        if (r.cust_idx.empty()) return 0.0;
        int dep_i = idx_depot.at(r.depot_id);
        const auto &dep = depots[dep_i];

        double dist = 0.0;
        const auto &c0 = customers[r.cust_idx[0]];
        dist += haversine_km(dep.lat, dep.lon, c0.lat, c0.lon);
        for (int i = 0; i + 1 < (int)r.cust_idx.size(); ++i) {
            const auto &ca = customers[r.cust_idx[i]];
            const auto &cb = customers[r.cust_idx[i + 1]];
            dist += haversine_km(ca.lat, ca.lon, cb.lat, cb.lon);
        }
        const auto &cl = customers[r.cust_idx.back()];
        dist += haversine_km(cl.lat, cl.lon, dep.lat, dep.lon);
        return dist;
    }

    pair<double, double> route_load(const Route &r) const {
        double w = 0.0, v = 0.0;
        for (int idx : r.cust_idx) {
            w += customers[idx].weight;
            v += customers[idx].volume;
        }
        return {w, v};
    }

    double route_time_min(const Route &r) const {
        double dist = route_distance(r);
        double travel_min = (AVERAGE_SPEED_KMPH > 0.0)
                            ? (dist / AVERAGE_SPEED_KMPH * 60.0)
                            : 0.0;
        double service_min = 0.0;
        for (int idx : r.cust_idx) {
            service_min += customers[idx].service_time;
        }
        return travel_min + service_min;
    }

    pair<double, double> vehicle_capacity(const string &vid) const {
        auto it = veh_by_id.find(vid);
        if (it == veh_by_id.end()) return {0.0, 0.0};
        return {it->second.cap_weight, it->second.cap_volume};
    }

    bool route_feasible(const Route &r) const {
        if (r.cust_idx.empty()) return true;

        // HARD CONSTRAINT: 1 trip = 1 zone
        if ((int)r.cust_idx.size() > 1) return false;

        auto load = route_load(r);
        auto cap = vehicle_capacity(r.vehicle_id);
        if (cap.first > 0.0 && load.first > cap.first * capacity_util) return false;
        if (cap.second > 0.0 && load.second > cap.second * capacity_util) return false;

        auto itv = veh_by_id.find(r.vehicle_id);
        double dist = route_distance(r);
        if (itv != veh_by_id.end()) {
            const auto &v = itv->second;
            if (v.max_distance > 0.0 && dist > v.max_distance) return false;
            double tmin = route_time_min(r);
            if (v.max_hours > 0.0 && tmin > v.max_hours * 60.0) return false;
        }
        return true;
    }

    pair<Solution, vector<int>> destroy_random(
        const Solution &sol,
        double remove_fraction
    ) {
        vector<int> all_cust_indices;
        for (const auto &r : sol.routes) {
            for (int cid_idx : r.cust_idx) {
                all_cust_indices.push_back(cid_idx);
            }
        }
        int total_cust = (int)all_cust_indices.size();
        if (total_cust == 0) return {sol, {}};

        int n_remove = max(1, (int)(total_cust * remove_fraction));
        shuffle(all_cust_indices.begin(), all_cust_indices.end(), rng);

        vector<int> removed_customer_indices(
            all_cust_indices.begin(),
            all_cust_indices.begin() + n_remove
        );
        unordered_set<int> removed_set(
            removed_customer_indices.begin(),
            removed_customer_indices.end()
        );

        vector<Route> new_routes;
        for (auto &r : sol.routes) {
            Route nr = r;
            nr.cust_idx.clear();
            for (int cid_idx : r.cust_idx) {
                if (removed_set.find(cid_idx) == removed_set.end()) {
                    nr.cust_idx.push_back(cid_idx);
                }
            }
            if (!nr.cust_idx.empty()) new_routes.push_back(nr);
        }

        Solution destroyed;
        destroyed.routes = new_routes;
        destroyed.total_distance = 0.0;
        destroyed.total_cost = 0.0;
        return {destroyed, removed_customer_indices};
    }

    Solution repair_greedy(
        const Solution &sol,
        const vector<int> &removed
    ) {
        vector<Route> routes = sol.routes;

        for (int cid_idx : removed) {
            const auto &c = customers[cid_idx];
            const string &depot_id = c.nearest_depot_id;

            vector<int> candidates;
            for (int i = 0; i < (int)routes.size(); ++i) {
                if (routes[i].depot_id == depot_id) {
                    candidates.push_back(i);
                }
            }

            double best_delta = numeric_limits<double>::infinity();
            int best_route = -1;
            int best_pos = -1;

            for (int r_idx : candidates) {
                Route base = routes[r_idx];
                double old_dist = route_distance(base);
                int n = (int)base.cust_idx.size();
                for (int pos = 0; pos <= n; ++pos) {
                    Route temp = base;
                    temp.cust_idx.insert(temp.cust_idx.begin() + pos, cid_idx);
                    if (!route_feasible(temp)) continue;
                    double new_dist = route_distance(temp);
                    double delta = new_dist - old_dist;
                    if (delta < best_delta) {
                        best_delta = delta;
                        best_route = r_idx;
                        best_pos = pos;
                    }
                }
            }

            if (best_route != -1) {
                routes[best_route].cust_idx.insert(
                    routes[best_route].cust_idx.begin() + best_pos, cid_idx
                );
            } else {
                string vid;
                double best_cap = -1.0;
                for (auto &v : vehicles) {
                    if (v.start_depot_id == depot_id && v.cap_weight > best_cap) {
                        best_cap = v.cap_weight;
                        vid = v.id;
                    }
                }
                if (vid.empty()) continue;
                Route nr;
                nr.depot_id = depot_id;
                nr.vehicle_id = vid;
                nr.cust_idx.push_back(cid_idx);
                if (route_feasible(nr)) {
                    routes.push_back(nr);
                }
            }
        }

        Solution new_sol;
        new_sol.routes = routes;
        new_sol.total_distance = 0.0;
        new_sol.total_cost = 0.0;
        return new_sol;
    }

    Solution run(
        const Solution &initial,
        int max_iters = 200,
        double remove_fraction = 0.08,
        double start_T = 800.0,
        double cooling = 0.995,
        int patience = 60
    ) {
        cout << "\n================ ALNS (start) ================\n";
        cout << "[ALNS] max_iterations = " << max_iters
             << ", remove_fraction = " << remove_fraction << "\n";

        Solution current = initial;
        CostSummary cs_cur = compute_cost(current, customers, depots, vehicles);
        double cur_cost = cs_cur.total_cost;
        double cur_dist = cs_cur.total_distance;

        Solution best = current;
        double best_cost = cur_cost;
        double best_dist = cur_dist;

        cout << "[ALNS] Initial dist = " << cur_dist
             << " km, cost = " << cur_cost << "\n";

        double T = start_T;
        int no_improve = 0;

        for (int it = 0; it < max_iters; ++it) {
            auto [destroyed, removed] = destroy_random(current, remove_fraction);
            Solution candidate = repair_greedy(destroyed, removed);
            CostSummary cs_cand = compute_cost(candidate, customers, depots, vehicles);
            double cand_cost = cs_cand.total_cost;
            double cand_dist = cs_cand.total_distance;

            double delta = cand_cost - cur_cost;
            bool accept = false;
            if (delta < 0) {
                accept = true;
            } else {
                double prob = (T > 1e-9) ? exp(-delta / T) : 0.0;
                uniform_real_distribution<double> U(0.0, 1.0);
                if (U(rng) < prob) accept = true;
            }

            if (accept) {
                current = candidate;
                cur_cost = cand_cost;
                cur_dist = cand_dist;
                if (cur_cost < best_cost) {
                    best = current;
                    best_cost = cur_cost;
                    best_dist = cur_dist;
                    no_improve = 0;
                } else {
                    no_improve++;
                }
            } else {
                no_improve++;
            }

            T *= cooling;

            if ((it + 1) % 20 == 0) {
                cout << "[ALNS] Iter " << (it + 1) << "/" << max_iters
                     << ", current cost = " << cur_cost
                     << ", best cost = " << best_cost
                     << ", current dist = " << cur_dist
                     << ", T = " << T
                     << ", no_improve = " << no_improve << "\n";
            }

            if (no_improve >= patience) {
                cout << "[ALNS] Early stopping at iter " << (it + 1)
                     << " (no improvement " << patience << " iters)\n";
                break;
            }
        }

        best.total_distance = best_dist;
        best.total_cost = best_cost;
        cout << "[ALNS] Done. Best distance = " << best_dist
             << " km, best cost = " << best_cost << "\n";
        cout << "================ ALNS (done) ================\n\n";
        return best;
    }
};

// ============================================================
// 11. Tabu Search trên customer trung gian (objective = cost)
// ============================================================

class TabuSearch {
public:
    vector<Customer> &customers;
    const vector<Depot> &depots;
    const vector<Vehicle> &vehicles;
    unordered_map<string, int> idx_depot;
    unordered_map<string, Vehicle> veh_by_id;

    unordered_map<string, vector<string>> veh_ids_by_depot; // NEW

    double capacity_util;
    mt19937 rng;
    int tabu_tenure;

    TabuSearch(
        vector<Customer> &cust,
        const vector<Depot> &dep,
        const vector<Vehicle> &veh,
        double cap_util = 0.98,
        unsigned seed = 123,
        int tabu_tenure_ = 20
    ) : customers(cust),
        depots(dep),
        vehicles(veh),
        capacity_util(cap_util),
        rng(seed),
        tabu_tenure(tabu_tenure_)
    {
        for (int i = 0; i < (int)depots.size(); ++i) {
            idx_depot[depots[i].id] = i;
        }
        for (auto &v : vehicles) {
            veh_by_id[v.id] = v;
            veh_ids_by_depot[v.start_depot_id].push_back(v.id);   // NEW
        }
    }

    double route_distance(const Route &r) const {
        if (r.cust_idx.empty()) return 0.0;
        int dep_i = idx_depot.at(r.depot_id);
        const auto &dep = depots[dep_i];

        double dist = 0.0;
        const auto &c0 = customers[r.cust_idx[0]];
        dist += haversine_km(dep.lat, dep.lon, c0.lat, c0.lon);
        for (int i = 0; i + 1 < (int)r.cust_idx.size(); ++i) {
            const auto &ca = customers[r.cust_idx[i]];
            const auto &cb = customers[r.cust_idx[i + 1]];
            dist += haversine_km(ca.lat, ca.lon, cb.lat, cb.lon);
        }
        const auto &cl = customers[r.cust_idx.back()];
        dist += haversine_km(cl.lat, cl.lon, dep.lat, dep.lon);
        return dist;
    }

    pair<double, double> route_load(const Route &r) const {
        double w = 0.0, v = 0.0;
        for (int idx : r.cust_idx) {
            w += customers[idx].weight;
            v += customers[idx].volume;
        }
        return {w, v};
    }

    double route_time_min(const Route &r) const {
        double dist = route_distance(r);
        double travel_min = (AVERAGE_SPEED_KMPH > 0.0)
                            ? (dist / AVERAGE_SPEED_KMPH * 60.0)
                            : 0.0;
        double service_min = 0.0;
        for (int idx : r.cust_idx) {
            service_min += customers[idx].service_time;
        }
        return travel_min + service_min;
    }

    pair<double, double> vehicle_capacity(const string &vid) const {
        auto it = veh_by_id.find(vid);
        if (it == veh_by_id.end()) return {0.0, 0.0};
        return {it->second.cap_weight, it->second.cap_volume};
    }

    bool route_feasible(const Route &r) const {
        if (r.cust_idx.empty()) return true;

        // HARD CONSTRAINT: 1 trip = 1 zone
        if ((int)r.cust_idx.size() > 1) return false;

        auto load = route_load(r);
        auto cap = vehicle_capacity(r.vehicle_id);
        if (cap.first > 0.0 && load.first > cap.first * capacity_util) return false;
        if (cap.second > 0.0 && load.second > cap.second * capacity_util) return false;

        auto itv = veh_by_id.find(r.vehicle_id);
        double dist = route_distance(r);
        if (itv != veh_by_id.end()) {
            const auto &v = itv->second;
            if (v.max_distance > 0.0 && dist > v.max_distance) return false;
            double tmin = route_time_min(r);
            if (v.max_hours > 0.0 && tmin > v.max_hours * 60.0) return false;
        }
        return true;
    }

    struct TabuEntry {
        int cust_idx;
        int until_iter;
    };

    bool is_tabu(const vector<TabuEntry> &tabu_list, int cust_idx, int iter) const {
        for (auto &e : tabu_list) {
            if (e.cust_idx == cust_idx && iter < e.until_iter) return true;
        }
        return false;
    }

    Solution run(
        const Solution &start,
        int max_iters = 150,
        int neighborhood_size = 100
    ) {
        cout << "\n================ TABU SEARCH (start) ================" << "\n";
        cout << "[Tabu] max_iterations = " << max_iters
            << ", tabu_tenure = " << tabu_tenure << "\n";

        Solution current = start;
        CostSummary cs_start = compute_cost(current, customers, depots, vehicles);
        double cur_cost = cs_start.total_cost;
        double cur_dist = cs_start.total_distance;

        Solution best = current;
        double best_cost = cur_cost;
        double best_dist = cur_dist;

        cout << "[Tabu] Start from ALNS: dist = " << cur_dist
            << " km, cost = " << cur_cost << "\n";

        vector<TabuEntry> tabu_list;
        int stagnation = 0;

        for (int it = 0; it < max_iters; ++it) {
            double best_neigh_cost = numeric_limits<double>::infinity();
            double best_neigh_dist = 0.0;
            Solution best_neigh;
            int best_moved_customer = -1;
            bool found = false;

            // Neighborhood: đổi vehicle cho 1 zone (trip)
            for (int trial = 0; trial < neighborhood_size; ++trial) {
                if (current.routes.empty()) break;

                int r_idx = rng() % (int)current.routes.size();
                const Route &r = current.routes[r_idx];
                if (r.cust_idx.empty()) continue;    // safety, nhưng theo design thì không có

                int cid_idx = r.cust_idx[0];        // 1 trip = 1 zone

                // Danh sách vehicle tại depot của route này
                auto itveh = veh_ids_by_depot.find(r.depot_id);
                if (itveh == veh_ids_by_depot.end() || itveh->second.empty()) continue;

                const auto &veh_ids = itveh->second;
                if (veh_ids.size() <= 1) continue;  // chỉ có 1 xe thì không có gì để đổi

                // Chọn một vehicle khác với vehicle hiện tại
                string cur_vid = r.vehicle_id;
                string new_vid;
                for (int attempt = 0; attempt < 5; ++attempt) {
                    const string &cand_vid = veh_ids[rng() % veh_ids.size()];
                    if (cand_vid != cur_vid) {
                        new_vid = cand_vid;
                        break;
                    }
                }
                if (new_vid.empty() || new_vid == cur_vid) continue;

                Solution neigh = current;
                neigh.routes[r_idx].vehicle_id = new_vid;

                // Check feasible cho trip này
                if (!route_feasible(neigh.routes[r_idx])) continue;

                CostSummary cs_neigh = compute_cost(neigh, customers, depots, vehicles);
                double neigh_cost = cs_neigh.total_cost;
                double neigh_dist = cs_neigh.total_distance;

                bool tabu = is_tabu(tabu_list, cid_idx, it);
                bool aspir = (neigh_cost < best_cost);

                if ((!tabu || aspir) && neigh_cost < best_neigh_cost) {
                    best_neigh_cost = neigh_cost;
                    best_neigh_dist = neigh_dist;
                    best_neigh = neigh;
                    best_moved_customer = cid_idx;
                    found = true;
                }
            }

            if (!found) {
                cout << "[Tabu] No improving neighbor found at iter " << it + 1 << "\n";
                break;
            }

            current = best_neigh;
            cur_cost = best_neigh_cost;
            cur_dist = best_neigh_dist;

            if (best_moved_customer != -1) {
                tabu_list.push_back({best_moved_customer, it + tabu_tenure});
            }

            if (cur_cost < best_cost) {
                best = current;
                best_cost = cur_cost;
                best_dist = cur_dist;
                stagnation = 0;
            } else {
                stagnation++;
            }

            if ((it + 1) % 20 == 0) {
                cout << "[Tabu] Iter " << (it + 1) << "/" << max_iters
                    << ", current cost = " << cur_cost
                    << ", best cost = " << best_cost
                    << ", current dist = " << cur_dist
                    << ", stagnation = " << stagnation << "\n";
            }

            if (stagnation > tabu_tenure) {
                cout << "[Tabu] Stagnation > " << tabu_tenure
                    << ", restart from best.\n";
                current = best;
                stagnation = 0;
            }
        }

        best.total_distance = best_dist;
        best.total_cost = best_cost;
        cout << "[Tabu] Done. Best distance = " << best_dist
            << " km, best cost = " << best_cost << "\n";
        cout << "================ TABU SEARCH (done) ================" << "\n\n";
        return best;
    }

};

// ============================================================
// 12. Export helpers: zones, intermediate customers, routes, graphviz
// ============================================================

void export_zones_csv(
    const string &path,
    const vector<Zone> &zones,
    const vector<Customer> &customers
) {
    ofstream fout(path);
    if (!fout) {
        cerr << "[EXPORT] Cannot open " << path << endl;
        return;
    }

    fout << "Zone_Index,Depot_ID,Num_Customers,Medoid_Customer_ID,"
            "Total_Weight,Total_Volume,Total_Service\n";

    for (int zi = 0; zi < (int)zones.size(); ++zi) {
        const Zone &z = zones[zi];
        const Customer &c = customers[z.medoid_idx];

        fout << zi << ","
             << z.depot_id << ","
             << z.members.size() << ","
             << c.id << ","
             << z.total_weight << ","
             << z.total_volume << ","
             << z.total_service_time
             << "\n";
    }

    fout.close();
    cout << "[EXPORT] Zones saved to " << path << "\n";
}

void export_intermediate_customers_csv(
    const string &path,
    const vector<Zone> &zones,
    const vector<Customer> &customers,
    const vector<Customer> &inter
) {
    ofstream fout(path);
    if (!fout) {
        cerr << "[EXPORT] Cannot open " << path << endl;
        return;
    }

    fout << "Inter_Index,Inter_ID,Zone_Index,Depot_ID,Medoid_Customer_ID,"
            "Latitude,Longitude,Weight,Volume,Service\n";

    for (int zi = 0; zi < (int)zones.size(); ++zi) {
        const Zone &z = zones[zi];
        const Customer &center = customers[z.medoid_idx];
        const Customer &ic = inter[zi];

        fout << zi << ","
             << ic.id << ","
             << zi << ","
             << z.depot_id << ","
             << center.id << ","
             << ic.lat << ","
             << ic.lon << ","
             << ic.weight << ","
             << ic.volume << ","
             << ic.service_time
             << "\n";
    }

    fout.close();
    cout << "[EXPORT] Intermediate customers saved to " << path << "\n";
}

void export_routes_csv(
    const string &path,
    const Solution &sol,
    const vector<Customer> &customers
) {
    ofstream fout(path);
    if (!fout) {
        cerr << "[EXPORT] Cannot open " << path << endl;
        return;
    }

    fout << "Route_Index,Depot_ID,Vehicle_ID,Seq,Customer_ID,Latitude,Longitude\n";

    for (int rid = 0; rid < (int)sol.routes.size(); ++rid) {
        const Route &r = sol.routes[rid];

        for (int seq = 0; seq < (int)r.cust_idx.size(); ++seq) {
            int ci = r.cust_idx[seq];
            const Customer &c = customers[ci];

            fout << rid << ","
                 << r.depot_id << ","
                 << r.vehicle_id << ","
                 << seq << ","
                 << c.id << ","
                 << c.lat << ","
                 << c.lon
                 << "\n";
        }
    }

    fout.close();
    cout << "[EXPORT] Routes saved to " << path << "\n";
}

void export_graphviz(
    const string &path,
    const Solution &sol,
    const vector<Customer> &customers,
    const vector<Depot> &depots
) {
    ofstream fout(path);
    if (!fout) {
        cerr << "[EXPORT] Cannot open " << path << endl;
        return;
    }

    fout << "digraph Routes {\n";
    fout << "  graph [splines=true, overlap=false];\n";
    fout << "  node [shape=circle, fontsize=8];\n";

    unordered_map<string,int> idx_depot;
    for (int i = 0; i < (int)depots.size(); ++i)
        idx_depot[depots[i].id] = i;

    for (const Depot &d : depots) {
        fout << "  \"DEPOT_" << d.id << "\" [shape=box, style=filled, fillcolor=lightblue, "
             << "pos=\"" << d.lon << "," << d.lat << "!\"];\n";
    }

    unordered_set<string> added;
    for (const Route &r : sol.routes) {
        for (int ci : r.cust_idx) {
            const Customer &c = customers[ci];
            string name = "C_" + c.id;
            if (added.insert(name).second) {
                fout << "  \"" << name << "\" [shape=circle, "
                     << "pos=\"" << c.lon << "," << c.lat << "!\"];\n";
            }
        }
    }

    vector<string> colors = {
        "red","blue","green","orange","purple",
        "brown","cyan","magenta","darkgreen"
    };

    for (int rid = 0; rid < (int)sol.routes.size(); ++rid) {
        const Route &r = sol.routes[rid];
        if (r.cust_idx.empty()) continue;

        string color = colors[rid % colors.size()];
        string dnode = "DEPOT_" + r.depot_id;

        {
            const Customer &c0 = customers[r.cust_idx[0]];
            fout << "  \"" << dnode << "\" -> \"C_" << c0.id
                 << "\" [color=\"" << color << "\"];\n";
        }

        for (int i = 0; i + 1 < (int)r.cust_idx.size(); ++i) {
            const Customer &a = customers[r.cust_idx[i]];
            const Customer &b = customers[r.cust_idx[i+1]];
            fout << "  \"C_" << a.id << "\" -> \"C_" << b.id
                 << "\" [color=\"" << color << "\"];\n";
        }

        {
            const Customer &cl = customers[r.cust_idx.back()];
            fout << "  \"C_" << cl.id << "\" -> \"" << dnode
                 << "\" [color=\"" << color << "\"];\n";
        }
    }

    fout << "}\n";
    fout.close();
    cout << "[EXPORT] GraphViz saved to " << path << "\n";
}

void print_solution_summary(const Solution &sol) {
    cout << "\n========== SOLUTION SUMMARY ==========\n";
    cout << "Total routes   : " << sol.routes.size() << "\n";
    cout << "Total distance : " << sol.total_distance << "\n";
    cout << "Total cost     : " << sol.total_cost << "\n";

    for (int i = 0; i < (int)sol.routes.size(); ++i) {
        cout << "  Route " << i
             << " | Depot: " << sol.routes[i].depot_id
             << " | Vehicle: " << sol.routes[i].vehicle_id
             << " | Stops: " << sol.routes[i].cust_idx.size()
             << "\n";
    }
    cout << "=====================================\n";
}

// ============================================================
// 13. main()
// ============================================================

int main() {
    ios::sync_with_stdio(false);
    cin.tie(nullptr);

    cout << "=========== VRP PIPELINE C++ (Depot <-> Intermediate Customers) ===========" << "\n";

    string baseDir = "."; // folder chứa 4 file CSV
    string customers_path = baseDir + string("/customers.csv");
    string depots_path    = baseDir + string("/depots.csv");
    string vehicles_path  = baseDir + string("/vehicles.csv");
    string roads_path     = baseDir + string("/roads.csv");

    auto t_start = chrono::steady_clock::now();

    cout << "\n[MAIN] Step 1: Loading CSV data...\n";
    auto raw_customers = load_customers(customers_path);
    auto depots        = load_depots(depots_path);
    auto vehicles      = load_vehicles(vehicles_path);
    auto roads         = load_roads(roads_path);

    cout << "[MAIN] Loaded: "
         << raw_customers.size() << " raw customers, "
         << depots.size() << " depots, "
         << vehicles.size() << " vehicles, "
         << roads.size() << " roads\n";

    cout << "\n[MAIN] Step 2: Assign nearest depot for raw customers...\n";
    assign_nearest_depot(raw_customers, roads);

    cout << "\n[MAIN] Step 3: KMeans zoning per depot (on raw customers)...\n";
    auto zones0 = zoning_kmeans(raw_customers, depots, vehicles);

    cout << "\n[MAIN] Step 3b: Ensure zone feasibility (split overweight/overvolume)...\n";
    auto zones = ensure_zone_feasibility(zones0, raw_customers, vehicles, 0.9);

    cout << "\n[MAIN] Step 3c: Build intermediate customers for each zone...\n";
    auto inter_customers = build_intermediate_customers_from_zones(zones, raw_customers);
    cout << "[MAIN] Number of intermediate customers (zones) = "
         << inter_customers.size() << "\n";

    cout << "\n[MAIN] Step 4: Build initial solution on intermediate customers...\n";
    auto init_sol = build_initial_solution_stage1(inter_customers, depots, vehicles, 0.9);

    cout << "\n[MAIN] Step 5: Run ALNS (on intermediate customers)...\n";
    auto t_alns_start = chrono::steady_clock::now();
    ALNSSolver alns(inter_customers, depots, vehicles, 0.98, 42);
    auto alns_sol = alns.run(init_sol, 200, 0.08, 800.0, 0.995, 60);
    auto t_alns_end = chrono::steady_clock::now();
    cout << "[MAIN] ALNS finished in "
         << chrono::duration<double>(t_alns_end - t_alns_start).count()
         << " s\n";

    cout << "\n[MAIN] Step 6: Run Tabu Search (on intermediate customers)...\n";
    auto t_tabu_start = chrono::steady_clock::now();
    TabuSearch tabu(inter_customers, depots, vehicles, 0.98, 123, 20);
    auto final_sol = tabu.run(alns_sol, 150, 100);
    auto t_tabu_end = chrono::steady_clock::now();
    cout << "[MAIN] Tabu finished in "
         << chrono::duration<double>(t_tabu_end - t_tabu_start).count()
         << " s\n";

    CostSummary cs_final = compute_cost(final_sol, inter_customers, depots, vehicles);
    final_sol.total_distance = cs_final.total_distance;
    final_sol.total_cost = cs_final.total_cost;

    cout << "[MAIN] Final best distance = " << cs_final.total_distance
         << " km, final cost = " << cs_final.total_cost << "\n";
    cout << "[MAIN] Cost breakdown: fixed = " << cs_final.total_fixed_cost
         << ", variable = " << cs_final.total_variable_cost << "\n";

    print_solution_summary(final_sol);

    cout << "\n[MAIN] Step 7: Export final zones, intermediate customers, routes...\n";
    export_zones_csv("zones.csv", zones, raw_customers);
    export_intermediate_customers_csv("intermediate_customers.csv", zones, raw_customers, inter_customers);
    export_routes_csv("routes_best_objective_cpp.csv", final_sol, inter_customers);
    export_graphviz("routes_graph.dot", final_sol, inter_customers, depots);
    cout << "[MAIN] Use: dot -Kneato -n2 -Tpng routes_graph.dot -o routes_graph.png\n";

    auto t_end = chrono::steady_clock::now();
    cout << "[MAIN] Total runtime = "
         << chrono::duration<double>(t_end - t_start).count()
         << " s\n";

    cout << "=========== VRP PIPELINE C++ DONE ===========" << "\n";
    return 0;
}
