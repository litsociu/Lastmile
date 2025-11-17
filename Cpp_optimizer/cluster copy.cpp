#include <iostream>
#include <vector>
#include <cmath>
#include <algorithm>
#include <fstream>
#include <sstream>
#include <string>
#include <map>
#include <set>
#include <limits>
#include <queue>
#include <unordered_set>
#include <unordered_map>
#include <iomanip>
#include <random>
#include <utility>
#include <cctype>

using namespace std;
using ll = long long;

// Constants
static const double R_EARTH_KM = 6371.0;

// Provide M_PI if missing (common on Windows)
#ifndef M_PI
#define M_PI 3.14159265358979323846
#endif

// ===============================
// Data Structures
// ===============================

struct Customer {
    string id;     // Customer identifier
    double lat;    // Latitude
    double lon;    // Longitude
};

struct Vehicle {
    string id;
    double fixed_cost;
    double variable_cost;
};

struct ResultRow {
    int P;                   // Number of clusters
    double obj;              // Objective function value (intra + alpha * vehicle cost)
    double intra;            // Total intra-cluster distance
    double route;            // Route cost (TSP over medoids + depot)
    vector<int> medoids;     // Indices of medoid customers
    string vehicle_id;       // Optimal vehicle ID for this P
};

// ===============================
// Utility Functions
// ===============================

static double deg2rad(double deg) {
    return deg * M_PI / 180.0;
}

static double haversine_km_pair(double lat1, double lon1, double lat2, double lon2) {
    double dlat = deg2rad(lat2 - lat1);
    double dlon = deg2rad(lon2 - lon1);
    double a = sin(dlat/2.0) * sin(dlat/2.0)
             + cos(deg2rad(lat1)) * cos(deg2rad(lat2)) * sin(dlon/2.0) * sin(dlon/2.0);
    a = min(1.0, max(0.0, a));
    return 2.0 * R_EARTH_KM * asin(sqrt(a));
}

// ===============================
// CSV Parsing
// ===============================

bool read_csv_infer(
    const string &path,
    vector<Customer> &out_customers,
    string &id_col, string &lat_col, string &lon_col
) {
    ifstream ifs(path);
    if (!ifs.is_open()) return false;

    string header;
    if (!getline(ifs, header)) return false;

    vector<string> cols;
    string s;
    bool in_quotes = false;
    for (char c : header) {
        if (c == '"') in_quotes = !in_quotes;
        else if (c == ',' && !in_quotes) { cols.push_back(s); s.clear(); }
        else s.push_back(c);
    }
    cols.push_back(s);

    for (auto &c : cols) {
        while (!c.empty() && isspace((unsigned char)c.front())) c.erase(c.begin());
        while (!c.empty() && isspace((unsigned char)c.back())) c.pop_back();
    }

    id_col = lat_col = lon_col = "";
    for (auto &c : cols) {
        string lc = c;
        transform(lc.begin(), lc.end(), lc.begin(), ::tolower);
        if (id_col.empty() && (lc.find("id") != string::npos || lc.find("customer") != string::npos)) id_col = c;
        if (lat_col.empty() && (lc.find("lat") != string::npos || lc.find("latitude") != string::npos)) lat_col = c;
        if (lon_col.empty() && (lc.find("lon") != string::npos || lc.find("lng") != string::npos || lc.find("long") != string::npos)) lon_col = c;
    }

    if (lat_col == "" || lon_col == "") {
        if (cols.size() < 3) return false;
        id_col = cols[0];
        lat_col = cols[1];
        lon_col = cols[2];
    }

    unordered_map<string,int> idx;
    for (size_t i = 0; i < cols.size(); ++i) idx[cols[i]] = (int)i;

    string line;
    while (getline(ifs, line)) {
        vector<string> fields;
        string s;
        bool inq = false;
        for (size_t i = 0; i < line.size(); ++i) {
            char c = line[i];
            if (c == '"') {
                inq = !inq;
                if (inq && i+1 < line.size() && line[i+1] == '"') { s.push_back('"'); ++i; }
            } else if (c == ',' && !inq) { fields.push_back(s); s.clear(); }
            else s.push_back(c);
        }
        fields.push_back(s);

        while (fields.size() < cols.size()) fields.push_back("");

        if (idx.find(id_col) == idx.end() || idx.find(lat_col) == idx.end() || idx.find(lon_col) == idx.end()) continue;

        string id = fields[idx[id_col]];
        string slat = fields[idx[lat_col]];
        string slon = fields[idx[lon_col]];

        if (id.empty() || slat.empty() || slon.empty()) continue;

        try {
            double lat = stod(slat);
            double lon = stod(slon);
            out_customers.push_back({id, lat, lon});
        } catch (...) { continue; }
    }

    return true;
}

bool read_vehicles_csv(const string &path, vector<Vehicle> &vehicles) {
    ifstream ifs(path);
    if (!ifs.is_open()) return false;

    string header;
    if (!getline(ifs, header)) return false;

    vector<string> cols;
    string s;
    bool in_quotes = false;
    for (char c : header) {
        if (c == '"') in_quotes = !in_quotes;
        else if (c == ',' && !in_quotes) { cols.push_back(s); s.clear(); }
        else s.push_back(c);
    }
    cols.push_back(s);

    for (auto &c : cols) {
        while (!c.empty() && isspace((unsigned char)c.front())) c.erase(c.begin());
        while (!c.empty() && isspace((unsigned char)c.back())) c.pop_back();
    }

    unordered_map<string,int> idx;
    for (size_t i = 0; i < cols.size(); ++i) idx[cols[i]] = (int)i;

    string id_col = "", fixed_col = "", variable_col = "";
    for (auto &c : cols) {
        string lc = c;
        transform(lc.begin(), lc.end(), lc.begin(), ::tolower);
        if (lc.find("id") != string::npos) id_col = c;
        else if (lc.find("fixed") != string::npos) fixed_col = c;
        else if (lc.find("variable") != string::npos || lc.find("var") != string::npos) variable_col = c;
    }

    if (id_col == "" || fixed_col == "" || variable_col == "") return false;

    string line;
    while (getline(ifs, line)) {
        vector<string> fields;
        string s; bool inq = false;
        for (size_t i = 0; i < line.size(); ++i) {
            char c = line[i];
            if (c == '"') {
                inq = !inq;
                if (inq && i+1 < line.size() && line[i+1] == '"') { s.push_back('"'); ++i; }
            } else if (c == ',' && !inq) { fields.push_back(s); s.clear(); }
            else s.push_back(c);
        }
        fields.push_back(s);

        while (fields.size() < cols.size()) fields.push_back("");

        try {
            Vehicle v;
            v.id = fields[idx[id_col]];
            v.fixed_cost = stod(fields[idx[fixed_col]]);
            v.variable_cost = stod(fields[idx[variable_col]]);
            vehicles.push_back(v);
        } catch (...) { continue; }
    }

    return !vehicles.empty();
}

// ===============================
// Distance Matrix Computation
// ===============================

vector<vector<double>> build_full_hav_matrix(const vector<Customer> &cust) {
    int N = (int)cust.size();
    vector<vector<double>> D(N, vector<double>(N, 0.0));
    for (int i = 0; i < N; ++i) {
        D[i][i] = 0.0;
        for (int j = i+1; j < N; ++j) {
            double d = haversine_km_pair(cust[i].lat, cust[i].lon, cust[j].lat, cust[j].lon);
            D[i][j] = D[j][i] = d;
        }
    }
    return D;
}

vector<vector<double>> haversine_matrix_points(const vector<pair<double,double>> &pts) {
    int n = (int)pts.size();
    vector<vector<double>> M(n, vector<double>(n, 0.0));
    for (int i = 0; i < n; ++i) {
        for (int j = i+1; j < n; ++j) {
            double d = haversine_km_pair(pts[i].first, pts[i].second, pts[j].first, pts[j].second);
            M[i][j] = M[j][i] = d;
        }
    }
    return M;
}

// ===============================
// Clustering: PAM (k-medoids)
// ===============================

pair<vector<int>, vector<int>> pam_medoids(const vector<vector<double>> &D, int k, int max_iter=200, int seed=0) {
    int N = (int)D.size();
    if (k <= 0 || k > N) throw runtime_error("k out of range");

    mt19937 rng(seed);
    unordered_set<int> chosen;
    vector<int> medoids;

    while ((int)medoids.size() < k) {
        int c = rng() % N;
        if (chosen.insert(c).second) medoids.push_back(c);
    }

    for (int iter = 0; iter < max_iter; ++iter) {
        vector<int> assign(N, -1);
        for (int i = 0; i < N; ++i) {
            double best = numeric_limits<double>::infinity();
            int bi = -1;
            for (int m = 0; m < k; ++m) if (D[i][medoids[m]] < best) { best = D[i][medoids[m]]; bi = m; }
            assign[i] = bi;
        }

        double current_cost = 0.0;
        for (int i = 0; i < N; ++i) current_cost += D[i][medoids[assign[i]]];

        bool improved = false;
        for (int m_idx = 0; m_idx < k && !improved; ++m_idx) {
            for (int cand = 0; cand < N; ++cand) {
                if (find(medoids.begin(), medoids.end(), cand) != medoids.end()) continue;

                vector<int> newmed = medoids;
                newmed[m_idx] = cand;

                double newcost = 0.0;
                for (int i = 0; i < N; ++i) {
                    double bestd = numeric_limits<double>::infinity();
                    for (int mm = 0; mm < k; ++mm) bestd = min(bestd, D[i][newmed[mm]]);
                    newcost += bestd;
                    if (newcost + 1e-9 >= current_cost) break;
                }

                if (newcost + 1e-9 < current_cost) {
                    medoids = newmed;
                    improved = true;
                    break;
                }
            }
        }

        if (!improved) break;
    }

    vector<int> assign(N, -1);
    for (int i = 0; i < N; ++i) {
        double best = numeric_limits<double>::infinity();
        int bi = -1;
        for (size_t m = 0; m < medoids.size(); ++m) if (D[i][medoids[m]] < best) { best = D[i][medoids[m]]; bi = (int)m; }
        assign[i] = bi;
    }

    return {medoids, assign};
}

// ===============================
// Clustering: K-means
// ===============================

pair<vector<int>, vector<int>> kmeans_lloyd(
    const vector<pair<double,double>> &points,
    int k, int max_iter=200, int seed=0
) {
    int N = (int)points.size();
    if (k <= 0 || k > N) throw runtime_error("k out of range for kmeans");

    mt19937 rng(seed);
    unordered_set<int> chosen;
    vector<int> medoid_init;
    while ((int)medoid_init.size() < k) {
        int c = rng() % N;
        if (chosen.insert(c).second) medoid_init.push_back(c);
    }

    vector<pair<double,double>> centers(k);
    for (int i = 0; i < k; ++i) centers[i] = points[medoid_init[i]];

    vector<int> labels(N, 0);

    for (int it = 0; it < max_iter; ++it) {
        bool changed = false;
        for (int i = 0; i < N; ++i) {
            double best = numeric_limits<double>::infinity();
            int bi = 0;
            for (int j = 0; j < k; ++j) {
                double dlat = points[i].first - centers[j].first;
                double dlon = points[i].second - centers[j].second;
                double dd = dlat*dlat + dlon*dlon;
                if (dd < best) { best = dd; bi = j; }
            }
            if (labels[i] != bi) { labels[i] = bi; changed = true; }
        }

        vector<double> sx(k,0.0), sy(k,0.0);
        vector<int> cnt(k,0);
        for (int i = 0; i < N; ++i) { int c = labels[i]; sx[c] += points[i].first; sy[c] += points[i].second; cnt[c]++; }
        for (int j = 0; j < k; ++j) {
            if (cnt[j] > 0) { centers[j].first /= cnt[j]; centers[j].second /= cnt[j]; }
            else { centers[j] = points[rng()%N]; changed = true; }
        }

        if (!changed) break;
    }

    return {labels, medoid_init};
}

// ===============================
// TSP heuristics
// ===============================

double tsp_length_from_distance_matrix(const vector<vector<double>> &D) {
    int M = (int)D.size();
    if (M <= 1) return 0.0;

    vector<int> visited; visited.push_back(0);
    vector<char> used(M,0); used[0] = 1;

    for (int step = 1; step < M; ++step) {
        int cur = visited.back();
        double best = numeric_limits<double>::infinity();
        int bi = -1;
        for (int j = 0; j < M; ++j) if (!used[j]) {
            if (D[cur][j] < best) { best = D[cur][j]; bi = j; }
        }
        if (bi == -1) break;
        visited.push_back(bi); used[bi] = 1;
    }

    bool improved = true; int it_count = 0;
    while (improved && it_count < 5000) {
        improved = false; ++it_count;
        for (int i = 1; i+1 < M-1; ++i) {
            for (int j = i+1; j < M; ++j) {
                if (j-i==1) continue;
                int a = visited[i-1], b = visited[i];
                int c = visited[j-1], d = visited[j % M];
                if (D[a][c] + D[b][d] + 1e-9 < D[a][b] + D[c][d]) {
                    reverse(visited.begin()+i, visited.begin()+j);
                    improved = true;
                }
            }
            if (improved) break;
        }
    }

    double length = 0.0;
    for (int i = 0; i < M-1; ++i) length += D[visited[i]][visited[i+1]];
    length += D[visited.back()][visited.front()];
    return length;
}

// ===============================
// Main
// ===============================

int main(int argc, char** argv) {
    ios::sync_with_stdio(false);
    cin.tie(nullptr);

    string customers_path = "D:/LogChaLan/Lastmile-1/data/LMDO processed/c_lo_lam/customers.csv";
    string vehicles_path = "D:/LogChaLan/Lastmile-1/data/LMDO processed/c_lo_lam/vehicles.csv";
    double depot_lat = NAN, depot_lon = NAN;
    int pmin = 2, pmax = 8;
    double alpha = 1.0;
    int max_exact_n = 2000;
    string out_prefix = "results";
    int seed = 0;

    for (int i = 1; i < argc; ++i) {
        string a = argv[i];
        if (a == "--customers" && i+1 < argc) customers_path = argv[++i];
        else if (a == "--customers" && i+1 < argc) customers_path = argv[++i];
        else if (a == "--vehicles" && i+1 < argc) vehicles_path = argv[++i];
        else if (a == "--depot-lat" && i+1 < argc) depot_lat = stod(argv[++i]);
        else if (a == "--depot-lon" && i+1 < argc) depot_lon = stod(argv[++i]);
        else if (a == "--pmin" && i+1 < argc) pmin = stoi(argv[++i]);
        else if (a == "--pmax" && i+1 < argc) pmax = stoi(argv[++i]);
        else if (a == "--alpha" && i+1 < argc) alpha = stod(argv[++i]);
        else if (a == "--max-exact-n" && i+1 < argc) max_exact_n = stoi(argv[++i]);
        else if (a == "--out-prefix" && i+1 < argc) out_prefix = argv[++i];
        else if (a == "--seed" && i+1 < argc) seed = stoi(argv[++i]);
    }

    // -------------------------------
    // Read customers
    // -------------------------------
    vector<Customer> customers;
    string id_col, lat_col, lon_col;
    if (!read_csv_infer(customers_path, customers, id_col, lat_col, lon_col)) {
        cerr << "Failed to read customers CSV: " << customers_path << "\n";
        return 2;
    }

    int N = (int)customers.size();
    if (N == 0) { cerr << "No customers loaded.\n"; return 3; }
    cout << "Loaded " << N << " customers.\n";

    // -------------------------------
    // Read vehicles
    // -------------------------------
    vector<Vehicle> vehicles;
    if (!read_vehicles_csv(vehicles_path, vehicles)) {
        cerr << "Failed to read vehicles CSV or no valid vehicles: " << vehicles_path << "\n";
        return 4;
    }
    cout << "Loaded " << vehicles.size() << " vehicles.\n";

    // -------------------------------
    // Adjust cluster limits
    // -------------------------------
    if (pmin < 1) pmin = 1;
    if (pmax < pmin) pmax = pmin;
    if (pmax > N-1) pmax = max(pmin, N-1);

    vector<ResultRow> results;
    ResultRow best; bool have_best = false;

    bool use_exact = (N <= max_exact_n);
    cout << "Using exact PAM? " << (use_exact ? "YES" : "NO") << " (N=" << N << ", max_exact_n=" << max_exact_n << ")\n";

    vector<pair<double,double>> coords;
    coords.reserve(N);
    for (auto &c: customers) coords.emplace_back(c.lat, c.lon);

    vector<vector<double>> Dfull;
    if (use_exact) {
        cout << "Building full haversine distance matrix...\n";
        Dfull = build_full_hav_matrix(customers);
        cout << "Done.\n";
    }

    // -------------------------------
    // Main loop over P
    // -------------------------------
    for (int P = pmin; P <= pmax; ++P) {
        cout << "Trying P=" << P << " ...\n";
        double intra = 0.0, route_cost = 0.0, obj = 0.0;
        vector<int> medoid_indices, assign;

        if (use_exact) {
            auto pamres = pam_medoids(Dfull, P, 200, seed);
            medoid_indices = pamres.first;
            assign = pamres.second;

            for (int i = 0; i < N; ++i) intra += Dfull[i][ medoid_indices[assign[i]] ];

            pair<double,double> depot;
            if (!isnan(depot_lat) && !isnan(depot_lon)) depot = {depot_lat, depot_lon};
            else {
                double s1=0.0, s2=0.0;
                for (auto &p: coords) { s1+=p.first; s2+=p.second; }
                depot = {s1/N, s2/N};
            }

            vector<pair<double,double>> nodes; nodes.push_back(depot);
            for (int m : medoid_indices) nodes.push_back(coords[m]);
            route_cost = tsp_length_from_distance_matrix(haversine_matrix_points(nodes));

        } else {
            auto km = kmeans_lloyd(coords, P, 200, seed);
            vector<int> labels = km.first;

            vector<pair<double,double>> cent(P,{0.0,0.0});
            vector<int> cnt(P,0);
            for (int i = 0; i < N; ++i) { int l = labels[i]; cent[l].first+=coords[i].first; cent[l].second+=coords[i].second; cnt[l]++; }
            for (int j=0;j<P;j++) if(cnt[j]>0) { cent[j].first/=cnt[j]; cent[j].second/=cnt[j]; }

            medoid_indices.assign(P,-1);
            for (int j=0;j<P;j++) {
                if(cnt[j]==0){ medoid_indices[j]=rand()%N; continue; }
                double bestd=numeric_limits<double>::infinity(); int bi=-1;
                for(int i=0;i<N;i++) if(labels[i]==j){
                    double d=haversine_km_pair(coords[i].first, coords[i].second, cent[j].first, cent[j].second);
                    if(d<bestd){ bestd=d; bi=i; }
                }
                if(bi<0) bi=rand()%N;
                medoid_indices[j]=bi;
            }

            assign.assign(N,-1);
            intra=0.0;
            for(int i=0;i<N;i++){
                double bestd=numeric_limits<double>::infinity(); int bi=-1;
                for(int j=0;j<P;j++){
                    double d=haversine_km_pair(coords[i].first, coords[i].second, coords[medoid_indices[j]].first, coords[medoid_indices[j]].second);
                    if(d<bestd){ bestd=d; bi=j; }
                }
                assign[i]=bi; intra+=bestd;
            }

            pair<double,double> depot;
            if(!isnan(depot_lat)&&!isnan(depot_lon)) depot={depot_lat,depot_lon};
            else {
                double s1=0.0, s2=0.0;
                for(auto &p: coords){ s1+=p.first; s2+=p.second; }
                depot={s1/N, s2/N};
            }

            vector<pair<double,double>> nodes; nodes.push_back(depot);
            for(int m: medoid_indices) nodes.push_back(coords[m]);
            route_cost=tsp_length_from_distance_matrix(haversine_matrix_points(nodes));
        }

        // -------------------------------
        // Vehicle selection
        // -------------------------------
        string best_vehicle_id;
        double best_vehicle_cost = numeric_limits<double>::infinity();
        for(auto &v: vehicles){
            double total_cost = v.fixed_cost + v.variable_cost * route_cost;
            if(total_cost < best_vehicle_cost){
                best_vehicle_cost = total_cost;
                best_vehicle_id = v.id;
            }
        }

        obj = intra + alpha * best_vehicle_cost;

        ResultRow row;
        row.P = P;
        row.obj = obj;
        row.intra = intra;
        row.route = route_cost;
        row.medoids = medoid_indices;
        row.vehicle_id = best_vehicle_id;
        results.push_back(row);

        cout<<"  P="<<P<<": obj="<<obj<<", intra="<<intra<<", route="<<route_cost<<", vehicle="<<best_vehicle_id<<"\n";
        if(!have_best || obj<best.obj){ best=row; have_best=true; }
    }

    // -------------------------------
    // Save clusters
    // -------------------------------
    auto final_medoids = best.medoids;
    vector<int> final_assign(N,-1);
    vector<double> dist_to_med(N,0.0);
    for(int i=0;i<N;i++){
        double bestd=numeric_limits<double>::infinity();
        int bi=-1;
        for(size_t k=0;k<final_medoids.size();k++){
            int med=final_medoids[k];
            double d=haversine_km_pair(coords[i].first, coords[i].second, coords[med].first, coords[med].second);
            if(d<bestd){ bestd=d; bi=(int)k; }
        }
        final_assign[i]=bi; dist_to_med[i]=bestd;
    }

    string out_clusters=out_prefix+"_clusters.csv";
    ofstream ofs(out_clusters);
    ofs<<"Customer_ID,Cluster_ID,Assigned_Medoid_Index,Assigned_Medoid_ID,Distance_km\n";
    for(int i=0;i<N;i++){
        int cluster_id=final_assign[i];
        int medoid_global_idx=final_medoids[cluster_id];
        ofs<<"\""<<customers[i].id<<"\""<<","<<cluster_id<<","<<medoid_global_idx
            <<",\""<<customers[medoid_global_idx].id<<"\""<<","<<fixed<<setprecision(6)<<dist_to_med[i]<<"\n";
    }
    ofs.close();
    cout<<"Saved clusters to "<<out_clusters<<"\n";

    string out_obj=out_prefix+"_obj_vs_P.csv";
    ofstream ofs2(out_obj);
    ofs2<<"P,obj,intra,route,vehicle_id\n";
    for(auto &r: results) ofs2<<r.P<<","<<r.obj<<","<<r.intra<<","<<r.route<<","<<r.vehicle_id<<"\n";
    ofs2.close();
    cout<<"Saved objective curve to "<<out_obj<<"\n";

    cout<<"Best P="<<best.P<<", vehicle="<<best.vehicle_id<<", objective="<<best.obj<<"\n";
    cout<<"Done.\n";
    return 0;
}
