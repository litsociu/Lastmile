#pragma once

#include "config.h"
#include "utils.h"
#include <random>
#include <limits>

using namespace std;

class Initializer {
public:
    static Solution create_initial_solution(const Instance& inst, int seed = 42) {
        Solution sol;
        mt19937 rng(seed);
        
        // 1. K-Means Clustering
        // Determine K
        double total_w = 0;
        double total_v = 0;
        for (auto& p : inst.customers) {
            total_w += p.second.weight;
            total_v += p.second.volume;
        }
        
        double avg_cap_w = 0;
        double avg_cap_v = 0;
        for (auto& p : inst.vehicles) {
            avg_cap_w += p.second.cap_weight;
            avg_cap_v += p.second.cap_volume;
        }
        avg_cap_w /= inst.vehicles.size();
        avg_cap_v /= inst.vehicles.size();
        
        int k_w = ceil(total_w / (avg_cap_w * 0.7));
        int k_v = ceil(total_v / (avg_cap_v * 0.7));
        int K = max({10, k_w, k_v});
        K = min(K, (int)inst.customers.size() / 5);
        
        cout << "[INIT] K-Means with K=" << K << endl;
        
        // Initialize centroids
        vector<pair<double, double>> centroids;
        vector<string> cust_ids;
        for (auto& p : inst.customers) cust_ids.push_back(p.first);
        
        // Random pick
        shuffle(cust_ids.begin(), cust_ids.end(), rng);
        for (int i = 0; i < K; ++i) {
            centroids.push_back({inst.customers.at(cust_ids[i]).lat, inst.customers.at(cust_ids[i]).lon});
        }
        
        // Run K-Means
        map<int, vector<string>> clusters;
        for (int iter = 0; iter < 20; ++iter) {
            clusters.clear();
            vector<pair<double, double>> new_centroids(K, {0, 0});
            vector<int> counts(K, 0);
            
            for (auto& cid : cust_ids) {
                double min_dist = 1e9;
                int best_k = 0;
                double lat = inst.customers.at(cid).lat;
                double lon = inst.customers.at(cid).lon;
                
                for (int k = 0; k < K; ++k) {
                    double d = haversine(lat, lon, centroids[k].first, centroids[k].second);
                    if (d < min_dist) {
                        min_dist = d;
                        best_k = k;
                    }
                }
                clusters[best_k].push_back(cid);
                new_centroids[best_k].first += lat;
                new_centroids[best_k].second += lon;
                counts[best_k]++;
            }
            
            // Update centroids
            double shift = 0;
            for (int k = 0; k < K; ++k) {
                if (counts[k] > 0) {
                    new_centroids[k].first /= counts[k];
                    new_centroids[k].second /= counts[k];
                    shift += haversine(centroids[k].first, centroids[k].second, new_centroids[k].first, new_centroids[k].second);
                    centroids[k] = new_centroids[k];
                }
            }
            if (shift < 0.1) break;
        }
        
        // 2. Assign Clusters to Vehicles (Greedy)
        // Flatten clusters to list of cluster objects
        struct ClusterInfo {
            int id;
            vector<string> nodes;
            double lat, lon;
            double w, v;
        };
        
        vector<ClusterInfo> cluster_infos;
        for (auto& p : clusters) {
            if (p.second.empty()) continue;
            ClusterInfo ci;
            ci.id = p.first;
            ci.nodes = p.second;
            ci.w = 0; ci.v = 0;
            ci.lat = 0; ci.lon = 0;
            for (auto& cid : ci.nodes) {
                ci.w += inst.customers.at(cid).weight;
                ci.v += inst.customers.at(cid).volume;
                ci.lat += inst.customers.at(cid).lat;
                ci.lon += inst.customers.at(cid).lon;
            }
            ci.lat /= ci.nodes.size();
            ci.lon /= ci.nodes.size();
            cluster_infos.push_back(ci);
        }
        
        // Sort clusters? Maybe random shuffle is better for distribution
        shuffle(cluster_infos.begin(), cluster_infos.end(), rng);
        
        map<string, double> veh_time;
        for (auto& p : inst.vehicles) veh_time[p.first] = 0;
        
        for (auto& clus : cluster_infos) {
            bool assigned = false;
            
            // Find best vehicle (nearest depot)
            vector<string> vids;
            for (auto& p : inst.vehicles) vids.push_back(p.first);
            
            // Sort vehicles by distance to cluster center
            sort(vids.begin(), vids.end(), [&](const string& a, const string& b) {
                string d1 = inst.vehicles.at(a).start_depot;
                string d2 = inst.vehicles.at(b).start_depot;
                double dist1 = haversine(clus.lat, clus.lon, inst.depots.at(d1).lat, inst.depots.at(d1).lon);
                double dist2 = haversine(clus.lat, clus.lon, inst.depots.at(d2).lat, inst.depots.at(d2).lon);
                return dist1 < dist2;
            });
            
            for (auto& vid : vids) {
                const Vehicle& veh = inst.vehicles.at(vid);
                if (clus.w > veh.cap_weight || clus.v > veh.cap_volume) continue;
                
                // Find centroid customer
                string best_c = clus.nodes[0];
                double min_d = 1e9;
                for (auto& c : clus.nodes) {
                    double d = haversine(clus.lat, clus.lon, inst.customers.at(c).lat, inst.customers.at(c).lon);
                    if (d < min_d) {
                        min_d = d;
                        best_c = c;
                    }
                }
                
                // Estimate time
                string depot = veh.start_depot;
                auto t1 = inst.get_dist_time(depot, best_c);
                auto t2 = inst.get_dist_time(best_c, depot);
                double trip_time = t1.second + inst.customers.at(best_c).service_time + t2.second;
                
                if (veh_time[vid] + trip_time <= veh.max_time) {
                    // Create Route
                    Route r;
                    r.vehicle_id = vid;
                    r.stops = {depot};
                    r.stops.insert(r.stops.end(), clus.nodes.begin(), clus.nodes.end());
                    r.stops.push_back(depot);
                    
                    r.cluster_id = to_string(clus.id);
                    r.centroid_id = best_c;
                    r.cluster_customers = clus.nodes;
                    r.load_w = clus.w;
                    r.load_v = clus.v;
                    
                    sol.routes[vid].push_back(r);
                    veh_time[vid] += trip_time;
                    assigned = true;
                    break;
                }
            }
            
            if (!assigned) {
                cout << "[WARN] Cluster " << clus.id << " unassigned." << endl;
            }
        }
        
        return sol;
    }
};
