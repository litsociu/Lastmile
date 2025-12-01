#pragma once

#include "config.h"
#include "initialization.h"
#include "objectives.h"
#include <random>

using namespace std;

class ALNS {
public:
    static Solution run(const Instance& inst, Solution initial_sol, int max_iter = 50, int seed = 42) {
        mt19937 rng(seed);
        Solution current = initial_sol;
        current.calculate_objective(inst);
        Solution best = current;
        
        cout << "[ALNS] Start Obj: " << current.objective << endl;
        
        for (int it = 0; it < max_iter; ++it) {
            Solution candidate = current; // Copy
            
            // 1. Destroy (Randomly remove a cluster)
            // Flatten routes to find a random cluster
            vector<pair<string, int>> all_routes; // vid, route_idx
            for (auto& p : candidate.routes) {
                for (int i = 0; i < p.second.size(); ++i) {
                    all_routes.push_back({p.first, i});
                }
            }
            
            if (!all_routes.empty()) {
                // Remove 10% of clusters
                int n_remove = max(1, (int)(all_routes.size() * 0.1));
                shuffle(all_routes.begin(), all_routes.end(), rng);
                
                vector<Route> removed_routes;
                
                // Mark for removal (tricky with vector indices shifting)
                // Easier: Rebuild routes map
                map<string, vector<Route>> new_routes_map = candidate.routes;
                vector<Route> unassigned;
                
                for (int k = 0; k < n_remove; ++k) {
                    string vid = all_routes[k].first;
                    int idx = all_routes[k].second;
                    // We can't easily remove by index if we process multiple from same vehicle
                    // Simplified: Just remove the first one selected
                    // In full impl, handle indices carefully
                }
                
                // VERY SIMPLIFIED DESTROY/REPAIR FOR DEMO
                // Just try to move one cluster to another vehicle
                int idx = rng() % all_routes.size();
                string vid_from = all_routes[idx].first;
                int r_idx = all_routes[idx].second;
                
                if (r_idx < candidate.routes[vid_from].size()) {
                    Route r = candidate.routes[vid_from][r_idx];
                    
                    // Remove from old
                    candidate.routes[vid_from].erase(candidate.routes[vid_from].begin() + r_idx);
                    
                    // Try to insert into best vehicle
                    string best_vid = "";
                    double best_cost = 1e15;
                    
                    for (auto& p : inst.vehicles) {
                        string vid_to = p.first;
                        const Vehicle& veh = p.second;
                        
                        // Check capacity/time (simplified check)
                        // In real impl, need to check existing load + new route
                        
                        // Calculate cost
                        string depot = veh.start_depot;
                        auto leg1 = inst.get_dist_time(depot, r.centroid_id);
                        auto leg2 = inst.get_dist_time(r.centroid_id, depot);
                        double cost = (leg1.first + leg2.first) * veh.var_cost;
                        
                        if (cost < best_cost) {
                            best_cost = cost;
                            best_vid = vid_to;
                        }
                    }
                    
                    if (!best_vid.empty()) {
                        r.vehicle_id = best_vid;
                        // Update stops for new depot
                        string new_depot = inst.vehicles.at(best_vid).start_depot;
                        r.stops[0] = new_depot;
                        r.stops.back() = new_depot;
                        
                        candidate.routes[best_vid].push_back(r);
                    } else {
                        // Put back if fail
                        candidate.routes[vid_from].push_back(r);
                    }
                }
            }
            
            // Evaluate
            double obj = candidate.calculate_objective(inst);
            
            if (obj < current.objective) {
                current = candidate;
                if (obj < best.objective) {
                    best = candidate;
                    cout << "[ALNS] New Best: " << best.objective << " (Iter " << it << ")" << endl;
                }
            }
        }
        
        return best;
    }
};
