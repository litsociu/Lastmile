#pragma once

#include "config.h"

double Solution::calculate_objective(const Instance& inst) {
    double total_cost = 0;
    double penalty = 0;
    
    // 1. Route Costs
    for (auto& p : routes) {
        string vid = p.first;
        const Vehicle& veh = inst.vehicles.at(vid);
        
        if (p.second.empty()) continue;
        
        total_cost += veh.fixed_cost;
        
        for (auto& route : p.second) {
            // Distance cost
            // Logic: Depot -> Centroid -> Depot
            string depot = route.stops[0];
            string centroid = route.centroid_id;
            
            auto leg1 = inst.get_dist_time(depot, centroid);
            auto leg2 = inst.get_dist_time(centroid, depot);
            
            double dist = leg1.first + leg2.first;
            total_cost += dist * veh.var_cost;
            
            // Time Window Penalty (Simplified)
            // Check if centroid arrival is within TW of centroid customer?
            // Or aggregate TW?
            // For now, just check centroid customer TW
            double arrival = leg1.second;
            const Customer& c = inst.customers.at(centroid);
            if (arrival > c.tw_end) {
                penalty += (arrival - c.tw_end) * 100.0; // Lambda_L
            }
        }
    }
    
    // 2. Unserved Penalty
    // Find all served customers
    set<string> served;
    for (auto& p : routes) {
        for (auto& r : p.second) {
            for (auto& c : r.cluster_customers) served.insert(c);
        }
    }
    
    for (auto& p : inst.customers) {
        if (served.find(p.first) == served.end()) {
            penalty += 1e5; // Big penalty
        }
    }
    
    this->objective = total_cost + penalty;
    return this->objective;
}
