#pragma once

#include <string>
#include <vector>
#include <map>
#include <set>
#include <iostream>
#include <cmath>
#include <algorithm>

using namespace std;

struct Customer {
    string id;
    double lat;
    double lon;
    double weight;
    double volume;
    double service_time;
    double tw_start;
    double tw_end;
};

struct Depot {
    string id;
    double lat;
    double lon;
    double capacity;
};

struct Vehicle {
    string id;
    string start_depot;
    double cap_weight;
    double cap_volume;
    double max_time;
    double fixed_cost;
    double var_cost;
    double max_dist;
};

struct Road {
    string from;
    string to;
    double distance;
    double time;
};

struct Instance {
    map<string, Customer> customers;
    map<string, Depot> depots;
    map<string, Vehicle> vehicles;
    
    // Distance matrix: map<from, map<to, pair<dist, time>>>
    map<string, map<string, pair<double, double>>> matrix;
    
    // Helper to get dist/time
    pair<double, double> get_dist_time(const string& from, const string& to) const {
        if (from == to) return {0.0, 0.0};
        auto it1 = matrix.find(from);
        if (it1 != matrix.end()) {
            auto it2 = it1->second.find(to);
            if (it2 != it1->second.end()) {
                return it2->second;
            }
        }
        // Fallback: Haversine distance, assume 30km/h
        // Note: In real app, should handle missing road data better
        return {1e9, 1e9}; 
    }
};

struct Route {
    string vehicle_id;
    vector<string> stops; // [depot, c1, c2, ..., depot]
    
    // Metadata for cluster info
    string cluster_id;
    string centroid_id;
    vector<string> cluster_customers;
    
    double load_w = 0;
    double load_v = 0;
    double distance = 0;
    double time = 0;
};

struct Solution {
    map<string, vector<Route>> routes; // vehicle_id -> list of routes
    double objective = 1e15;
    
    // Helper to get total cost
    double calculate_objective(const Instance& inst);
};
