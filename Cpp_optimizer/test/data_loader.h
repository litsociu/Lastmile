#pragma once

#include "config.h"
#include "utils.h"
#include <fstream>
#include <iostream>

using namespace std;

class DataLoader {
public:
    static Instance load_instance(const string& data_dir) {
        Instance inst;
        
        // 1. Load Customers
        cout << "Loading customers..." << endl;
        ifstream file_cust(data_dir + "/customers.csv");
        string line;
        getline(file_cust, line); // skip header
        while (getline(file_cust, line)) {
            vector<string> row = split(line, ',');
            if (row.size() < 8) continue;
            
            Customer c;
            c.id = row[0];
            c.lat = stod(row[1]);
            c.lon = stod(row[2]);
            c.weight = stod(row[3]);
            c.volume = stod(row[4]);
            c.service_time = stod(row[5]);
            c.tw_start = stod(row[6]);
            c.tw_end = stod(row[7]);
            
            inst.customers[c.id] = c;
        }
        
        // 2. Load Depots
        cout << "Loading depots..." << endl;
        ifstream file_depot(data_dir + "/depots.csv");
        getline(file_depot, line);
        while (getline(file_depot, line)) {
            vector<string> row = split(line, ',');
            if (row.size() < 4) continue;
            
            Depot d;
            d.id = row[0];
            d.lat = stod(row[1]);
            d.lon = stod(row[2]);
            d.capacity = stod(row[3]);
            
            inst.depots[d.id] = d;
        }
        
        // 3. Load Vehicles
        cout << "Loading vehicles..." << endl;
        ifstream file_veh(data_dir + "/vehicles.csv");
        getline(file_veh, line);
        while (getline(file_veh, line)) {
            vector<string> row = split(line, ',');
            if (row.size() < 8) continue;
            
            Vehicle v;
            v.id = row[0];
            v.start_depot = row[1];
            v.cap_weight = stod(row[2]);
            v.cap_volume = stod(row[3]);
            v.max_time = stod(row[4]);
            v.fixed_cost = stod(row[5]);
            v.var_cost = stod(row[6]);
            v.max_dist = stod(row[7]);
            
            inst.vehicles[v.id] = v;
        }
        
        // 4. Load Roads
        cout << "Loading roads..." << endl;
        ifstream file_road(data_dir + "/roads.csv");
        getline(file_road, line);
        while (getline(file_road, line)) {
            vector<string> row = split(line, ',');
            if (row.size() < 4) continue;
            
            string from = row[0];
            string to = row[1];
            double dist = stod(row[2]);
            double time = stod(row[3]);
            
            inst.matrix[from][to] = {dist, time};
        }
        
        cout << "Data loaded: " << inst.customers.size() << " customers, " 
             << inst.vehicles.size() << " vehicles." << endl;
             
        return inst;
    }
};
