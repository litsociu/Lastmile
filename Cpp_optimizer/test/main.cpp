#include "config.h"
#include "data_loader.h"
#include "initialization.h"
#include "algorithm.h"
#include <fstream>

using namespace std;

void save_solution(const Solution& sol, const string& output_file) {
    ofstream out(output_file);
    out << "Vehicle_ID,Trip_ID,Depot_ID,Cluster_ID,Centroid_ID,Load_Weight,Load_Volume,Distance" << endl;
    
    for (auto& p : sol.routes) {
        string vid = p.first;
        int trip_idx = 1;
        for (auto& r : p.second) {
            out << vid << "," 
                << trip_idx++ << ","
                << r.stops[0] << ","
                << r.cluster_id << ","
                << r.centroid_id << ","
                << r.load_w << ","
                << r.load_v << ","
                << "0" // Distance placeholder
                << endl;
        }
    }
    cout << "Saved solution to " << output_file << endl;
}

int main() {
    string data_dir = "data"; // Relative to executable
    
    // 1. Load Data
    Instance inst = DataLoader::load_instance(data_dir);
    
    // 2. Initialize
    cout << ">>> Initializing Solution..." << endl;
    Solution sol_init = Initializer::create_initial_solution(inst);
    sol_init.calculate_objective(inst);
    cout << "Initial Objective: " << sol_init.objective << endl;
    
    // 3. Optimize
    cout << ">>> Running ALNS..." << endl;
    Solution sol_final = ALNS::run(inst, sol_init, 50);
    
    // 4. Save
    save_solution(sol_final, "result_routes.csv");
    
    return 0;
}
