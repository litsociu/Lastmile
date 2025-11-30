from __future__ import annotations
import sys
import os

# Add parent dir to path to allow importing phong package if run from inside
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from phong.config import load_data, build_instance
from phong.initialization import initialize_solution
from phong.algorithm import run_alns, run_tabu
from phong.objectives import evaluate
from phong.output import save_solution, print_solution_stats

def main():
    print(">>> STARTING OPTIMIZATION PIPELINE (PHONG) <<<")
    
    # 1. Load Data
    try:
        # Tự động tìm data ở thư mục cha hoặc hiện tại
        customers, depots, vehicles, roads = load_data()
    except FileNotFoundError as e:
        print(f"[ERROR] {e}")
        print("Vui lòng đảm bảo các file .xlsx (customers_clustered, depots, vehicles, roads) nằm đúng chỗ.")
        return

    # 2. Build Instance
    # Giả sử chạy cho depot prefix D001 như code cũ
    try:
        inst = build_instance("D001", customers, depots, vehicles, roads)
        print(f"[INFO] Instance built. Customers: {len(inst.customers)}, Vehicles: {len(inst.vehicles)}")
    except Exception as e:
        print(f"[ERROR] Build instance failed: {e}")
        return

    # 3. Initialization (Feasible)
    print("\n>>> INITIALIZATION (Greedy Feasible) ...")
    sol_init = initialize_solution(inst)
    evaluate(sol_init, inst)
    print(f"[INIT] Objective: {sol_init.objective:.2f}")
    print_solution_stats(sol_init)

    # 4. ALNS
    print("\n>>> RUNNING ALNS ...")
    sol_alns = run_alns(inst, sol_init, max_iter=50)
    print(f"[ALNS] Final Objective: {sol_alns.objective:.2f}")
    
    # 5. Tabu (Optional/Placeholder)
    print("\n>>> RUNNING TABU SEARCH ...")
    sol_tabu = run_tabu(inst, sol_alns, max_iter=10)
    print(f"[TABU] Final Objective: {sol_tabu.objective:.2f}")

    # 6. Output
    save_solution(sol_tabu, inst, output_dir="OUTPUT_PHONG")
    print("\n>>> DONE. <<<")

if __name__ == "__main__":
    main()
