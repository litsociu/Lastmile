# run_hcm.py
from __future__ import annotations
import traceback

from data_io_hcm import load_data
from instance_builder import build_instance_for_depot_prefix
from optimizer_algorithms import example_run_alns, example_run_tabu


def main():
    try:
        customers_df, depots_df, vehicles_df, roads_df = load_data()
        inst = build_instance_for_depot_prefix("D001", customers_df, depots_df, vehicles_df, roads_df)

        print(">>> ALNS D001")
        sol_alns = example_run_alns(inst)
        print(">>> DONE ALNS:", sol_alns.objective)

        print("\n>>> TABU D001")
        sol_tabu = example_run_tabu(inst, sol_alns)
        print(">>> DONE TABU:", sol_tabu.objective)
    except Exception:
        traceback.print_exc()


if __name__ == "__main__":
    main()
