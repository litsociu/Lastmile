"""
Quick validation script để test toàn bộ phong pipeline.
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def test_imports():
    """Test all imports work correctly"""
    print("="*60)
    print("TEST 1: Checking module imports...")
    print("="*60)
    try:
        from phong import config, initialization, objectives, constraints, algorithm, output, utils
        print("✓ All modules imported successfully")
        return True
    except Exception as e:
        print(f"✗ Import failed: {e}")
        return False

def test_data_loading():
    """Test data loading"""
    print("\n" + "="*60)
    print("TEST 2: Data loading and validation...")
    print("="*60)
    try:
        from phong.config import load_data
        customers, depots, vehicles, roads = load_data()
        print(f"\n✓ Data loaded successfully!")
        print(f"  - Customers: {len(customers)} rows")
        print(f"  - Depots: {len(depots)} rows")
        print(f"  - Vehicles: {len(vehicles)} rows")
        print(f"  - Roads: {len(roads)} rows")
        return True, (customers, depots, vehicles, roads)
    except Exception as e:
        print(f"✗ Data loading failed: {e}")
        import traceback
        traceback.print_exc()
        return False, None

def test_instance_building(data):
    """Test instance building"""
    print("\n" + "="*60)
    print("TEST 3: Building instance...")
    print("="*60)
    try:
        from phong.config import build_instance
        customers, depots, vehicles, roads = data
        inst = build_instance("D001", customers, depots, vehicles, roads)
        print(f"✓ Instance built successfully!")
        print(f"  - Customers in instance: {len(inst.customers)}")
        print(f"  - Vehicles: {len(inst.vehicles)}")
        print(f"  - Depots: {len(set(inst.depots.values()))}")
        return True, inst
    except Exception as e:
        print(f"✗ Instance building failed: {e}")
        import traceback
        traceback.print_exc()
        return False, None

def test_initialization(inst):
    """Test initialization"""
    print("\n" + "="*60)
    print("TEST 4: Testing initialization...")
    print("="*60)
    try:
        from phong.initialization import initialize_solution
        from phong.objectives import evaluate
        sol = initialize_solution(inst, rng_seed=42)
        evaluate(sol, inst)
        print(f"✓ Initialization successful!")
        print(f"  - Initial objective: {sol.objective:.2f}")
        print(f"  - Routes created: {sum(1 for r in sol.routes.values() if len(r.stops) > 2)}")
        return True
    except Exception as e:
        print(f"✗ Initialization failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    print("\n" + "#"*60)
    print("# PHONG MODULE VALIDATION TEST")
    print("#"*60 + "\n")
    
    # Test 1: Imports
    if not test_imports():
        print("\n❌ Test suite FAILED at import stage")
        return
    
    # Test 2: Data loading  
    success, data = test_data_loading()
    if not success:
        print("\n❌ Test suite FAILED at data loading stage")
        return
    
    # Test 3: Instance building
    success, inst = test_instance_building(data)
    if not success:
        print("\n❌ Test suite FAILED at instance building stage")
        return
    
    # Test 4: Initialization
    if not test_initialization(inst):
        print("\n❌ Test suite FAILED at initialization stage")
        return
    
    print("\n" + "="*60)
    print("✅ ALL TESTS PASSED!")
    print("="*60)
    print("\nPhong module is stable and ready to use.")
    print("You can now run 'python -m phong.main' for full optimization.")

if __name__ == "__main__":
    main()
