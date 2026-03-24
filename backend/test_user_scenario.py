#!/usr/bin/env python3
"""
Test that simulates the exact user scenario: delete from MongoDB and reopen.
"""
import os
import sys

# Add the backend directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import client, DB_NAME, school_years

def simulate_user_scenario():
    """Simulate: Delete school year from MongoDB -> Close app -> Reopen app"""
    print("=== SIMULATING USER SCENARIO ===")
    print("Scenario: Delete school year from MongoDB -> Restart application -> Check if it's gone")
    
    test_year = "2025-2026"
    
    print(f"\n1. Creating school year {test_year} in MongoDB...")
    school_years.update_one(
        {"label": test_year},
        {"$set": {"label": test_year, "start_year": 2025, "end_year": 2026, "is_current": True}},
        upsert=True
    )
    
    print("2. Verifying school year exists:")
    exists_before = school_years.find_one({"label": test_year}) is not None
    print(f"   {test_year} in MongoDB: {'YES' if exists_before else 'NO'}")
    
    print(f"\n3. Deleting school year {test_year} from MongoDB (like user did)...")
    result = school_years.delete_one({"label": test_year})
    print(f"   Deleted {result.deleted_count} document(s)")
    
    print("4. Verifying school year is deleted:")
    exists_after = school_years.find_one({"label": test_year}) is not None
    print(f"   {test_year} in MongoDB: {'YES - ERROR' if exists_after else 'NO - GOOD'}")
    
    print("\n5. Simulating application restart (importing and testing functions)...")
    
    try:
        # This simulates the application starting up
        from app import list_school_year_docs, get_current_school_year_label, ensure_school_year_scope_defaults
        
        print("   a) Checking list_school_year_docs()...")
        docs = list_school_year_docs()
        found = any(doc.get("label") == test_year for doc in docs)
        print(f"      Found {test_year}: {'YES - ERROR' if found else 'NO - GOOD'}")
        
        print("   b) Checking get_current_school_year_label()...")
        current = get_current_school_year_label()
        current_exists = school_years.find_one({"label": current}) is not None
        print(f"      Returned: {current}")
        print(f"      Exists in DB: {'YES' if current_exists else 'NO'}")
        if current == test_year and not current_exists:
            print(f"      Result: GOOD - Returns derived label but doesn't create it")
        elif current != test_year:
            print(f"      Result: GOOD - Returns different year")
        else:
            print(f"      Result: ERROR - Returns deleted year that exists")
        
        print("   c) Running ensure_school_year_scope_defaults()...")
        ensure_school_year_scope_defaults()
        docs_after = list_school_year_docs()
        found_after = any(doc.get("label") == test_year for doc in docs_after)
        print(f"      Found {test_year}: {'YES - ERROR' if found_after else 'NO - GOOD'}")
        
        print("   d) Final database check...")
        final_check = school_years.find_one({"label": test_year}) is not None
        print(f"      {test_year} in MongoDB: {'YES - ERROR' if final_check else 'NO - GOOD'}")
        
    except Exception as e:
        print(f"   Error during restart simulation: {e}")
        return False
    
    print("\n=== SCENARIO RESULT ===")
    success = not exists_after and not found and not found_after and not final_check
    print(f"School year deletion is permanent: {'YES - SUCCESS' if success else 'NO - FAILURE'}")
    
    return success

if __name__ == "__main__":
    success = simulate_user_scenario()
    if success:
        print("\n✅ USER ISSUE FIXED: Deleted school years stay deleted!")
    else:
        print("\n❌ USER ISSUE PERSISTS: School years are still being restored!")
    sys.exit(0 if success else 1)
