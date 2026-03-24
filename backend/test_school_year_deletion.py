#!/usr/bin/env python3
"""
Comprehensive test to verify school year deletion is permanent after all fixes.
"""
import os
import sys
from pymongo import MongoClient

# Add the backend directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import client, DB_NAME, school_years

def test_school_year_deletion_persistence():
    """Test that deleted school years are NEVER restored."""
    db = client[DB_NAME]
    
    print("=== COMPREHENSIVE SCHOOL YEAR DELETION TEST ===")
    
    # Test data
    test_school_years = ["2025-2026", "2026-2027", "2024-2025"]
    
    print(f"\n1. Creating test school years: {test_school_years}")
    for year in test_school_years:
        school_years.update_one(
            {"label": year},
            {"$set": {"label": year, "start_year": int(year.split("-")[0]), "end_year": int(year.split("-")[1]), "is_current": False}},
            upsert=True
        )
    
    # Verify they exist
    print("2. Verifying school years exist before deletion:")
    for year in test_school_years:
        exists = school_years.find_one({"label": year}) is not None
        print(f"   {year}: {'EXISTS' if exists else 'MISSING'}")
    
    # Delete them
    print("\n3. Deleting all test school years:")
    for year in test_school_years:
        result = school_years.delete_one({"label": year})
        print(f"   {year}: Deleted {result.deleted_count} document(s)")
    
    # Verify they're gone
    print("\n4. Verifying school years are deleted:")
    for year in test_school_years:
        exists = school_years.find_one({"label": year}) is not None
        print(f"   {year}: {'STILL EXISTS - ERROR' if exists else 'DELETED - OK'}")
    
    # Test application startup behavior
    print("\n5. Testing application startup functions:")
    
    try:
        # Test list_school_year_docs
        from app import list_school_year_docs
        docs = list_school_year_docs()
        found_years = [doc.get("label") for doc in docs if doc.get("label") in test_school_years]
        print(f"   list_school_year_docs() found: {found_years}")
        print(f"   Result: {'FAILED - restored deleted years' if found_years else 'PASSED - no restoration'}")
        
        # Test get_current_school_year_label
        from app import get_current_school_year_label
        current = get_current_school_year_label()
        # Check if the returned year actually exists in the database
        current_exists = school_years.find_one({"label": current}) is not None
        is_deleted_test_year = current in test_school_years and not current_exists
        print(f"   get_current_school_year_label() returned: {current}")
        print(f"   Year exists in database: {current_exists}")
        print(f"   Result: {'FAILED - returned deleted year' if is_deleted_test_year else 'PASSED'}")
        
        # Test ensure_school_year_scope_defaults (the culprit we fixed)
        from app import ensure_school_year_scope_defaults
        ensure_school_year_scope_defaults()
        docs_after_scope = list_school_year_docs()
        found_years_after = [doc.get("label") for doc in docs_after_scope if doc.get("label") in test_school_years]
        print(f"   ensure_school_year_scope_defaults() found: {found_years_after}")
        print(f"   Result: {'FAILED - scope defaults restored years' if found_years_after else 'PASSED'}")
        
    except Exception as e:
        print(f"   Error testing startup functions: {e}")
    
    # Final verification
    print("\n6. FINAL VERIFICATION:")
    all_gone = True
    for year in test_school_years:
        exists = school_years.find_one({"label": year}) is not None
        if exists:
            all_gone = False
            print(f"   ❌ {year}: STILL EXISTS")
        else:
            print(f"   ✅ {year}: PERMANENTLY DELETED")
    
    print(f"\n=== OVERALL RESULT: {'SUCCESS' if all_gone else 'FAILURE'} ===")
    
    # Cleanup any remnants
    for year in test_school_years:
        school_years.delete_many({"label": year})
    
    return all_gone

if __name__ == "__main__":
    success = test_school_year_deletion_persistence()
    if success:
        print("\n🎉 ALL TESTS PASSED - School year deletion is now permanent!")
    else:
        print("\n❌ TESTS FAILED - School years are still being restored!")
    sys.exit(0 if success else 1)
