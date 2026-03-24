#!/usr/bin/env python3
"""
Test school year switching and archive functionality.
"""
import os
import sys

# Add the backend directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import client, DB_NAME, school_years

def test_school_year_switching():
    """Test creating new school year and switching to previous years with archive mode."""
    print("=== TESTING SCHOOL YEAR SWITCHING & ARCHIVE ===")
    
    try:
        from app import (
            is_archived_school_year, 
            get_current_school_year_label,
            resolve_selected_school_year,
            list_school_year_docs
        )
        
        # Get the actual current school year from the database
        current_year = get_current_school_year_label()
        print(f"   Current school year in database: {current_year}")
        
        # Create a test previous year (1 year back)
        current_start = int(current_year.split("-")[0])
        current_end = int(current_year.split("-")[1])
        previous_year = f"{current_start - 1}-{current_end - 1}"
        
        print(f"\n1. Setting up test school years...")
        print(f"   Using existing current school year: {current_year}")
        
        # Create previous school year
        school_years.update_one(
            {"label": previous_year},
            {"$set": {"label": previous_year, "start_year": current_start - 1, "end_year": current_start, "is_current": False}},
            upsert=True
        )
        print(f"   Created previous school year: {previous_year}")
        
        print("\n2. Testing archive functionality...")
        
        # Test is_archived_school_year function
        current_is_archived = is_archived_school_year(current_year)
        previous_is_archived = is_archived_school_year(previous_year)
        
        print(f"   is_archived_school_year('{current_year}'): {current_is_archived}")
        print(f"   is_archived_school_year('{previous_year}'): {previous_is_archived}")
        
        # Test get_current_school_year_label
        current_label = get_current_school_year_label()
        print(f"   get_current_school_year_label(): {current_label}")
        
        # Test resolve_selected_school_year
        resolved_current = resolve_selected_school_year(current_year)
        resolved_previous = resolve_selected_school_year(previous_year)
        
        print(f"   resolve_selected_school_year('{current_year}'): {resolved_current}")
        print(f"   resolve_selected_school_year('{previous_year}'): {resolved_previous}")
        
        # Test list_school_year_docs
        docs = list_school_year_docs()
        print(f"   Found {len(docs)} school years:")
        for doc in docs:
            is_current = doc.get("is_current", False)
            label = doc.get("label")
            archived = is_archived_school_year(label)
            print(f"     - {label} ({'Current' if is_current else 'Archived' if archived else 'Unknown'})")
        
        print("\n3. Testing students page context...")
        
        # Simulate students_page context for current year
        selected_current = current_year
        archived_view_current = selected_current != get_current_school_year_label()
        print(f"   Selected: {selected_current}")
        print(f"   Current: {get_current_school_year_label()}")
        print(f"   Archived view: {archived_view_current}")
        
        # Simulate students_page context for previous year
        selected_previous = previous_year
        archived_view_previous = selected_previous != get_current_school_year_label()
        print(f"   Selected: {selected_previous}")
        print(f"   Current: {get_current_school_year_label()}")
        print(f"   Archived view: {archived_view_previous}")
        
        # Verify results
        print("\n4. Verification:")
        
        success = True
        
        if current_is_archived:
            print("   ❌ FAIL: Current school year should not be archived")
            success = False
        else:
            print("   ✅ PASS: Current school year is not archived")
            
        if not previous_is_archived:
            print("   ❌ FAIL: Previous school year should be archived")
            success = False
        else:
            print("   ✅ PASS: Previous school year is archived")
            
        if archived_view_current:
            print("   ❌ FAIL: Current year should not be in archived view")
            success = False
        else:
            print("   ✅ PASS: Current year is not in archived view")
            
        if not archived_view_previous:
            print("   ❌ FAIL: Previous year should be in archived view")
            success = False
        else:
            print("   ✅ PASS: Previous year is in archived view")
        
        return success
        
    except Exception as e:
        print(f"   Error during testing: {e}")
        return False
    
    finally:
        # Cleanup
        print("\n5. Cleaning up test data...")
        if 'previous_year' in locals():
            school_years.delete_many({"label": previous_year})
        print("   Test data cleaned up")

if __name__ == "__main__":
    success = test_school_year_switching()
    if success:
        print("\n🎉 SCHOOL YEAR SWITCHING & ARCHIVE WORKING CORRECTLY!")
    else:
        print("\n❌ SCHOOL YEAR SWITCHING & ARCHIVE HAVE ISSUES!")
    sys.exit(0 if success else 1)
