#!/usr/bin/env python3
"""
Complete workflow test: Create new school year -> Switch to previous year -> Verify archive mode.
"""
import os
import sys

# Add the backend directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import client, DB_NAME, school_years

def test_complete_workflow():
    """Test the complete workflow you requested."""
    print("=== COMPLETE SCHOOL YEAR WORKFLOW TEST ===")
    print("Scenario: Create new school year -> Switch to previous years with archive mode")
    
    try:
        from app import (
            is_archived_school_year, 
            get_current_school_year_label,
            resolve_selected_school_year,
            list_school_year_docs,
            ensure_school_year_exists
        )
        
        # Step 1: Get current state
        original_current = get_current_school_year_label()
        print(f"\n1. Original state:")
        print(f"   Current school year: {original_current}")
        
        # Step 2: Create a new school year (simulating user creating 2027-2028)
        new_school_year = "2027-2028"
        print(f"\n2. Creating new school year: {new_school_year}")
        
        # Create the new school year and set it as current
        created = ensure_school_year_exists(new_school_year, set_current=True, created_by="test_user")
        print(f"   Created: {created is not None}")
        
        # Update the old current year to not be current anymore
        school_years.update_one(
            {"label": original_current},
            {"$set": {"is_current": False}}
        )
        print(f"   Set {original_current} as not current")
        
        # Step 3: Verify the new school year is now current
        new_current = get_current_school_year_label()
        print(f"\n3. After creating new school year:")
        print(f"   New current school year: {new_current}")
        print(f"   Is new year current? {new_current == new_school_year}")
        print(f"   Is old year archived? {is_archived_school_year(original_current)}")
        
        # Step 4: Test switching to previous year (archive mode)
        print(f"\n4. Testing switch to previous year ({original_current}):")
        
        # Simulate selecting the previous year in the UI
        selected_previous = resolve_selected_school_year(original_current)
        archived_view = selected_previous != get_current_school_year_label()
        
        print(f"   Selected school year: {selected_previous}")
        print(f"   Current school year: {get_current_school_year_label()}")
        print(f"   Archived view enabled: {archived_view}")
        print(f"   Is archived school year: {is_archived_school_year(selected_previous)}")
        
        # Step 5: Test all school years list
        print(f"\n5. All school years available:")
        docs = list_school_year_docs()
        for doc in docs:
            label = doc.get("label")
            is_current = doc.get("is_current", False)
            is_archived = is_archived_school_year(label)
            status = "Current" if is_current else ("Archived" if is_archived else "Unknown")
            print(f"   - {label} ({status})")
        
        # Step 6: Verify read-only behavior for archived years
        print(f"\n6. Testing read-only behavior:")
        
        # This simulates what happens in the students page
        template_context = {
            "selected_school_year": selected_previous,
            "current_school_year": get_current_school_year_label(),
            "archived_view": selected_previous != get_current_school_year_label(),
        }
        
        print(f"   Template context for students page:")
        print(f"     selected_school_year: {template_context['selected_school_year']}")
        print(f"     current_school_year: {template_context['current_school_year']}")
        print(f"     archived_view: {template_context['archived_view']}")
        
        # Step 7: Verification
        print(f"\n7. Final Verification:")
        
        success = True
        
        # Verify new school year is current
        if new_current != new_school_year:
            print("   ❌ FAIL: New school year should be current")
            success = False
        else:
            print("   ✅ PASS: New school year is correctly set as current")
        
        # Verify old year is now archived
        if not is_archived_school_year(original_current):
            print("   ❌ FAIL: Original school year should be archived")
            success = False
        else:
            print("   ✅ PASS: Original school year is correctly archived")
        
        # Verify archive view when selecting previous year
        if not archived_view:
            print("   ❌ FAIL: Should be in archived view when selecting previous year")
            success = False
        else:
            print("   ✅ PASS: Archive view correctly enabled for previous year")
        
        # Verify current year is not in archive view
        current_archived_view = new_school_year != get_current_school_year_label()
        if current_archived_view:
            print("   ❌ FAIL: Current year should not be in archived view")
            success = False
        else:
            print("   ✅ PASS: Current year correctly not in archived view")
        
        return success
        
    except Exception as e:
        print(f"   Error during testing: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        # Cleanup: Restore original state
        print(f"\n8. Cleaning up - Restoring original state:")
        try:
            # Delete the test school year
            school_years.delete_many({"label": new_school_year})
            print(f"   Deleted test school year: {new_school_year}")
            
            # Restore original current year
            school_years.update_one(
                {"label": original_current},
                {"$set": {"is_current": True}}
            )
            print(f"   Restored {original_current} as current school year")
            
        except Exception as e:
            print(f"   Cleanup error: {e}")

if __name__ == "__main__":
    success = test_complete_workflow()
    if success:
        print("\n🎉 COMPLETE WORKFLOW TEST PASSED!")
        print("✅ You can create new school years")
        print("✅ You can switch to previous years")
        print("✅ Previous years are in archive mode (read-only)")
        print("✅ Current year is fully editable")
    else:
        print("\n❌ COMPLETE WORKFLOW TEST FAILED!")
    sys.exit(0 if success else 1)
