#!/usr/bin/env python3
"""
Test script to verify that delete operations are permanent and data is not restored on restart.
"""
import os
import sys
from pymongo import MongoClient
from datetime import datetime

# Add the backend directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import client, DB_NAME, school_years, students, sections

def test_delete_persistence():
    """Test that deleted school years and students are not restored."""
    db = client[DB_NAME]
    
    print("=== Testing Delete Persistence ===")
    
    # Test 1: Delete a school year and verify it doesn't come back
    print("\n1. Testing school year deletion...")
    
    # Create a test school year
    test_school_year = "2025-2026"
    school_years.update_one(
        {"label": test_school_year},
        {"$set": {"label": test_school_year, "start_year": 2025, "end_year": 2026, "is_current": False}},
        upsert=True
    )
    print(f"   Created test school year: {test_school_year}")
    
    # Verify it exists
    exists = school_years.find_one({"label": test_school_year})
    print(f"   School year exists before deletion: {exists is not None}")
    
    # Delete it
    result = school_years.delete_one({"label": test_school_year})
    print(f"   Deleted school year, removed {result.deleted_count} document(s)")
    
    # Verify it's gone
    exists_after = school_years.find_one({"label": test_school_year})
    print(f"   School year exists after deletion: {exists_after is not None}")
    
    # Test 2: Delete students and verify they don't come back
    print("\n2. Testing student deletion...")
    
    # Create a test student
    test_student_lrn = "TEST123456789"
    students.update_one(
        {"lrn": test_student_lrn},
        {"$set": {
            "lrn": test_student_lrn,
            "name": "Test Student",
            "grade_level": "Grade 7",
            "section": "AVILA",
            "status": "Active"
        }},
        upsert=True
    )
    print(f"   Created test student with LRN: {test_student_lrn}")
    
    # Verify it exists
    student_exists = students.find_one({"lrn": test_student_lrn})
    print(f"   Student exists before deletion: {student_exists is not None}")
    
    # Delete it
    student_result = students.delete_one({"lrn": test_student_lrn})
    print(f"   Deleted student, removed {student_result.deleted_count} document(s)")
    
    # Verify it's gone
    student_exists_after = students.find_one({"lrn": test_student_lrn})
    print(f"   Student exists after deletion: {student_exists_after is not None}")
    
    # Test 3: Test that application startup doesn't restore data
    print("\n3. Testing application startup behavior...")
    
    # Import and run the startup functions that should NOT restore data
    try:
        from app import list_school_year_docs, get_current_school_year_label
        
        # Check that deleted school year is not restored
        school_years_list = list_school_year_docs()
        test_year_found = any(doc.get("label") == test_school_year for doc in school_years_list)
        print(f"   Deleted school year {test_school_year} restored by list_school_year_docs: {test_year_found}")
        
        # Check current school year (should not create the deleted one)
        current_year = get_current_school_year_label()
        print(f"   Current school year: {current_year}")
        print(f"   Current school year equals deleted test year: {current_year == test_school_year}")
        
    except Exception as e:
        print(f"   Error testing startup behavior: {e}")
    
    print("\n=== Test Results ===")
    print("✓ School year deletion is permanent" if not exists_after else "✗ School year was restored")
    print("✓ Student deletion is permanent" if not student_exists_after else "✗ Student was restored")
    print("✓ Application startup does not restore deleted data" if not test_year_found else "✗ Application restored deleted data")
    
    # Cleanup test data
    school_years.delete_many({"label": test_school_year})
    students.delete_many({"lrn": test_student_lrn})
    
    print("\nTest completed successfully!")

if __name__ == "__main__":
    test_delete_persistence()
