#!/usr/bin/env python3
"""
Check what Grade 12 sections exist in the database.
"""
import os
import sys

# Add the backend directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import client, DB_NAME, sections, school_years

def check_grade12_sections():
    """Check Grade 12 sections in current and previous school years."""
    print("=== CHECKING GRADE 12 SECTIONS ===")
    
    try:
        from app import get_current_school_year_label, list_school_year_docs
        
        current_year = get_current_school_year_label()
        print(f"\nCurrent school year: {current_year}")
        
        # Get all school years
        school_years_list = list_school_year_docs()
        print(f"\nFound {len(school_years_list)} school years:")
        
        for sy_doc in school_years_list:
            sy_label = sy_doc.get("label")
            is_current = sy_doc.get("is_current", False)
            print(f"\n--- School Year: {sy_label} ({'Current' if is_current else 'Previous'}) ---")
            
            # Check Grade 12 sections for this school year
            grade12_sections = list(sections.find({
                "grade_level": "Grade 12",
                "school_year": sy_label
            }).sort("section", 1))
            
            if grade12_sections:
                print(f"Grade 12 sections found ({len(grade12_sections)}):")
                for section in grade12_sections:
                    print(f"  - {section.get('section')} (ID: {section.get('_id')})")
            else:
                print("No Grade 12 sections found")
        
        # Check all Grade 12 sections regardless of school year
        print(f"\n--- ALL Grade 12 SECTIONS IN DATABASE ---")
        all_grade12 = list(sections.find({"grade_level": "Grade 12"}).sort("school_year", 1))
        
        if all_grade12:
            print(f"Total Grade 12 sections: {len(all_grade12)}")
            for section in all_grade12:
                print(f"  - {section.get('section')} | {section.get('school_year')} | ID: {section.get('_id')}")
        else:
            print("No Grade 12 sections found in entire database")
        
        return True
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    check_grade12_sections()
