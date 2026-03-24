#!/usr/bin/env python3
"""
Final verification: Check all grade levels have sections in current school year.
"""
import os
import sys

# Add the backend directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import client, DB_NAME, sections

def verify_all_sections():
    """Verify all grade levels have sections in current school year."""
    print("=== FINAL VERIFICATION: ALL SECTIONS ===")
    
    try:
        from app import get_current_school_year_label
        
        current_year = get_current_school_year_label()
        print(f"Current school year: {current_year}")
        
        # Expected grade levels
        expected_grades = ["Grade 7", "Grade 8", "Grade 9", "Grade 10", "Grade 11", "Grade 12"]
        
        print(f"\nChecking sections for all grade levels:")
        
        all_good = True
        
        for grade in expected_grades:
            grade_sections = list(sections.find({
                "grade_level": grade,
                "school_year": current_year
            }).sort("section", 1))
            
            if grade_sections:
                print(f"✅ {grade}: {len(grade_sections)} sections")
                for section in grade_sections:
                    print(f"     - {section.get('section')}")
            else:
                print(f"❌ {grade}: NO SECTIONS FOUND")
                all_good = False
        
        print(f"\n=== SUMMARY ===")
        if all_good:
            print("🎉 ALL GRADE LEVELS HAVE SECTIONS IN CURRENT SCHOOL YEAR!")
            print("✅ Grade 7-12 sections are properly configured")
            print("✅ Students can now be enrolled in all grade levels")
        else:
            print("❌ SOME GRADE LEVELS ARE MISSING SECTIONS")
        
        return all_good
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = verify_all_sections()
    sys.exit(0 if success else 1)
