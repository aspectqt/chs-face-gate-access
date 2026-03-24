#!/usr/bin/env python3
"""
Check Grade 11 sections and create them if missing.
"""
import os
import sys

# Add the backend directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import client, DB_NAME, sections

def check_and_create_grade11_sections():
    """Check and create Grade 11 sections for current school year."""
    print("=== CHECKING AND CREATING GRADE 11 SECTIONS ===")
    
    try:
        from app import get_current_school_year_label, ensure_predefined_sections
        
        current_year = get_current_school_year_label()
        print(f"Current school year: {current_year}")
        
        # Check existing Grade 11 sections
        grade11_sections = list(sections.find({
            "grade_level": "Grade 11",
            "school_year": current_year
        }).sort("section", 1))
        
        if grade11_sections:
            print(f"Grade 11 sections already exist ({len(grade11_sections)}):")
            for section in grade11_sections:
                print(f"  - {section.get('section')}")
        else:
            print("No Grade 11 sections found. Creating them...")
            ensure_predefined_sections(current_year, allow_create=True)
            
            # Check again after creation
            grade11_sections = list(sections.find({
                "grade_level": "Grade 11",
                "school_year": current_year
            }).sort("section", 1))
            
            if grade11_sections:
                print(f"✅ Created {len(grade11_sections)} Grade 11 sections:")
                for section in grade11_sections:
                    print(f"  - {section.get('section')} (ID: {section.get('_id')})")
            else:
                print("❌ Failed to create Grade 11 sections")
        
        return True
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    check_and_create_grade11_sections()
