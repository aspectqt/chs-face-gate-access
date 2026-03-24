#!/usr/bin/env python3
"""
Create missing Grade 12 sections for current school year.
"""
import os
import sys

# Add the backend directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import client, DB_NAME, sections

def create_missing_grade12_sections():
    """Create Grade 12 sections for current school year."""
    print("=== CREATING MISSING GRADE 12 SECTIONS ===")
    
    try:
        from app import get_current_school_year_label, ensure_predefined_sections
        
        current_year = get_current_school_year_label()
        print(f"Current school year: {current_year}")
        
        # Create Grade 12 sections for current school year
        print(f"\nCreating Grade 12 sections for {current_year}...")
        ensure_predefined_sections(current_year, allow_create=True)
        
        # Check if sections were created
        grade12_sections = list(sections.find({
            "grade_level": "Grade 12",
            "school_year": current_year
        }).sort("section", 1))
        
        if grade12_sections:
            print(f"✅ Success! Created {len(grade12_sections)} Grade 12 sections:")
            for section in grade12_sections:
                print(f"  - {section.get('section')} (ID: {section.get('_id')})")
        else:
            print("❌ Failed to create Grade 12 sections")
            return False
        
        return True
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = create_missing_grade12_sections()
    if success:
        print("\n🎉 Grade 12 sections created successfully!")
    else:
        print("\n❌ Failed to create Grade 12 sections")
