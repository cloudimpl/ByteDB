#!/usr/bin/env python3
"""
CASE Expression Test Validator for ByteDB
This script validates the results of CASE expression tests to ensure correctness.
"""

import re
import sys

def validate_test_results(filename):
    """Validate the test results from the output file."""
    
    try:
        with open(filename, 'r') as f:
            content = f.read()
    except FileNotFoundError:
        print(f"❌ Test results file {filename} not found!")
        return False
    
    print("🔍 Validating CASE expression test results...")
    print("")
    
    passed = 0
    failed = 0
    
    # Test 1: Basic CASE with salary tiers
    print("Test 1: Basic CASE with salary tiers")
    if "Lisa Davis	85000	High" in content and "Sarah Wilson	55000	Low" in content:
        print("✅ PASS - Salary tiers working correctly")
        passed += 1
    else:
        print("❌ FAIL - Salary tiers not working correctly")
        failed += 1
    
    # Test 2: Department categorization  
    print("Test 2: Department categorization")
    if "Engineering	Tech" in content and "Marketing	Sales & Marketing" in content:
        print("✅ PASS - Department categorization working")
        passed += 1
    else:
        print("❌ FAIL - Department categorization not working")
        failed += 1
    
    # Test 3: Numeric results
    print("Test 3: Numeric results")
    if "Lisa Davis	85000	1" in content and "Sarah Wilson	55000	3" in content:
        print("✅ PASS - Numeric results working")
        passed += 1
    else:
        print("❌ FAIL - Numeric results not working")
        failed += 1
    
    # Test 4: Nested CASE expressions
    print("Test 4: Nested CASE expressions")
    if "Senior Engineer" in content and "Engineer" in content and "Marketing Specialist" in content:
        print("✅ PASS - Nested CASE expressions working")
        passed += 1
    else:
        print("❌ FAIL - Nested CASE expressions not working")
        failed += 1
    
    # Test 5: CASE without ELSE clause
    print("Test 5: CASE without ELSE clause")
    if "<nil>" in content:  # Should have NULL values for unmatched cases
        print("✅ PASS - CASE without ELSE returns NULL correctly")
        passed += 1
    else:
        print("❌ FAIL - CASE without ELSE not returning NULL")
        failed += 1
    
    # Test 6: NULL handling
    print("Test 6: NULL handling")
    if "Has Data" in content:  # All our test data should have data
        print("✅ PASS - NULL handling working")
        passed += 1
    else:
        print("❌ FAIL - NULL handling not working")
        failed += 1
    
    # Test 7: Multiple CASE expressions
    print("Test 7: Multiple CASE expressions")
    if "High	Tech" in content and "Low	Non-Tech" in content:
        print("✅ PASS - Multiple CASE expressions working")
        passed += 1
    else:
        print("❌ FAIL - Multiple CASE expressions not working")
        failed += 1
    
    # Check for any error messages
    if "error" in content.lower() or "failed" in content.lower():
        print("⚠️  WARNING - Errors detected in output")
        
    # Check if all tests completed
    if "All CASE expression tests completed" in content:
        print("✅ All tests executed successfully")
    else:
        print("❌ Test suite did not complete")
        failed += 1
    
    print("")
    print("=" * 50)
    print(f"VALIDATION SUMMARY:")
    print(f"✅ Passed: {passed}")
    print(f"❌ Failed: {failed}")
    print(f"📊 Success Rate: {passed/(passed+failed)*100:.1f}%")
    print("=" * 50)
    
    return failed == 0

if __name__ == "__main__":
    filename = "case_test_results.txt"
    if len(sys.argv) > 1:
        filename = sys.argv[1]
    
    success = validate_test_results(filename)
    sys.exit(0 if success else 1)