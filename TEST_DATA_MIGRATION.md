# Test Data Migration Guide

## Problem
Previously, tests were using `generateSampleData()` which could change over time, causing tests to break when data generation was updated. This made tests non-deterministic and fragile.

## Solution
We've implemented a fixed test data system that ensures tests always use the same data.

### New Structure

1. **test_data.go** - Contains fixed test data arrays that never change
   - `TestEmployees` - 10 fixed employee records
   - `TestProducts` - 10 fixed product records  
   - `TestDepartments` - 6 fixed department records
   - Constants documenting key facts about the test data

2. **test_helpers.go** - Helper functions for test data setup
   - `SetupTestData()` - Creates test data in ./testdata directory
   - `CleanupTestData()` - Removes test data
   - `NewTestQueryEngine()` - Creates QueryEngine for tests

3. **test_base.go** - TestMain function that runs before/after all tests
   - Automatically sets up test data before tests run
   - Cleans up after tests complete

### Migration Steps

To migrate existing tests:

1. **Replace data directory references**
   ```go
   // Old
   engine := NewQueryEngine("./data")
   
   // New
   engine := NewTestQueryEngine()
   ```

2. **Remove generateSampleData() calls**
   ```go
   // Old
   generateSampleData()
   engine := NewQueryEngine("./data")
   
   // New (no generateSampleData needed)
   engine := NewTestQueryEngine()
   ```

3. **Use test data constants**
   ```go
   // Instead of hardcoding expectations
   if len(result.Rows) != 7 { // Magic number!
   
   // Use documented constants
   if len(result.Rows) != TestEmployeesOver70k {
   ```

### Test Data Facts

Key facts about the fixed test data (see test_data.go for complete list):

**Employees:**
- Total: 10 employees
- Engineering: 4 (John, Mike, Lisa, Chris)
- Salary > 70k: 7 employees
- Salary > 80k: 2 employees (Lisa, Mike)
- Min salary: 55,000 (Sarah)
- Max salary: 85,000 (Lisa)

**Products:**
- Total: 10 products
- Price > 100: 3 products (Laptop, Monitor, Phone)
- Price > 500: 2 products (Laptop, Phone)
- Exact prices: Mouse (29.99), Laptop (999.99)

**Departments:**
- Total: 6 departments
- All have budget > 200k
- 4 have budget > 500k
- Research has 0 employees

### Benefits

1. **Deterministic** - Tests always use the same data
2. **Fast** - No need to regenerate data for each test
3. **Documented** - Constants document the test data characteristics
4. **Isolated** - Test data in separate directory from production data
5. **Maintainable** - Single source of truth for test data

### Example Test

```go
func TestEmployeeSalaries(t *testing.T) {
    engine := NewTestQueryEngine()
    defer engine.Close()
    
    result, err := engine.Execute("SELECT name FROM employees WHERE salary > 70000")
    if err != nil {
        t.Fatalf("Query failed: %v", err)
    }
    
    // Use constant instead of magic number
    if len(result.Rows) != TestEmployeesOver70k {
        t.Errorf("Expected %d employees with salary > 70k, got %d", 
            TestEmployeesOver70k, len(result.Rows))
    }
}
```