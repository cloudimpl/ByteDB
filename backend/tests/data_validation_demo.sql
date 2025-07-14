-- @test name=count_validation
-- @description Test COUNT with exact data validation
-- @expect_rows 1
-- @expect_columns total
-- @expect_data [{"total": 10}]
SELECT COUNT(*) as total FROM employees;

-- @test name=specific_employee_validation
-- @description Test specific employee lookup with data validation
-- @expect_rows 1
-- @expect_columns id,name,department
-- @expect_data [{"id": 1, "name": "John Doe", "department": "Engineering"}]
SELECT id, name, department FROM employees WHERE id = 1;

-- @test name=department_count_validation
-- @description Test department counts with exact data validation
-- @expect_rows 5
-- @expect_columns department,count
-- @expect_data [{"department": "Engineering", "count": 4}, {"department": "Finance", "count": 1}, {"department": "HR", "count": 1}, {"department": "Marketing", "count": 2}, {"department": "Sales", "count": 2}]
SELECT department, COUNT(*) as count
FROM employees
GROUP BY department
ORDER BY department;