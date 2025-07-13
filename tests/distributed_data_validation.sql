-- Distributed SQL tests with data validation
-- Test exact data content validation

-- @test name=distributed_exact_data
-- @description Test exact data matching for distributed query
-- @workers 3
-- @expected_workers 3
-- @expect_rows 5
-- @expect_columns department,count
-- @expect_data [{"department": "Engineering", "count": 4}, {"department": "Finance", "count": 1}, {"department": "HR", "count": 1}, {"department": "Marketing", "count": 2}, {"department": "Sales", "count": 2}]
SELECT department, COUNT(*) as count
FROM employees
GROUP BY department
ORDER BY department;

-- @test name=distributed_aggregation_values
-- @description Verify distributed aggregation produces correct values
-- @workers 3
-- @network_optimization
-- @verify_partial_aggs
-- @expect_rows 1
-- @expect_columns total_salary,avg_salary,min_salary,max_salary
-- @expect_data [{"total_salary": 710000, "avg_salary": 71000, "min_salary": 55000, "max_salary": 85000}]
SELECT 
    SUM(salary) as total_salary,
    AVG(salary) as avg_salary,
    MIN(salary) as min_salary,
    MAX(salary) as max_salary
FROM employees;

-- @test name=distributed_filtered_data
-- @description Test filtered query with specific data validation
-- @workers 3
-- @expect_rows 6
-- @expect_columns name,department,salary
-- @expect_data [{"name": "David Brown", "department": "Sales", "salary": 70000}, {"name": "Maria Rodriguez", "department": "Sales", "salary": 72000}, {"name": "John Doe", "department": "Engineering", "salary": 75000}, {"name": "Chris Anderson", "department": "Engineering", "salary": 78000}, {"name": "Mike Johnson", "department": "Engineering", "salary": 80000}, {"name": "Lisa Davis", "department": "Engineering", "salary": 85000}]
SELECT name, department, salary
FROM employees
WHERE salary >= 70000
ORDER BY salary, name;

-- @test name=distributed_top_salaries
-- @description Get top 3 salaries across distributed data
-- @workers 3
-- @expect_rows 3
-- @expect_columns name,department,salary
-- @expect_data [{"name": "Sarah Wilson", "department": "Engineering", "salary": 95000}, {"name": "Jane Smith", "department": "Engineering", "salary": 80000}, {"name": "John Doe", "department": "Engineering", "salary": 75000}]
SELECT name, department, salary
FROM employees
ORDER BY salary DESC
LIMIT 3;

-- @test name=distributed_department_stats
-- @description Verify department statistics with exact values
-- @workers 3
-- @network_optimization
-- @expect_rows 5
-- @expect_columns department,emp_count,avg_salary,max_salary
-- @expect_data [{"department": "Engineering", "emp_count": 4, "avg_salary": 72500, "max_salary": 95000}, {"department": "Finance", "emp_count": 1, "avg_salary": 55000, "max_salary": 55000}, {"department": "HR", "emp_count": 2, "avg_salary": 47500, "max_salary": 50000}, {"department": "Marketing", "emp_count": 2, "avg_salary": 57500, "max_salary": 60000}, {"department": "Sales", "emp_count": 1, "avg_salary": 70000, "max_salary": 70000}]
SELECT 
    department,
    COUNT(*) as emp_count,
    AVG(salary) as avg_salary,
    MAX(salary) as max_salary
FROM employees
GROUP BY department
ORDER BY department;

-- @test name=distributed_specific_employee
-- @description Test fetching specific employee data
-- @workers 3
-- @expect_rows 1
-- @expect_columns id,name,email,department,salary
-- @expect_data [{"id": 1, "name": "John Doe", "email": "john@example.com", "department": "Engineering", "salary": 75000}]
SELECT id, name, email, department, salary
FROM employees
WHERE id = 1;

-- @test name=distributed_empty_result
-- @description Test query with no matching data
-- @workers 3
-- @expect_rows 0
-- @expect_columns name,department,salary
-- @expect_data []
SELECT name, department, salary
FROM employees
WHERE salary > 100000;

-- @test name=distributed_department_count_only
-- @description Test simple count per department
-- @workers 3
-- @network_optimization
-- @verify_partial_aggs
-- @expect_rows 5
-- @expect_columns department,total
-- @expect_data [{"department": "Engineering", "total": 4}, {"department": "Finance", "total": 1}, {"department": "HR", "total": 2}, {"department": "Marketing", "total": 2}, {"department": "Sales", "total": 1}]
SELECT department, COUNT(*) as total
FROM employees
GROUP BY department
ORDER BY department;