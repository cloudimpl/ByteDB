-- @test name=simple_select
-- @description Test basic SELECT query functionality
-- @expect_rows 10
-- @expect_columns age,department,hire_date,id,name,salary
-- @tags basic,select
-- @trace_level INFO
-- @trace_components QUERY,EXECUTION
SELECT * FROM employees;

-- @test name=filtered_select
-- @description Test SELECT with WHERE clause
-- @expect_rows 1
-- @expect_columns age,department,hire_date,id,name,salary
-- @expect_data [{"age": 30, "department": "Engineering", "hire_date": "2020-01-15", "id": 1, "name": "John Doe", "salary": 75000}]
-- @tags basic,where
-- @trace_level DEBUG
-- @trace_components QUERY,FILTER
SELECT * FROM employees WHERE id = 1;

-- @test name=count_all
-- @description Test COUNT(*) functionality
-- @expect_rows 1
-- @expect_columns count
-- @expect_data [{"count": 10}]
-- @tags basic,aggregate
-- @trace_level INFO
-- @trace_components QUERY,AGGREGATE
SELECT COUNT(*) as count FROM employees;

-- @test name=department_group_by
-- @description Test GROUP BY with COUNT
-- @expect_rows 5
-- @expect_columns department,count
-- @expect_data [{"department": "Engineering", "count": 4}, {"department": "Finance", "count": 1}, {"department": "HR", "count": 1}, {"department": "Marketing", "count": 2}, {"department": "Sales", "count": 2}]
-- @tags basic,group_by,aggregate
-- @trace_level DEBUG
-- @trace_components QUERY,AGGREGATE,EXECUTION
SELECT department, COUNT(*) as count 
FROM employees 
GROUP BY department 
ORDER BY department;

-- @test name=salary_filter
-- @description Test filtering by salary range
-- @expect_rows 7
-- @expect_columns name,salary
-- @expect_data [{"name": "Sarah Wilson", "salary": 95000}, {"name": "Jane Smith", "salary": 80000}, {"name": "John Doe", "salary": 75000}, {"name": "Mike Johnson", "salary": 70000}, {"name": "Jennifer Davis", "salary": 68000}, {"name": "David Lee", "salary": 67000}, {"name": "Jessica Martinez", "salary": 66000}]
-- @tags basic,where,numeric
-- @trace_level INFO
-- @trace_components QUERY,FILTER
SELECT name, salary 
FROM employees 
WHERE salary > 65000
ORDER BY salary DESC;

-- @test name=string_functions
-- @description Test basic string functions
-- @expect_rows 10
-- @expect_columns name,upper_name,department_length
-- @tags basic,functions,string
-- @trace_level DEBUG
-- @trace_components QUERY,EXECUTION
SELECT name, 
       UPPER(name) as upper_name,
       LENGTH(department) as department_length
FROM employees
ORDER BY name;

-- @test name=department_join
-- @description Test INNER JOIN between employees and departments
-- @expect_rows 10
-- @tags basic,join
-- @trace_level DEBUG
-- @trace_components QUERY,JOIN,EXECUTION
SELECT e.name, e.salary, d.budget
FROM employees e
JOIN departments d ON e.department = d.name
ORDER BY e.name;

-- @test name=arithmetic_expressions
-- @description Test arithmetic operations in SELECT
-- @expect_rows 10
-- @expect_columns name,salary,bonus,total_compensation
-- @tags basic,arithmetic
-- @trace_level INFO
-- @trace_components QUERY,EXECUTION
SELECT name, 
       salary,
       salary * 0.1 as bonus,
       salary + (salary * 0.1) as total_compensation
FROM employees
ORDER BY salary DESC;

-- @test name=between_operator
-- @description Test BETWEEN operator
-- @expect_rows 8
-- @expect_columns name,salary
-- @expect_data [{"name": "Emily Brown", "salary": 60000}, {"name": "Jessica Martinez", "salary": 66000}, {"name": "David Lee", "salary": 67000}, {"name": "Jennifer Davis", "salary": 68000}, {"name": "Mike Johnson", "salary": 70000}, {"name": "John Doe", "salary": 75000}, {"name": "Kevin Chen", "salary": 76000}, {"name": "Jane Smith", "salary": 80000}]
-- @tags basic,where,between
-- @trace_level DEBUG
-- @trace_components QUERY,FILTER
SELECT name, salary
FROM employees
WHERE salary BETWEEN 60000 AND 80000
ORDER BY salary;

-- @test name=in_operator
-- @description Test IN operator with department filter
-- @expect_rows 8
-- @expect_columns name,department
-- @expect_data [{"name": "David Lee", "department": "Engineering"}, {"name": "Jane Smith", "department": "Engineering"}, {"name": "John Doe", "department": "Engineering"}, {"name": "Sarah Wilson", "department": "Engineering"}, {"name": "Emily Brown", "department": "Marketing"}, {"name": "Kevin Chen", "department": "Marketing"}, {"name": "Jessica Martinez", "department": "Sales"}, {"name": "Mike Johnson", "department": "Sales"}]
-- @tags basic,where,in
-- @trace_level DEBUG
-- @trace_components QUERY,FILTER
SELECT name, department
FROM employees
WHERE department IN ('Engineering', 'Sales', 'Marketing')
ORDER BY department, name;