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
-- @tags basic,where
-- @trace_level DEBUG
-- @trace_components QUERY,FILTER
SELECT * FROM employees WHERE id = 1;

-- @test name=count_all
-- @description Test COUNT(*) functionality
-- @expect_rows 1
-- @expect_columns count
-- @tags basic,aggregate
-- @trace_level INFO
-- @trace_components QUERY,AGGREGATE
SELECT COUNT(*) as count FROM employees;

-- @test name=department_group_by
-- @description Test GROUP BY with COUNT
-- @expect_rows 5
-- @expect_columns department
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
-- @tags basic,where,in
-- @trace_level DEBUG
-- @trace_components QUERY,FILTER
SELECT name, department
FROM employees
WHERE department IN ('Engineering', 'Sales', 'Marketing')
ORDER BY department, name;