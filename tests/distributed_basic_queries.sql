-- @test name=distributed_simple_scan
-- @description Test basic distributed scan across all workers
-- @expect_rows 10
-- @expect_columns age,department,hire_date,id,name,salary
-- @tags distributed,basic,scan
-- @workers 3
-- @expected_workers 3
-- @trace_level INFO
-- @trace_components QUERY,EXECUTION,DISTRIBUTED
SELECT * FROM employees;

-- @test name=distributed_count_aggregation
-- @description Test distributed COUNT(*) with network optimization
-- @expect_rows 1
-- @expect_columns total_count
-- @tags distributed,aggregate,optimization
-- @workers 3
-- @expected_workers 3
-- @network_optimization
-- @verify_partial_aggs
-- @trace_level DEBUG
-- @trace_components AGGREGATE,DISTRIBUTED,OPTIMIZER
SELECT COUNT(*) as total_count FROM employees;

-- @test name=distributed_filtered_query
-- @description Test distributed query with WHERE clause predicate pushdown
-- @expect_rows 4
-- @tags distributed,filter,pushdown
-- @workers 3
-- @expected_workers 3
-- @trace_level DEBUG
-- @trace_components QUERY,FILTER,DISTRIBUTED
SELECT name, salary, department 
FROM employees 
WHERE department = 'Engineering'
ORDER BY salary DESC;

-- @test name=distributed_group_by
-- @description Test distributed GROUP BY with partial aggregation
-- @expect_rows 5
-- @expect_columns department,employee_count
-- @tags distributed,group_by,aggregate,optimization
-- @workers 3
-- @expected_workers 3
-- @network_optimization
-- @verify_partial_aggs
-- @trace_level DEBUG
-- @trace_components AGGREGATE,DISTRIBUTED,OPTIMIZER
SELECT department, COUNT(*) as employee_count
FROM employees
GROUP BY department
ORDER BY department;

-- @test name=distributed_complex_aggregation
-- @description Test multiple aggregate functions with distributed execution
-- @expect_rows 5
-- @expect_columns department,count,avg_salary,max_salary,min_salary
-- @tags distributed,aggregate,complex
-- @workers 3
-- @expected_workers 3
-- @network_optimization
-- @verify_partial_aggs
-- @trace_level DEBUG
-- @trace_components AGGREGATE,DISTRIBUTED,OPTIMIZER
SELECT 
    department,
    COUNT(*) as count,
    AVG(salary) as avg_salary,
    MAX(salary) as max_salary,
    MIN(salary) as min_salary
FROM employees
GROUP BY department
ORDER BY avg_salary DESC;

-- @test name=distributed_filtered_aggregation
-- @description Test distributed aggregation with WHERE clause
-- @expect_rows 2
-- @tags distributed,aggregate,filter
-- @workers 3
-- @expected_workers 1
-- @trace_level VERBOSE
-- @trace_components AGGREGATE,FILTER,DISTRIBUTED,OPTIMIZER
SELECT 
    department,
    COUNT(*) as high_earners,
    AVG(salary) as avg_high_salary
FROM employees
WHERE salary > 70000
GROUP BY department
ORDER BY high_earners DESC;

-- @test name=distributed_join_simple
-- @description Test distributed JOIN between employees and departments
-- @expect_rows 10
-- @tags distributed,join,basic
-- @workers 3
-- @trace_level DEBUG
-- @trace_components JOIN,DISTRIBUTED,EXECUTION
SELECT 
    e.name,
    e.salary,
    d.budget
FROM employees e
JOIN departments d ON e.department = d.name
ORDER BY e.name;

-- @test name=distributed_arithmetic_expressions
-- @description Test arithmetic operations in distributed query
-- @expect_rows 10
-- @expect_columns name,salary,bonus,total_compensation
-- @tags distributed,arithmetic,expressions
-- @workers 3
-- @expected_workers 3
-- @trace_level INFO
-- @trace_components QUERY,EXECUTION,DISTRIBUTED
SELECT 
    name, 
    salary,
    salary * 0.15 as bonus,
    salary + (salary * 0.15) as total_compensation
FROM employees
WHERE department IN ('Engineering', 'Sales')
ORDER BY total_compensation DESC;

-- @test name=distributed_case_expression
-- @description Test CASE expression in distributed query
-- @expect_rows 10
-- @expect_columns name,department,salary,performance_category
-- @tags distributed,case,expressions
-- @workers 3
-- @expected_workers 3
-- @trace_level DEBUG
-- @trace_components CASE,DISTRIBUTED,EXECUTION
SELECT 
    name,
    department,
    salary,
    CASE 
        WHEN department = 'Engineering' AND salary > 80000 THEN 'Top Performer'
        WHEN department = 'Sales' AND salary > 75000 THEN 'Top Performer'
        WHEN salary > 65000 THEN 'Good Performer'
        ELSE 'Standard Performer'
    END as performance_category
FROM employees
ORDER BY salary DESC;

-- @test name=distributed_subquery_scalar
-- @description Test scalar subquery in distributed environment
-- @expect_rows 10
-- @tags distributed,subquery,scalar
-- @workers 3
-- @trace_level DEBUG
-- @trace_components QUERY,DISTRIBUTED,EXECUTION
SELECT 
    name,
    salary,
    (SELECT AVG(salary) FROM employees) as company_avg
FROM employees
WHERE salary > 65000
ORDER BY salary DESC;