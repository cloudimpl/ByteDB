-- @test name=simple_case_expression
-- @description Test basic CASE expression functionality
-- @expect_rows 10
-- @expect_columns name,salary,salary_grade
-- @tags case,expressions
-- @trace_level DEBUG
-- @trace_components CASE,EXECUTION,PARSER
SELECT name, 
       salary,
       CASE 
           WHEN salary > 80000 THEN 'High'
           WHEN salary > 60000 THEN 'Medium'
           ELSE 'Low'
       END as salary_grade
FROM employees
ORDER BY name;

-- @test name=case_with_order_by
-- @description Test CASE expression with ORDER BY - this was the bug we fixed
-- @expect_rows 10
-- @expect_columns name,salary,salary_grade
-- @tags case,order_by,regression
-- @trace_level DEBUG
-- @trace_components CASE,SORT,OPTIMIZER,EXECUTION
SELECT name, 
       salary,
       CASE 
           WHEN salary > 80000 THEN 'High'
           WHEN salary > 60000 THEN 'Medium'
           ELSE 'Low'
       END as salary_grade
FROM employees
ORDER BY salary_grade DESC;

-- @test name=case_in_where_clause
-- @description Test CASE expression in WHERE clause
-- @expect_rows 10
-- @expect_columns name,salary
-- @tags case,where
-- @trace_level DEBUG
-- @trace_components CASE,FILTER,EXECUTION
SELECT name, salary
FROM employees
WHERE CASE 
          WHEN department = 'Engineering' THEN salary > 70000
          WHEN department = 'Sales' THEN salary > 65000
          ELSE salary > 60000
      END
ORDER BY salary DESC;

-- @test name=nested_case_expressions
-- @description Test nested CASE expressions
-- @expect_rows 10
-- @expect_columns name,department,performance_rating
-- @tags case,nested,complex
-- @trace_level VERBOSE
-- @trace_components CASE,EXECUTION
SELECT name, 
       department,
       CASE 
           WHEN department = 'Engineering' THEN
               CASE 
                   WHEN salary > 85000 THEN 'Senior Engineer'
                   WHEN salary > 70000 THEN 'Mid-level Engineer'
                   ELSE 'Junior Engineer'
               END
           WHEN department = 'Sales' THEN
               CASE 
                   WHEN salary > 75000 THEN 'Senior Sales'
                   WHEN salary > 60000 THEN 'Mid-level Sales'
                   ELSE 'Junior Sales'
               END
           ELSE 'Other'
       END as performance_rating
FROM employees
ORDER BY department, salary DESC;

-- @test name=case_with_null_handling
-- @description Test CASE expression with NULL handling
-- @expect_rows 10
-- @expect_columns name,age,age_group
-- @tags case,null
-- @trace_level DEBUG
-- @trace_components CASE,EXECUTION
SELECT name,
       age,
       CASE 
           WHEN age IS NULL THEN 'Unknown'
           WHEN age < 30 THEN 'Young'
           WHEN age < 40 THEN 'Mid-career'
           ELSE 'Senior'
       END as age_group
FROM employees
ORDER BY age;

-- @test name=case_with_aggregation
-- @description Test CASE expression with GROUP BY and aggregation
-- @expect_rows 1
-- @expect_columns salary_bracket
-- @tags case,group_by,aggregate
-- @trace_level DEBUG
-- @trace_components CASE,AGGREGATE,EXECUTION
SELECT 
    CASE 
        WHEN salary > 80000 THEN 'High'
        WHEN salary > 60000 THEN 'Medium'
        ELSE 'Low'
    END as salary_bracket,
    COUNT(*) as employee_count,
    AVG(salary) as avg_salary
FROM employees
GROUP BY CASE 
    WHEN salary > 80000 THEN 'High'
    WHEN salary > 60000 THEN 'Medium'
    ELSE 'Low'
END
ORDER BY avg_salary DESC;

-- @test name=case_with_mathematical_operations
-- @description Test CASE expression with mathematical operations
-- @expect_rows 10
-- @expect_columns name,salary,bonus_rate,bonus_amount
-- @tags case,arithmetic
-- @trace_level DEBUG
-- @trace_components CASE,EXECUTION
SELECT name,
       salary,
       CASE 
           WHEN salary > 80000 THEN 0.15
           WHEN salary > 60000 THEN 0.10
           ELSE 0.05
       END as bonus_rate,
       salary * CASE 
           WHEN salary > 80000 THEN 0.15
           WHEN salary > 60000 THEN 0.10
           ELSE 0.05
       END as bonus_amount
FROM employees
ORDER BY bonus_amount DESC;

-- @test name=case_expression_validation
-- @description Test CASE expression with specific expected values for validation
-- @expect_rows 2
-- @tags case,validation
-- @trace_level VERBOSE
-- @trace_components CASE,EXECUTION
-- @timeout 5s
SELECT name,
       salary,
       CASE 
           WHEN salary > 80000 THEN 'High'
           WHEN salary > 60000 THEN 'Medium'
           ELSE 'Low'
       END as grade
FROM employees
WHERE name = 'John Doe' OR name = 'Jane Smith'
ORDER BY salary DESC;