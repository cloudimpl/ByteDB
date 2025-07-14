-- @test name=distributed_count_optimization
-- @description Verify COUNT(*) transfers only counts, not raw data
-- @expect_rows 1
-- @expect_columns total
-- @tags distributed,optimization,aggregate,network
-- @workers 3
-- @expected_workers 3
-- @expected_fragments 3
-- @network_optimization
-- @verify_partial_aggs
-- @trace_level VERBOSE
-- @trace_components AGGREGATE,DISTRIBUTED,OPTIMIZER,NETWORK
-- @performance max_duration=500ms
SELECT COUNT(*) as total 
FROM employees;

-- @test name=distributed_sum_avg_optimization
-- @description Verify SUM and AVG partial aggregation optimization
-- @expect_rows 1
-- @expect_columns total_salary,avg_salary
-- @tags distributed,optimization,aggregate,network
-- @workers 3
-- @expected_workers 3
-- @network_optimization
-- @verify_partial_aggs
-- @trace_level DEBUG
-- @trace_components AGGREGATE,DISTRIBUTED,OPTIMIZER
SELECT 
    SUM(salary) as total_salary,
    AVG(salary) as avg_salary
FROM employees;

-- @test name=distributed_group_by_optimization
-- @description Verify GROUP BY with partial aggregation on workers
-- @expect_rows 5
-- @tags distributed,optimization,group_by,aggregate
-- @workers 3
-- @expected_workers 3
-- @network_optimization
-- @verify_partial_aggs
-- @trace_level DEBUG
-- @trace_components AGGREGATE,DISTRIBUTED,OPTIMIZER,GROUP_BY
SELECT 
    department,
    COUNT(*) as count,
    SUM(salary) as total_salary,
    AVG(salary) as avg_salary,
    MAX(salary) as max_salary,
    MIN(salary) as min_salary
FROM employees
GROUP BY department
ORDER BY department;

-- @test name=distributed_filtered_aggregation_optimization
-- @description Verify predicate pushdown with aggregation optimization
-- @expect_rows 1
-- @tags distributed,optimization,filter,aggregate,pushdown
-- @workers 3
-- @expected_workers 3
-- @network_optimization
-- @verify_partial_aggs
-- @trace_level VERBOSE
-- @trace_components FILTER,AGGREGATE,DISTRIBUTED,OPTIMIZER
SELECT 
    COUNT(*) as engineering_count,
    AVG(salary) as avg_engineering_salary
FROM employees
WHERE department = 'Engineering' AND salary > 70000;

-- @test name=distributed_multi_group_optimization
-- @description Test complex GROUP BY with multiple columns
-- @expect_rows 10
-- @tags distributed,optimization,group_by,complex
-- @workers 3
-- @expected_workers 3
-- @network_optimization
-- @verify_partial_aggs
-- @trace_level DEBUG
-- @trace_components AGGREGATE,DISTRIBUTED,OPTIMIZER,GROUP_BY
SELECT 
    department,
    CASE 
        WHEN age < 30 THEN 'Young'
        WHEN age < 40 THEN 'Mid'
        ELSE 'Senior'
    END as age_group,
    COUNT(*) as count,
    AVG(salary) as avg_salary
FROM employees
GROUP BY 
    department,
    CASE 
        WHEN age < 30 THEN 'Young'
        WHEN age < 40 THEN 'Mid'
        ELSE 'Senior'
    END
ORDER BY department, age_group;

-- @test name=distributed_having_clause_optimization
-- @description Test HAVING clause with distributed aggregation
-- @expect_rows 2
-- @tags distributed,optimization,having,aggregate
-- @workers 3
-- @expected_workers 3
-- @network_optimization
-- @verify_partial_aggs
-- @trace_level DEBUG
-- @trace_components AGGREGATE,DISTRIBUTED,OPTIMIZER,HAVING
SELECT 
    department,
    COUNT(*) as emp_count,
    AVG(salary) as avg_salary
FROM employees
GROUP BY department
HAVING COUNT(*) > 2
ORDER BY avg_salary DESC;

-- @test name=distributed_distinct_optimization
-- @description Test DISTINCT with distributed execution
-- @expect_rows 5
-- @expect_columns department
-- @tags distributed,optimization,distinct
-- @workers 3
-- @expected_workers 3
-- @trace_level DEBUG
-- @trace_components DISTINCT,DISTRIBUTED,OPTIMIZER
SELECT DISTINCT department
FROM employees
ORDER BY department;

-- @test name=distributed_complex_expression_optimization
-- @description Test complex expressions with aggregation optimization
-- @expect_rows 5
-- @tags distributed,optimization,expressions,aggregate
-- @workers 3
-- @expected_workers 3
-- @network_optimization
-- @verify_partial_aggs
-- @trace_level VERBOSE
-- @trace_components AGGREGATE,DISTRIBUTED,OPTIMIZER,EXPRESSION
SELECT 
    department,
    COUNT(*) as total_employees,
    SUM(salary * 1.1) as total_with_raise,
    AVG(salary * 1.1) as avg_with_raise,
    SUM(CASE WHEN salary > 70000 THEN salary * 0.15 ELSE salary * 0.10 END) as total_bonus
FROM employees
GROUP BY department
ORDER BY total_bonus DESC;

-- @test name=distributed_nested_aggregation
-- @description Test nested aggregation patterns
-- @expect_rows 1
-- @tags distributed,optimization,aggregate,nested
-- @workers 3
-- @expected_workers 3
-- @network_optimization
-- @trace_level DEBUG
-- @trace_components AGGREGATE,DISTRIBUTED,OPTIMIZER
SELECT 
    COUNT(DISTINCT department) as unique_departments,
    AVG(salary) as overall_avg_salary,
    MAX(salary) - MIN(salary) as salary_range
FROM employees;

-- @test name=distributed_percentile_approximation
-- @description Test approximate percentile calculation
-- @expect_rows 1
-- @tags distributed,optimization,percentile,approximate
-- @workers 3
-- @expected_workers 3
-- @trace_level DEBUG
-- @trace_components AGGREGATE,DISTRIBUTED,PERCENTILE
-- @performance max_duration=1s
SELECT 
    COUNT(*) as total,
    MIN(salary) as min_salary,
    MAX(salary) as max_salary,
    AVG(salary) as avg_salary
FROM employees
WHERE salary BETWEEN (
    SELECT MIN(salary) + (MAX(salary) - MIN(salary)) * 0.25 FROM employees
) AND (
    SELECT MIN(salary) + (MAX(salary) - MIN(salary)) * 0.75 FROM employees
);