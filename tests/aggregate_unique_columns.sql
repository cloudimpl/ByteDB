-- Test unique column names for aggregate functions

-- @test name=duplicate_count_functions
-- @description Test that duplicate COUNT functions get unique column names
-- @expect_rows 1
-- @expect_columns count,count_2
-- @tags aggregate,unique_columns
SELECT COUNT(*), COUNT(name) FROM employees;

-- @test name=mixed_aggregates_with_duplicates
-- @description Test mixed aggregate functions with potential duplicates
-- @expect_rows 1
-- @expect_columns sum_salary,sum_age,count,count_2,avg_salary
-- @tags aggregate,unique_columns
SELECT SUM(salary), SUM(age), COUNT(*), COUNT(department), AVG(salary) FROM employees;

-- @test name=explicit_aliases_no_conflict
-- @description Test that explicit aliases prevent conflicts
-- @expect_rows 1
-- @expect_columns total_employees,named_employees,department_count
-- @tags aggregate,unique_columns
SELECT COUNT(*) as total_employees, COUNT(name) as named_employees, COUNT(department) as department_count FROM employees;

-- @test name=same_function_different_columns
-- @description Test same function on different columns
-- @expect_rows 1
-- @expect_columns sum_salary,sum_age,min_salary,min_age,max_salary,max_age
-- @tags aggregate,unique_columns
SELECT SUM(salary), SUM(age), MIN(salary), MIN(age), MAX(salary), MAX(age) FROM employees;