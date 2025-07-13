-- @test name=invalid_table_name
-- @description Test error handling for non-existent table
-- @expect_error table not found
-- @tags error,negative
-- @trace_level DEBUG
-- @trace_components QUERY,PARSER
SELECT * FROM non_existent_table;

-- @test name=invalid_column_name
-- @description Test error handling for non-existent column
-- @expect_error column not found
-- @tags error,negative
-- @trace_level DEBUG
-- @trace_components QUERY,PARSER
SELECT invalid_column FROM employees;

-- @test name=syntax_error
-- @description Test error handling for SQL syntax error
-- @expect_error syntax error
-- @tags error,negative,syntax
-- @trace_level DEBUG
-- @trace_components QUERY,PARSER
SELECT FROM employees WHERE;

-- @test name=division_by_zero
-- @description Test error handling for division by zero
-- @expect_error division by zero
-- @tags error,negative,arithmetic
-- @trace_level DEBUG
-- @trace_components QUERY,EXECUTION
SELECT name, salary / 0 as invalid_calc FROM employees;

-- @test name=invalid_function
-- @description Test error handling for unknown function
-- @expect_error unknown function
-- @tags error,negative,functions
-- @trace_level DEBUG
-- @trace_components QUERY,PARSER
SELECT name, UNKNOWN_FUNCTION(salary) FROM employees;

-- @test name=type_mismatch
-- @description Test error handling for type mismatch in comparison
-- @expect_error type mismatch
-- @tags error,negative,types
-- @trace_level DEBUG
-- @trace_components QUERY,FILTER
SELECT * FROM employees WHERE salary = 'not_a_number';

-- @test name=invalid_group_by
-- @description Test error handling for invalid GROUP BY usage
-- @expect_error must appear in GROUP BY
-- @tags error,negative,group_by
-- @trace_level DEBUG
-- @trace_components QUERY,AGGREGATE
SELECT name, COUNT(*) FROM employees GROUP BY department;

-- @test name=invalid_join_condition
-- @description Test error handling for invalid JOIN condition
-- @expect_error invalid join condition
-- @tags error,negative,join
-- @trace_level DEBUG
-- @trace_components QUERY,JOIN
SELECT * FROM employees e JOIN departments d ON invalid_condition;