-- CASE Expression Test Suite for ByteDB
-- This file contains comprehensive tests for CASE expression functionality
-- Run with: ./bytedb ./data < test_case_expressions.sql

-- Test 1: Basic CASE with string literals
SELECT 'Test 1: Basic CASE with salary tiers' as test_name;
SELECT name, salary, 
       CASE WHEN salary > 80000 THEN 'High' 
            WHEN salary > 60000 THEN 'Medium' 
            ELSE 'Low' END as salary_tier 
FROM employees 
ORDER BY salary DESC;

-- Test 2: CASE with department categorization
SELECT 'Test 2: Department categorization' as test_name;
SELECT name, department, 
       CASE WHEN department = 'Engineering' THEN 'Tech' 
            WHEN department = 'Marketing' THEN 'Sales & Marketing' 
            ELSE department END as dept_category 
FROM employees;

-- Test 3: CASE with numeric results
SELECT 'Test 3: Numeric results' as test_name;
SELECT name, salary, 
       CASE WHEN salary >= 75000 THEN 1 
            WHEN salary >= 65000 THEN 2 
            ELSE 3 END as salary_rank 
FROM employees 
ORDER BY salary DESC;

-- Test 4: Nested CASE expressions
SELECT 'Test 4: Nested CASE expressions' as test_name;
SELECT name, department, salary,
       CASE WHEN department = 'Engineering' THEN 
            CASE WHEN salary > 80000 THEN 'Senior Engineer' 
                 ELSE 'Engineer' END 
            WHEN department = 'Marketing' THEN 'Marketing Specialist' 
            ELSE 'Other' END as role 
FROM employees;

-- Test 5: CASE without ELSE clause (should return NULL)
SELECT 'Test 5: CASE without ELSE clause' as test_name;
SELECT name, department, salary, 
       CASE WHEN salary > 80000 THEN 'High Earner' 
            WHEN department = 'Engineering' THEN 'Engineer' END as category 
FROM employees;

-- Test 6: CASE with IS NULL conditions
SELECT 'Test 6: NULL handling' as test_name;
SELECT name, department, salary, 
       CASE WHEN department IS NULL THEN 'No Department' 
            WHEN salary IS NULL THEN 'No Salary' 
            ELSE 'Has Data' END as data_status 
FROM employees;

-- Test 7: Multiple CASE expressions in same query
SELECT 'Test 7: Multiple CASE expressions' as test_name;
SELECT name, salary, department, 
       CASE WHEN salary > 75000 THEN 'High' ELSE 'Low' END as salary_tier,
       CASE WHEN department = 'Engineering' THEN 'Tech' ELSE 'Non-Tech' END as tech_status 
FROM employees;

-- Test 8: CASE with simple comparison operators
SELECT 'Test 8: Comparison operators' as test_name;
SELECT name, salary,
       CASE WHEN salary > 80000 THEN 'Above 80k'
            WHEN salary >= 70000 THEN 'Above 70k'
            WHEN salary >= 60000 THEN 'Above 60k'
            ELSE 'Below 60k' END as salary_bracket
FROM employees
ORDER BY salary DESC;

-- Test 9: CASE with boolean logic (simple conditions)
SELECT 'Test 9: Boolean conditions' as test_name;
SELECT name, salary, department,
       CASE WHEN salary > 75000 THEN 'High Salary'
            WHEN department = 'Marketing' THEN 'Marketing Dept'
            WHEN department = 'Engineering' THEN 'Engineering Dept'
            ELSE 'Other' END as classification
FROM employees;

-- Test 10: CASE expressions with string operations
SELECT 'Test 10: String operations' as test_name;
SELECT name, department,
       CASE WHEN department = 'Engineering' THEN 'ENG'
            WHEN department = 'Marketing' THEN 'MKT' 
            WHEN department = 'Finance' THEN 'FIN'
            WHEN department = 'Sales' THEN 'SLS'
            WHEN department = 'HR' THEN 'HR'
            ELSE 'UNK' END as dept_code
FROM employees;

-- Test 11: Complex nested conditions
SELECT 'Test 11: Complex nested CASE' as test_name;
SELECT name, salary, department,
       CASE 
         WHEN department = 'Engineering' THEN 
           CASE 
             WHEN salary > 80000 THEN 'Senior Engineer (80k+)'
             WHEN salary > 75000 THEN 'Engineer (75k+)'
             ELSE 'Junior Engineer (<75k)'
           END
         WHEN department = 'Marketing' THEN
           CASE 
             WHEN salary > 65000 THEN 'Senior Marketing'
             ELSE 'Marketing'
           END
         ELSE 'Other Department'
       END as detailed_role
FROM employees;

-- Test 12: CASE with edge cases and boundary values
SELECT 'Test 12: Edge cases and boundaries' as test_name;
SELECT name, salary,
       CASE WHEN salary = 75000 THEN 'Exactly 75k'
            WHEN salary = 80000 THEN 'Exactly 80k'
            WHEN salary > 100000 THEN 'Above 100k' 
            WHEN salary < 50000 THEN 'Below 50k'
            ELSE 'Normal range' END as boundary_test
FROM employees;

-- Test 13: CASE with mixed data types in results
SELECT 'Test 13: Mixed data types' as test_name;
SELECT name, salary,
       CASE WHEN salary > 80000 THEN 1
            WHEN department = 'Marketing' THEN 0
            ELSE -1 END as numeric_category
FROM employees;

SELECT 'All CASE expression tests completed' as final_message;