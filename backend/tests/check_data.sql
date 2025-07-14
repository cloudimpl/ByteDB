-- @test name=check_employees
-- @description Check employee data
-- @expect_rows 10
SELECT id, name, department, salary FROM employees ORDER BY id;