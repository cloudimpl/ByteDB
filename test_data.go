package main

// Fixed test data that never changes
// This ensures all tests are deterministic and don't break when data generation changes

// Fixed employee test data
var TestEmployees = []Employee{
	{1, "John Doe", "Engineering", 75000.0, 30, "2020-01-15"},
	{2, "Jane Smith", "Marketing", 65000.0, 28, "2019-03-22"},
	{3, "Mike Johnson", "Engineering", 80000.0, 35, "2018-07-10"},
	{4, "Sarah Wilson", "HR", 55000.0, 32, "2021-05-18"},
	{5, "David Brown", "Sales", 70000.0, 29, "2019-11-02"},
	{6, "Lisa Davis", "Engineering", 85000.0, 33, "2017-12-01"},
	{7, "Tom Miller", "Marketing", 62000.0, 27, "2020-08-14"},
	{8, "Anna Garcia", "Finance", 68000.0, 31, "2019-06-25"},
	{9, "Chris Anderson", "Engineering", 78000.0, 34, "2018-04-03"},
	{10, "Maria Rodriguez", "Sales", 72000.0, 26, "2021-02-09"},
}

// Fixed product test data
var TestProducts = []Product{
	{1, "Laptop", "Electronics", 999.99, true, "TechCorp"},
	{2, "Mouse", "Electronics", 29.99, true, "TechCorp"},
	{3, "Keyboard", "Electronics", 89.99, false, "TechCorp"},
	{4, "Monitor", "Electronics", 299.99, true, "ScreenCorp"},
	{5, "Webcam", "Electronics", 79.99, true, "CameraCorp"},
	{6, "Notebook", "Stationery", 5.99, true, "PaperCorp"},
	{7, "Pen", "Stationery", 1.99, true, "PaperCorp"},
	{8, "Eraser", "Stationery", 0.99, false, "PaperCorp"},
	{9, "Calculator", "Electronics", 15.99, true, "MathCorp"},
	{10, "Phone", "Electronics", 699.99, true, "PhoneCorp"},
}

// Fixed department test data
var TestDepartments = []Department{
	{"Engineering", "Lisa Davis", 1000000.0, "Building A", 4},
	{"Marketing", "Jane Smith", 500000.0, "Building B", 2},
	{"HR", "Sarah Wilson", 300000.0, "Building C", 1},
	{"Sales", "David Brown", 750000.0, "Building B", 2},
	{"Finance", "Anna Garcia", 600000.0, "Building C", 1},
	{"Research", "Dr. Smith", 400000.0, "Building D", 0},
}

// Test data facts - document key facts about the test data for easy reference
const (
	// Employee counts by department
	TestEmployeesInEngineering = 4 // John, Mike, Lisa, Chris
	TestEmployeesInMarketing   = 2 // Jane, Tom
	TestEmployeesInHR          = 1 // Sarah
	TestEmployeesInSales       = 2 // David, Maria
	TestEmployeesInFinance     = 1 // Anna
	TestEmployeesInResearch    = 0 // No employees

	// Salary ranges
	TestMinSalary             = 55000.0  // Sarah Wilson (HR)
	TestMaxSalary             = 85000.0  // Lisa Davis (Engineering)
	TestAvgSalaryEngineering  = 79500.0  // (75000 + 80000 + 85000 + 78000) / 4
	TestEmployeesOver70k      = 7        // All except Jane(65k), Sarah(55k), Tom(62k)
	TestEmployeesOver80k      = 2        // Lisa(85k), Mike(80k)
	
	// Product counts and prices
	TestProductsCount         = 10
	TestProductsOver100       = 3        // Laptop(999.99), Monitor(299.99), Phone(699.99)
	TestProductsOver500       = 2        // Laptop(999.99), Phone(699.99)
	TestCheapestProduct       = 0.99     // Eraser
	TestMostExpensiveProduct  = 999.99   // Laptop
	
	// Department facts
	TestDepartmentsCount      = 6
	TestDeptsBudgetOver200k   = 6        // All departments
	TestDeptsBudgetOver500k   = 4        // Engineering, Marketing, Sales, Finance
	TestDeptsWithEmployees    = 5        // All except Research
)