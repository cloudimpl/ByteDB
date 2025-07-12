package core

import (
	"fmt"
	"strings"
	"time"
	"unicode"
)

// FunctionType represents the type of SQL function
type FunctionType int

const (
	StringFunc FunctionType = iota
	DateFunc
	MathFunc
	AggregateFunc
)

// FunctionCall represents a function call in SQL
type FunctionCall struct {
	Name  string
	Args  []interface{} // Can be Column, literal values, or other FunctionCalls
	Alias string
	Type  FunctionType
}

// FunctionRegistry manages all available SQL functions
type FunctionRegistry struct {
	functions map[string]FunctionDefinition
}

// FunctionDefinition defines a SQL function
type FunctionDefinition struct {
	Name        string
	Type        FunctionType
	MinArgs     int
	MaxArgs     int // -1 for unlimited
	Evaluator   FunctionEvaluator
	Description string
}

// FunctionEvaluator is the function that evaluates the SQL function
type FunctionEvaluator func(args []interface{}) (interface{}, error)

// NewFunctionRegistry creates a new function registry with all built-in functions
func NewFunctionRegistry() *FunctionRegistry {
	registry := &FunctionRegistry{
		functions: make(map[string]FunctionDefinition),
	}

	// Register string functions
	registry.registerStringFunctions()

	// Register date functions
	registry.registerDateFunctions()

	return registry
}

// registerStringFunctions registers all string manipulation functions
func (fr *FunctionRegistry) registerStringFunctions() {
	// CONCAT - concatenate strings
	fr.functions["concat"] = FunctionDefinition{
		Name:        "CONCAT",
		Type:        StringFunc,
		MinArgs:     1,
		MaxArgs:     -1,
		Description: "Concatenates one or more strings",
		Evaluator: func(args []interface{}) (interface{}, error) {
			var result strings.Builder
			for _, arg := range args {
				if arg == nil {
					continue
				}
				result.WriteString(fmt.Sprintf("%v", arg))
			}
			return result.String(), nil
		},
	}

	// SUBSTRING - extract substring
	fr.functions["substring"] = FunctionDefinition{
		Name:        "SUBSTRING",
		Type:        StringFunc,
		MinArgs:     2,
		MaxArgs:     3,
		Description: "Extracts a substring from a string",
		Evaluator: func(args []interface{}) (interface{}, error) {
			if args[0] == nil {
				return nil, nil
			}

			str := fmt.Sprintf("%v", args[0])
			start, err := toInt(args[1])
			if err != nil {
				return nil, fmt.Errorf("SUBSTRING: invalid start position: %v", err)
			}

			// SQL uses 1-based indexing
			if start < 1 {
				start = 1
			}
			start-- // Convert to 0-based for Go

			if start >= len(str) {
				return "", nil
			}

			if len(args) == 3 {
				length, err := toInt(args[2])
				if err != nil {
					return nil, fmt.Errorf("SUBSTRING: invalid length: %v", err)
				}

				end := start + length
				if end > len(str) {
					end = len(str)
				}

				return str[start:end], nil
			}

			return str[start:], nil
		},
	}

	// LENGTH - get string length
	fr.functions["length"] = FunctionDefinition{
		Name:        "LENGTH",
		Type:        StringFunc,
		MinArgs:     1,
		MaxArgs:     1,
		Description: "Returns the length of a string",
		Evaluator: func(args []interface{}) (interface{}, error) {
			if args[0] == nil {
				return nil, nil
			}
			str := fmt.Sprintf("%v", args[0])
			return len(str), nil
		},
	}

	// UPPER - convert to uppercase
	fr.functions["upper"] = FunctionDefinition{
		Name:        "UPPER",
		Type:        StringFunc,
		MinArgs:     1,
		MaxArgs:     1,
		Description: "Converts string to uppercase",
		Evaluator: func(args []interface{}) (interface{}, error) {
			if args[0] == nil {
				return nil, nil
			}
			str := fmt.Sprintf("%v", args[0])
			return strings.ToUpper(str), nil
		},
	}

	// LOWER - convert to lowercase
	fr.functions["lower"] = FunctionDefinition{
		Name:        "LOWER",
		Type:        StringFunc,
		MinArgs:     1,
		MaxArgs:     1,
		Description: "Converts string to lowercase",
		Evaluator: func(args []interface{}) (interface{}, error) {
			if args[0] == nil {
				return nil, nil
			}
			str := fmt.Sprintf("%v", args[0])
			return strings.ToLower(str), nil
		},
	}

	// TRIM - remove leading and trailing whitespace
	fr.functions["trim"] = FunctionDefinition{
		Name:        "TRIM",
		Type:        StringFunc,
		MinArgs:     1,
		MaxArgs:     1,
		Description: "Removes leading and trailing whitespace",
		Evaluator: func(args []interface{}) (interface{}, error) {
			if args[0] == nil {
				return nil, nil
			}
			str := fmt.Sprintf("%v", args[0])
			return strings.TrimSpace(str), nil
		},
	}

	// LTRIM - remove leading whitespace
	fr.functions["ltrim"] = FunctionDefinition{
		Name:        "LTRIM",
		Type:        StringFunc,
		MinArgs:     1,
		MaxArgs:     1,
		Description: "Removes leading whitespace",
		Evaluator: func(args []interface{}) (interface{}, error) {
			if args[0] == nil {
				return nil, nil
			}
			str := fmt.Sprintf("%v", args[0])
			return strings.TrimLeftFunc(str, unicode.IsSpace), nil
		},
	}

	// RTRIM - remove trailing whitespace
	fr.functions["rtrim"] = FunctionDefinition{
		Name:        "RTRIM",
		Type:        StringFunc,
		MinArgs:     1,
		MaxArgs:     1,
		Description: "Removes trailing whitespace",
		Evaluator: func(args []interface{}) (interface{}, error) {
			if args[0] == nil {
				return nil, nil
			}
			str := fmt.Sprintf("%v", args[0])
			return strings.TrimRightFunc(str, unicode.IsSpace), nil
		},
	}
}

// registerDateFunctions registers all date/time manipulation functions
func (fr *FunctionRegistry) registerDateFunctions() {
	// NOW - current timestamp
	fr.functions["now"] = FunctionDefinition{
		Name:        "NOW",
		Type:        DateFunc,
		MinArgs:     0,
		MaxArgs:     0,
		Description: "Returns current timestamp",
		Evaluator: func(args []interface{}) (interface{}, error) {
			return time.Now().Format("2006-01-02 15:04:05"), nil
		},
	}

	// CURRENT_DATE - current date
	fr.functions["current_date"] = FunctionDefinition{
		Name:        "CURRENT_DATE",
		Type:        DateFunc,
		MinArgs:     0,
		MaxArgs:     0,
		Description: "Returns current date",
		Evaluator: func(args []interface{}) (interface{}, error) {
			return time.Now().Format("2006-01-02"), nil
		},
	}

	// DATE_PART - extract part of date
	fr.functions["date_part"] = FunctionDefinition{
		Name:        "DATE_PART",
		Type:        DateFunc,
		MinArgs:     2,
		MaxArgs:     2,
		Description: "Extracts a part of a date",
		Evaluator: func(args []interface{}) (interface{}, error) {
			if args[1] == nil {
				return nil, nil
			}

			part := strings.ToLower(fmt.Sprintf("%v", args[0]))
			dateStr := fmt.Sprintf("%v", args[1])

			// Parse the date
			date, err := parseDate(dateStr)
			if err != nil {
				return nil, fmt.Errorf("DATE_PART: invalid date: %v", err)
			}

			switch part {
			case "year":
				return date.Year(), nil
			case "month":
				return int(date.Month()), nil
			case "day":
				return date.Day(), nil
			case "hour":
				return date.Hour(), nil
			case "minute":
				return date.Minute(), nil
			case "second":
				return date.Second(), nil
			case "dow", "dayofweek":
				return int(date.Weekday()), nil
			case "doy", "dayofyear":
				return date.YearDay(), nil
			case "quarter":
				return (int(date.Month()) + 2) / 3, nil
			default:
				return nil, fmt.Errorf("DATE_PART: unknown date part: %s", part)
			}
		},
	}

	// EXTRACT - SQL standard for date_part
	fr.functions["extract"] = fr.functions["date_part"]

	// DATE_TRUNC - truncate date to specified precision
	fr.functions["date_trunc"] = FunctionDefinition{
		Name:        "DATE_TRUNC",
		Type:        DateFunc,
		MinArgs:     2,
		MaxArgs:     2,
		Description: "Truncates a date to specified precision",
		Evaluator: func(args []interface{}) (interface{}, error) {
			if args[1] == nil {
				return nil, nil
			}

			precision := strings.ToLower(fmt.Sprintf("%v", args[0]))
			dateStr := fmt.Sprintf("%v", args[1])

			// Parse the date
			date, err := parseDate(dateStr)
			if err != nil {
				return nil, fmt.Errorf("DATE_TRUNC: invalid date: %v", err)
			}

			switch precision {
			case "year":
				return time.Date(date.Year(), 1, 1, 0, 0, 0, 0, date.Location()).Format("2006-01-02 15:04:05"), nil
			case "month":
				return time.Date(date.Year(), date.Month(), 1, 0, 0, 0, 0, date.Location()).Format("2006-01-02 15:04:05"), nil
			case "day":
				return time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, date.Location()).Format("2006-01-02 15:04:05"), nil
			case "hour":
				return time.Date(date.Year(), date.Month(), date.Day(), date.Hour(), 0, 0, 0, date.Location()).Format("2006-01-02 15:04:05"), nil
			case "minute":
				return time.Date(date.Year(), date.Month(), date.Day(), date.Hour(), date.Minute(), 0, 0, date.Location()).Format("2006-01-02 15:04:05"), nil
			default:
				return nil, fmt.Errorf("DATE_TRUNC: unknown precision: %s", precision)
			}
		},
	}
}

// GetFunction returns a function definition by name (case-insensitive)
func (fr *FunctionRegistry) GetFunction(name string) (FunctionDefinition, bool) {
	fn, exists := fr.functions[strings.ToLower(name)]
	return fn, exists
}

// EvaluateFunction evaluates a function call with the given arguments
func (fr *FunctionRegistry) EvaluateFunction(call *FunctionCall, row Row) (interface{}, error) {
	fn, exists := fr.GetFunction(call.Name)
	if !exists {
		return nil, fmt.Errorf("unknown function: %s", call.Name)
	}

	// Evaluate arguments
	evaluatedArgs := make([]interface{}, len(call.Args))
	for i, arg := range call.Args {
		switch v := arg.(type) {
		case Column:
			// Get column value from row
			var colValue interface{}
			if v.TableName != "" {
				colValue = row[v.TableName+"."+v.Name]
			} else {
				colValue = row[v.Name]
			}
			evaluatedArgs[i] = colValue
		case *FunctionCall:
			// Recursive function evaluation
			result, err := fr.EvaluateFunction(v, row)
			if err != nil {
				return nil, err
			}
			evaluatedArgs[i] = result
		default:
			// Literal value
			evaluatedArgs[i] = v
		}
	}

	// Check argument count
	if len(evaluatedArgs) < fn.MinArgs {
		return nil, fmt.Errorf("%s requires at least %d arguments, got %d", fn.Name, fn.MinArgs, len(evaluatedArgs))
	}
	if fn.MaxArgs != -1 && len(evaluatedArgs) > fn.MaxArgs {
		return nil, fmt.Errorf("%s accepts at most %d arguments, got %d", fn.Name, fn.MaxArgs, len(evaluatedArgs))
	}

	// Evaluate the function
	return fn.Evaluator(evaluatedArgs)
}

// Helper functions

func toInt(v interface{}) (int, error) {
	switch val := v.(type) {
	case int:
		return val, nil
	case int32:
		return int(val), nil
	case int64:
		return int(val), nil
	case float32:
		return int(val), nil
	case float64:
		return int(val), nil
	case string:
		return 0, fmt.Errorf("cannot convert string to int: %s", val)
	default:
		return 0, fmt.Errorf("cannot convert %T to int", v)
	}
}

func parseDate(dateStr string) (time.Time, error) {
	// Try multiple date formats
	formats := []string{
		"2006-01-02 15:04:05",
		"2006-01-02",
		time.RFC3339,
		time.RFC3339Nano,
	}

	for _, format := range formats {
		if date, err := time.Parse(format, dateStr); err == nil {
			return date, nil
		}
	}

	return time.Time{}, fmt.Errorf("cannot parse date: %s", dateStr)
}
