package vectorized

import (
	"fmt"
	"math"
	"strings"
)

// VectorizedExpressionEvaluator handles expression evaluation in vectorized batches
type VectorizedExpressionEvaluator struct {
	Expression *VectorizedExpression
	Context    *ExpressionContext
}

// ExpressionContext provides runtime context for expression evaluation
type ExpressionContext struct {
	Variables map[string]interface{}
	Functions map[string]*VectorizedFunction
}

// VectorizedFunction represents a function that can be applied to vector data
type VectorizedFunction struct {
	Name          string
	ArgumentTypes []DataType
	ReturnType    DataType
	Implementation func(args []*Vector) (*Vector, error)
	IsDeterministic bool
}

// Common expression types and operators
const (
	// Arithmetic operators
	ADD_OP = "+"
	SUB_OP = "-"
	MUL_OP = "*"
	DIV_OP = "/"
	MOD_OP = "%"
	
	// Comparison operators  
	EQ_OP = "="
	NE_OP = "!="
	LT_OP = "<"
	LE_OP = "<="
	GT_OP = ">"
	GE_OP = ">="
	
	// Logical operators
	AND_OP = "AND"
	OR_OP  = "OR"
	NOT_OP = "NOT"
	
	// String functions
	CONCAT_FUNC = "CONCAT"
	UPPER_FUNC  = "UPPER"
	LOWER_FUNC  = "LOWER"
	LENGTH_FUNC = "LENGTH"
	SUBSTR_FUNC = "SUBSTRING"
	
	// Math functions
	ABS_FUNC   = "ABS"
	CEIL_FUNC  = "CEIL"
	FLOOR_FUNC = "FLOOR"
	ROUND_FUNC = "ROUND"
	SQRT_FUNC  = "SQRT"
	
	// Aggregate functions (handled separately in aggregates.go)
	COUNT_FUNC = "COUNT"
	SUM_FUNC   = "SUM"
	AVG_FUNC   = "AVG"
	MIN_FUNC   = "MIN"
	MAX_FUNC   = "MAX"
)

// NewVectorizedExpressionEvaluator creates a new expression evaluator
func NewVectorizedExpressionEvaluator(expression *VectorizedExpression) *VectorizedExpressionEvaluator {
	context := &ExpressionContext{
		Variables: make(map[string]interface{}),
		Functions: createBuiltinFunctions(),
	}
	
	return &VectorizedExpressionEvaluator{
		Expression: expression,
		Context:    context,
	}
}

// Evaluate evaluates the expression against a vector batch
func (eval *VectorizedExpressionEvaluator) Evaluate(batch *VectorBatch) (*Vector, error) {
	return eval.evaluateExpression(eval.Expression, batch)
}

// evaluateExpression recursively evaluates expressions
func (eval *VectorizedExpressionEvaluator) evaluateExpression(expr *VectorizedExpression, batch *VectorBatch) (*Vector, error) {
	switch expr.Type {
	case COLUMN_REF:
		return eval.evaluateColumnReference(expr, batch)
	case CONSTANT:
		return eval.evaluateConstant(expr, batch)
	case FUNCTION_CALL:
		return eval.evaluateFunctionCall(expr, batch)
	case ARITHMETIC_OP:
		return eval.evaluateArithmeticOperation(expr, batch)
	case COMPARISON_OP:
		return eval.evaluateComparisonOperation(expr, batch)
	case LOGICAL_OP:
		return eval.evaluateLogicalOperation(expr, batch)
	default:
		return nil, fmt.Errorf("unsupported expression type: %v", expr.Type)
	}
}

// evaluateColumnReference evaluates column references
func (eval *VectorizedExpressionEvaluator) evaluateColumnReference(expr *VectorizedExpression, batch *VectorBatch) (*Vector, error) {
	if expr.ColumnIndex < 0 || expr.ColumnIndex >= len(batch.Columns) {
		return nil, fmt.Errorf("invalid column index: %d", expr.ColumnIndex)
	}
	
	return batch.Columns[expr.ColumnIndex], nil
}

// evaluateConstant evaluates constant values
func (eval *VectorizedExpressionEvaluator) evaluateConstant(expr *VectorizedExpression, batch *VectorBatch) (*Vector, error) {
	// Create a constant vector filled with the same value
	dataType := eval.inferDataType(expr.Value)
	vector := NewVector(dataType, batch.RowCount)
	vector.Length = batch.RowCount
	vector.IsConstant = true
	vector.ConstValue = expr.Value
	
	// Fill the vector with the constant value
	for i := 0; i < batch.RowCount; i++ {
		switch dataType {
		case INT32:
			vector.SetInt32(i, expr.Value.(int32))
		case INT64:
			vector.SetInt64(i, expr.Value.(int64))
		case FLOAT64:
			vector.SetFloat64(i, expr.Value.(float64))
		case STRING:
			vector.SetString(i, expr.Value.(string))
		case BOOLEAN:
			vector.SetBoolean(i, expr.Value.(bool))
		}
	}
	
	return vector, nil
}

// evaluateFunctionCall evaluates function calls
func (eval *VectorizedExpressionEvaluator) evaluateFunctionCall(expr *VectorizedExpression, batch *VectorBatch) (*Vector, error) {
	function, exists := eval.Context.Functions[expr.Function]
	if !exists {
		return nil, fmt.Errorf("unknown function: %s", expr.Function)
	}
	
	// Evaluate arguments
	args := make([]*Vector, len(expr.Arguments))
	for i, arg := range expr.Arguments {
		var err error
		args[i], err = eval.evaluateExpression(arg, batch)
		if err != nil {
			return nil, err
		}
	}
	
	// Call the function
	return function.Implementation(args)
}

// evaluateArithmeticOperation evaluates arithmetic operations
func (eval *VectorizedExpressionEvaluator) evaluateArithmeticOperation(expr *VectorizedExpression, batch *VectorBatch) (*Vector, error) {
	if len(expr.Arguments) != 2 {
		return nil, fmt.Errorf("arithmetic operation requires exactly 2 arguments")
	}
	
	left, err := eval.evaluateExpression(expr.Arguments[0], batch)
	if err != nil {
		return nil, err
	}
	
	right, err := eval.evaluateExpression(expr.Arguments[1], batch)
	if err != nil {
		return nil, err
	}
	
	return eval.performArithmeticOperation(expr.Function, left, right)
}

// evaluateComparisonOperation evaluates comparison operations
func (eval *VectorizedExpressionEvaluator) evaluateComparisonOperation(expr *VectorizedExpression, batch *VectorBatch) (*Vector, error) {
	if len(expr.Arguments) != 2 {
		return nil, fmt.Errorf("comparison operation requires exactly 2 arguments")
	}
	
	left, err := eval.evaluateExpression(expr.Arguments[0], batch)
	if err != nil {
		return nil, err
	}
	
	right, err := eval.evaluateExpression(expr.Arguments[1], batch)
	if err != nil {
		return nil, err
	}
	
	return eval.performComparisonOperation(expr.Function, left, right)
}

// evaluateLogicalOperation evaluates logical operations
func (eval *VectorizedExpressionEvaluator) evaluateLogicalOperation(expr *VectorizedExpression, batch *VectorBatch) (*Vector, error) {
	switch expr.Function {
	case NOT_OP:
		if len(expr.Arguments) != 1 {
			return nil, fmt.Errorf("NOT operation requires exactly 1 argument")
		}
		operand, err := eval.evaluateExpression(expr.Arguments[0], batch)
		if err != nil {
			return nil, err
		}
		return eval.performLogicalNot(operand)
		
	case AND_OP, OR_OP:
		if len(expr.Arguments) != 2 {
			return nil, fmt.Errorf("%s operation requires exactly 2 arguments", expr.Function)
		}
		
		left, err := eval.evaluateExpression(expr.Arguments[0], batch)
		if err != nil {
			return nil, err
		}
		
		right, err := eval.evaluateExpression(expr.Arguments[1], batch)
		if err != nil {
			return nil, err
		}
		
		if expr.Function == AND_OP {
			return eval.performLogicalAnd(left, right)
		} else {
			return eval.performLogicalOr(left, right)
		}
		
	default:
		return nil, fmt.Errorf("unsupported logical operation: %s", expr.Function)
	}
}

// Arithmetic operation implementations

func (eval *VectorizedExpressionEvaluator) performArithmeticOperation(operator string, left, right *Vector) (*Vector, error) {
	if left.Length != right.Length {
		return nil, fmt.Errorf("vector length mismatch: %d vs %d", left.Length, right.Length)
	}
	
	// Determine result data type
	resultType := eval.promoteDataTypes(left.DataType, right.DataType)
	result := NewVector(resultType, left.Length)
	result.Length = left.Length
	
	switch operator {
	case ADD_OP:
		return eval.performAddition(left, right, result)
	case SUB_OP:
		return eval.performSubtraction(left, right, result)
	case MUL_OP:
		return eval.performMultiplication(left, right, result)
	case DIV_OP:
		return eval.performDivision(left, right, result)
	case MOD_OP:
		return eval.performModulo(left, right, result)
	default:
		return nil, fmt.Errorf("unsupported arithmetic operator: %s", operator)
	}
}

func (eval *VectorizedExpressionEvaluator) performAddition(left, right, result *Vector) (*Vector, error) {
	switch result.DataType {
	case INT32:
		leftData := left.Data.([]int32)
		rightData := right.Data.([]int32)
		resultData := result.Data.([]int32)
		
		for i := 0; i < result.Length; i++ {
			if left.IsNull(i) || right.IsNull(i) {
				result.SetNull(i)
			} else {
				resultData[i] = leftData[i] + rightData[i]
			}
		}
		
	case INT64:
		leftData := eval.convertToInt64(left)
		rightData := eval.convertToInt64(right)
		resultData := result.Data.([]int64)
		
		// TODO: Use SIMD-optimized addition when available
		// For now, use scalar addition
		for i := 0; i < result.Length; i++ {
			if left.IsNull(i) || right.IsNull(i) {
				result.SetNull(i)
			} else {
				resultData[i] = leftData[i] + rightData[i]
			}
		}
		
	case FLOAT64:
		leftData := eval.convertToFloat64(left)
		rightData := eval.convertToFloat64(right)
		resultData := result.Data.([]float64)
		
		// TODO: Use SIMD-optimized addition when available
		// For now, use scalar addition
		for i := 0; i < result.Length; i++ {
			if left.IsNull(i) || right.IsNull(i) {
				result.SetNull(i)
			} else {
				resultData[i] = leftData[i] + rightData[i]
			}
		}
		
	default:
		return nil, fmt.Errorf("addition not supported for data type: %v", result.DataType)
	}
	
	return result, nil
}

func (eval *VectorizedExpressionEvaluator) performSubtraction(left, right, result *Vector) (*Vector, error) {
	switch result.DataType {
	case FLOAT64:
		leftData := eval.convertToFloat64(left)
		rightData := eval.convertToFloat64(right)
		resultData := result.Data.([]float64)
		
		// TODO: Add SIMD optimization here
		if false { // Disabled for now
			// SIMDSubtractFloat64(leftData, rightData, resultData)
			for i := 0; i < result.Length; i++ {
				if left.IsNull(i) || right.IsNull(i) {
					result.SetNull(i)
				}
			}
		} else {
			for i := 0; i < result.Length; i++ {
				if left.IsNull(i) || right.IsNull(i) {
					result.SetNull(i)
				} else {
					resultData[i] = leftData[i] - rightData[i]
				}
			}
		}
		
	default:
		// Similar implementation for other types
		return nil, fmt.Errorf("subtraction not supported for data type: %v", result.DataType)
	}
	
	return result, nil
}

func (eval *VectorizedExpressionEvaluator) performMultiplication(left, right, result *Vector) (*Vector, error) {
	switch result.DataType {
	case FLOAT64:
		leftData := eval.convertToFloat64(left)
		rightData := eval.convertToFloat64(right)
		resultData := result.Data.([]float64)
		
		// TODO: Add SIMD optimization here
		if false { // Disabled for now
			// SIMDMultiplyFloat64(leftData, rightData, resultData)
			for i := 0; i < result.Length; i++ {
				if left.IsNull(i) || right.IsNull(i) {
					result.SetNull(i)
				}
			}
		} else {
			for i := 0; i < result.Length; i++ {
				if left.IsNull(i) || right.IsNull(i) {
					result.SetNull(i)
				} else {
					resultData[i] = leftData[i] * rightData[i]
				}
			}
		}
		
	default:
		return nil, fmt.Errorf("multiplication not supported for data type: %v", result.DataType)
	}
	
	return result, nil
}

func (eval *VectorizedExpressionEvaluator) performDivision(left, right, result *Vector) (*Vector, error) {
	switch result.DataType {
	case FLOAT64:
		leftData := eval.convertToFloat64(left)
		rightData := eval.convertToFloat64(right)
		resultData := result.Data.([]float64)
		
		// TODO: Add SIMD optimization here
		if false { // Disabled for now
			// SIMDDivideFloat64(leftData, rightData, resultData)
			for i := 0; i < result.Length; i++ {
				if left.IsNull(i) || right.IsNull(i) {
					result.SetNull(i)
				}
			}
		} else {
			for i := 0; i < result.Length; i++ {
				if left.IsNull(i) || right.IsNull(i) {
					result.SetNull(i)
				} else if rightData[i] == 0 {
					result.SetNull(i) // Division by zero
				} else {
					resultData[i] = leftData[i] / rightData[i]
				}
			}
		}
		
	default:
		return nil, fmt.Errorf("division not supported for data type: %v", result.DataType)
	}
	
	return result, nil
}

func (eval *VectorizedExpressionEvaluator) performModulo(left, right, result *Vector) (*Vector, error) {
	// Implementation for modulo operation
	switch result.DataType {
	case INT64:
		leftData := eval.convertToInt64(left)
		rightData := eval.convertToInt64(right)
		resultData := result.Data.([]int64)
		
		for i := 0; i < result.Length; i++ {
			if left.IsNull(i) || right.IsNull(i) || rightData[i] == 0 {
				result.SetNull(i)
			} else {
				resultData[i] = leftData[i] % rightData[i]
			}
		}
		
	default:
		return nil, fmt.Errorf("modulo not supported for data type: %v", result.DataType)
	}
	
	return result, nil
}

// Comparison operation implementations

func (eval *VectorizedExpressionEvaluator) performComparisonOperation(operator string, left, right *Vector) (*Vector, error) {
	if left.Length != right.Length {
		return nil, fmt.Errorf("vector length mismatch: %d vs %d", left.Length, right.Length)
	}
	
	result := NewVector(BOOLEAN, left.Length)
	result.Length = left.Length
	resultData := result.Data.([]bool)
	
	switch operator {
	case EQ_OP:
		return eval.performEquality(left, right, result, resultData)
	case NE_OP:
		return eval.performInequality(left, right, result, resultData)
	case LT_OP:
		return eval.performLessThan(left, right, result, resultData)
	case LE_OP:
		return eval.performLessThanOrEqual(left, right, result, resultData)
	case GT_OP:
		return eval.performGreaterThan(left, right, result, resultData)
	case GE_OP:
		return eval.performGreaterThanOrEqual(left, right, result, resultData)
	default:
		return nil, fmt.Errorf("unsupported comparison operator: %s", operator)
	}
}

func (eval *VectorizedExpressionEvaluator) performEquality(left, right, result *Vector, resultData []bool) (*Vector, error) {
	switch left.DataType {
	case INT64:
		leftData := eval.convertToInt64(left)
		rightData := eval.convertToInt64(right)
		
		// TODO: Add SIMD optimization here
		if false { // Disabled for now
			// SIMDCompareEqualInt64(leftData, rightData, resultData)
			for i := 0; i < result.Length; i++ {
				if left.IsNull(i) || right.IsNull(i) {
					result.SetNull(i)
				}
			}
		} else {
			for i := 0; i < result.Length; i++ {
				if left.IsNull(i) || right.IsNull(i) {
					result.SetNull(i)
				} else {
					resultData[i] = leftData[i] == rightData[i]
				}
			}
		}
		
	case STRING:
		leftData := left.Data.([]string)
		rightData := right.Data.([]string)
		
		for i := 0; i < result.Length; i++ {
			if left.IsNull(i) || right.IsNull(i) {
				result.SetNull(i)
			} else {
				resultData[i] = leftData[i] == rightData[i]
			}
		}
		
	default:
		return nil, fmt.Errorf("equality comparison not supported for data type: %v", left.DataType)
	}
	
	return result, nil
}

func (eval *VectorizedExpressionEvaluator) performInequality(left, right, result *Vector, resultData []bool) (*Vector, error) {
	// Similar to equality but with != comparison
	switch left.DataType {
	case INT64:
		leftData := eval.convertToInt64(left)
		rightData := eval.convertToInt64(right)
		
		for i := 0; i < result.Length; i++ {
			if left.IsNull(i) || right.IsNull(i) {
				result.SetNull(i)
			} else {
				resultData[i] = leftData[i] != rightData[i]
			}
		}
		
	default:
		return nil, fmt.Errorf("inequality comparison not supported for data type: %v", left.DataType)
	}
	
	return result, nil
}

func (eval *VectorizedExpressionEvaluator) performLessThan(left, right, result *Vector, resultData []bool) (*Vector, error) {
	switch left.DataType {
	case INT64:
		leftData := eval.convertToInt64(left)
		rightData := eval.convertToInt64(right)
		
		// TODO: Add SIMD optimization here
		if false { // Disabled for now
			// SIMDCompareLessInt64(leftData, rightData, resultData)
			for i := 0; i < result.Length; i++ {
				if left.IsNull(i) || right.IsNull(i) {
					result.SetNull(i)
				}
			}
		} else {
			for i := 0; i < result.Length; i++ {
				if left.IsNull(i) || right.IsNull(i) {
					result.SetNull(i)
				} else {
					resultData[i] = leftData[i] < rightData[i]
				}
			}
		}
		
	default:
		return nil, fmt.Errorf("less than comparison not supported for data type: %v", left.DataType)
	}
	
	return result, nil
}

func (eval *VectorizedExpressionEvaluator) performLessThanOrEqual(left, right, result *Vector, resultData []bool) (*Vector, error) {
	// Similar to less than but with <= comparison
	switch left.DataType {
	case INT64:
		leftData := eval.convertToInt64(left)
		rightData := eval.convertToInt64(right)
		
		for i := 0; i < result.Length; i++ {
			if left.IsNull(i) || right.IsNull(i) {
				result.SetNull(i)
			} else {
				resultData[i] = leftData[i] <= rightData[i]
			}
		}
	}
	
	return result, nil
}

func (eval *VectorizedExpressionEvaluator) performGreaterThan(left, right, result *Vector, resultData []bool) (*Vector, error) {
	switch left.DataType {
	case FLOAT64:
		leftData := eval.convertToFloat64(left)
		rightData := eval.convertToFloat64(right)
		
		// TODO: Add SIMD optimization here
		if false { // Disabled for now
			// SIMDCompareGreaterFloat64(leftData, rightData, resultData)
			for i := 0; i < result.Length; i++ {
				if left.IsNull(i) || right.IsNull(i) {
					result.SetNull(i)
				}
			}
		} else {
			for i := 0; i < result.Length; i++ {
				if left.IsNull(i) || right.IsNull(i) {
					result.SetNull(i)
				} else {
					resultData[i] = leftData[i] > rightData[i]
				}
			}
		}
		
	default:
		return nil, fmt.Errorf("greater than comparison not supported for data type: %v", left.DataType)
	}
	
	return result, nil
}

func (eval *VectorizedExpressionEvaluator) performGreaterThanOrEqual(left, right, result *Vector, resultData []bool) (*Vector, error) {
	// Similar to greater than but with >= comparison
	switch left.DataType {
	case INT64:
		leftData := eval.convertToInt64(left)
		rightData := eval.convertToInt64(right)
		
		for i := 0; i < result.Length; i++ {
			if left.IsNull(i) || right.IsNull(i) {
				result.SetNull(i)
			} else {
				resultData[i] = leftData[i] >= rightData[i]
			}
		}
	}
	
	return result, nil
}

// Logical operation implementations

func (eval *VectorizedExpressionEvaluator) performLogicalAnd(left, right *Vector) (*Vector, error) {
	result := NewVector(BOOLEAN, left.Length)
	result.Length = left.Length
	
	leftData := left.Data.([]bool)
	rightData := right.Data.([]bool)
	resultData := result.Data.([]bool)
	
	for i := 0; i < result.Length; i++ {
		if left.IsNull(i) || right.IsNull(i) {
			result.SetNull(i)
		} else {
			resultData[i] = leftData[i] && rightData[i]
		}
	}
	
	return result, nil
}

func (eval *VectorizedExpressionEvaluator) performLogicalOr(left, right *Vector) (*Vector, error) {
	result := NewVector(BOOLEAN, left.Length)
	result.Length = left.Length
	
	leftData := left.Data.([]bool)
	rightData := right.Data.([]bool)
	resultData := result.Data.([]bool)
	
	for i := 0; i < result.Length; i++ {
		if left.IsNull(i) || right.IsNull(i) {
			result.SetNull(i)
		} else {
			resultData[i] = leftData[i] || rightData[i]
		}
	}
	
	return result, nil
}

func (eval *VectorizedExpressionEvaluator) performLogicalNot(operand *Vector) (*Vector, error) {
	result := NewVector(BOOLEAN, operand.Length)
	result.Length = operand.Length
	
	operandData := operand.Data.([]bool)
	resultData := result.Data.([]bool)
	
	for i := 0; i < result.Length; i++ {
		if operand.IsNull(i) {
			result.SetNull(i)
		} else {
			resultData[i] = !operandData[i]
		}
	}
	
	return result, nil
}

// Utility functions

func (eval *VectorizedExpressionEvaluator) inferDataType(value interface{}) DataType {
	switch value.(type) {
	case int32:
		return INT32
	case int64, int:
		return INT64
	case float32:
		return FLOAT32
	case float64:
		return FLOAT64
	case string:
		return STRING
	case bool:
		return BOOLEAN
	default:
		return STRING // Default fallback
	}
}

func (eval *VectorizedExpressionEvaluator) promoteDataTypes(left, right DataType) DataType {
	// Data type promotion rules
	if left == FLOAT64 || right == FLOAT64 {
		return FLOAT64
	}
	if left == FLOAT32 || right == FLOAT32 {
		return FLOAT64 // Promote to double precision
	}
	if left == INT64 || right == INT64 {
		return INT64
	}
	if left == INT32 || right == INT32 {
		return INT32
	}
	return left // Default to left type
}

func (eval *VectorizedExpressionEvaluator) convertToInt64(vector *Vector) []int64 {
	switch vector.DataType {
	case INT64:
		return vector.Data.([]int64)
	case INT32:
		data := vector.Data.([]int32)
		result := make([]int64, len(data))
		for i, v := range data {
			result[i] = int64(v)
		}
		return result
	default:
		// Return zeros for unsupported conversions
		return make([]int64, vector.Length)
	}
}

func (eval *VectorizedExpressionEvaluator) convertToFloat64(vector *Vector) []float64 {
	switch vector.DataType {
	case FLOAT64:
		return vector.Data.([]float64)
	case FLOAT32:
		data := vector.Data.([]float32)
		result := make([]float64, len(data))
		for i, v := range data {
			result[i] = float64(v)
		}
		return result
	case INT64:
		data := vector.Data.([]int64)
		result := make([]float64, len(data))
		for i, v := range data {
			result[i] = float64(v)
		}
		return result
	case INT32:
		data := vector.Data.([]int32)
		result := make([]float64, len(data))
		for i, v := range data {
			result[i] = float64(v)
		}
		return result
	default:
		return make([]float64, vector.Length)
	}
}

// Built-in function implementations

func createBuiltinFunctions() map[string]*VectorizedFunction {
	functions := make(map[string]*VectorizedFunction)
	
	// String functions
	functions[CONCAT_FUNC] = &VectorizedFunction{
		Name:          CONCAT_FUNC,
		ArgumentTypes: []DataType{STRING, STRING},
		ReturnType:    STRING,
		Implementation: func(args []*Vector) (*Vector, error) {
			if len(args) != 2 {
				return nil, fmt.Errorf("CONCAT requires exactly 2 arguments")
			}
			return concatStrings(args[0], args[1])
		},
		IsDeterministic: true,
	}
	
	functions[UPPER_FUNC] = &VectorizedFunction{
		Name:          UPPER_FUNC,
		ArgumentTypes: []DataType{STRING},
		ReturnType:    STRING,
		Implementation: func(args []*Vector) (*Vector, error) {
			if len(args) != 1 {
				return nil, fmt.Errorf("UPPER requires exactly 1 argument")
			}
			return upperString(args[0])
		},
		IsDeterministic: true,
	}
	
	functions[LOWER_FUNC] = &VectorizedFunction{
		Name:          LOWER_FUNC,
		ArgumentTypes: []DataType{STRING},
		ReturnType:    STRING,
		Implementation: func(args []*Vector) (*Vector, error) {
			if len(args) != 1 {
				return nil, fmt.Errorf("LOWER requires exactly 1 argument")
			}
			return lowerString(args[0])
		},
		IsDeterministic: true,
	}
	
	functions[LENGTH_FUNC] = &VectorizedFunction{
		Name:          LENGTH_FUNC,
		ArgumentTypes: []DataType{STRING},
		ReturnType:    INT32,
		Implementation: func(args []*Vector) (*Vector, error) {
			if len(args) != 1 {
				return nil, fmt.Errorf("LENGTH requires exactly 1 argument")
			}
			return stringLength(args[0])
		},
		IsDeterministic: true,
	}
	
	// Math functions
	functions[ABS_FUNC] = &VectorizedFunction{
		Name:          ABS_FUNC,
		ArgumentTypes: []DataType{FLOAT64},
		ReturnType:    FLOAT64,
		Implementation: func(args []*Vector) (*Vector, error) {
			if len(args) != 1 {
				return nil, fmt.Errorf("ABS requires exactly 1 argument")
			}
			return absoluteValue(args[0])
		},
		IsDeterministic: true,
	}
	
	functions[SQRT_FUNC] = &VectorizedFunction{
		Name:          SQRT_FUNC,
		ArgumentTypes: []DataType{FLOAT64},
		ReturnType:    FLOAT64,
		Implementation: func(args []*Vector) (*Vector, error) {
			if len(args) != 1 {
				return nil, fmt.Errorf("SQRT requires exactly 1 argument")
			}
			return squareRoot(args[0])
		},
		IsDeterministic: true,
	}
	
	return functions
}

// Function implementations

func concatStrings(left, right *Vector) (*Vector, error) {
	result := NewVector(STRING, left.Length)
	result.Length = left.Length
	
	leftData := left.Data.([]string)
	rightData := right.Data.([]string)
	resultData := result.Data.([]string)
	
	for i := 0; i < result.Length; i++ {
		if left.IsNull(i) || right.IsNull(i) {
			result.SetNull(i)
		} else {
			resultData[i] = leftData[i] + rightData[i]
		}
	}
	
	return result, nil
}

func upperString(input *Vector) (*Vector, error) {
	result := NewVector(STRING, input.Length)
	result.Length = input.Length
	
	inputData := input.Data.([]string)
	resultData := result.Data.([]string)
	
	for i := 0; i < result.Length; i++ {
		if input.IsNull(i) {
			result.SetNull(i)
		} else {
			resultData[i] = strings.ToUpper(inputData[i])
		}
	}
	
	return result, nil
}

func lowerString(input *Vector) (*Vector, error) {
	result := NewVector(STRING, input.Length)
	result.Length = input.Length
	
	inputData := input.Data.([]string)
	resultData := result.Data.([]string)
	
	for i := 0; i < result.Length; i++ {
		if input.IsNull(i) {
			result.SetNull(i)
		} else {
			resultData[i] = strings.ToLower(inputData[i])
		}
	}
	
	return result, nil
}

func stringLength(input *Vector) (*Vector, error) {
	result := NewVector(INT32, input.Length)
	result.Length = input.Length
	
	inputData := input.Data.([]string)
	resultData := result.Data.([]int32)
	
	for i := 0; i < result.Length; i++ {
		if input.IsNull(i) {
			result.SetNull(i)
		} else {
			resultData[i] = int32(len(inputData[i]))
		}
	}
	
	return result, nil
}

func absoluteValue(input *Vector) (*Vector, error) {
	result := NewVector(input.DataType, input.Length)
	result.Length = input.Length
	
	switch input.DataType {
	case FLOAT64:
		inputData := input.Data.([]float64)
		resultData := result.Data.([]float64)
		
		for i := 0; i < result.Length; i++ {
			if input.IsNull(i) {
				result.SetNull(i)
			} else {
				resultData[i] = math.Abs(inputData[i])
			}
		}
		
	case INT64:
		inputData := input.Data.([]int64)
		resultData := result.Data.([]int64)
		
		for i := 0; i < result.Length; i++ {
			if input.IsNull(i) {
				result.SetNull(i)
			} else {
				if inputData[i] < 0 {
					resultData[i] = -inputData[i]
				} else {
					resultData[i] = inputData[i]
				}
			}
		}
		
	default:
		return nil, fmt.Errorf("ABS not supported for data type: %v", input.DataType)
	}
	
	return result, nil
}

func squareRoot(input *Vector) (*Vector, error) {
	result := NewVector(FLOAT64, input.Length)
	result.Length = input.Length
	
	inputData := input.Data.([]float64)
	resultData := result.Data.([]float64)
	
	for i := 0; i < result.Length; i++ {
		if input.IsNull(i) {
			result.SetNull(i)
		} else if inputData[i] < 0 {
			result.SetNull(i) // Square root of negative number
		} else {
			resultData[i] = math.Sqrt(inputData[i])
		}
	}
	
	return result, nil
}

// Expression builder utilities

func NewColumnExpression(columnIndex int) *VectorizedExpression {
	return &VectorizedExpression{
		Type:        COLUMN_REF,
		ColumnIndex: columnIndex,
	}
}

func NewConstantExpression(value interface{}) *VectorizedExpression {
	return &VectorizedExpression{
		Type:  CONSTANT,
		Value: value,
	}
}

func NewArithmeticExpression(operator string, left, right *VectorizedExpression) *VectorizedExpression {
	return &VectorizedExpression{
		Type:      ARITHMETIC_OP,
		Function:  operator,
		Arguments: []*VectorizedExpression{left, right},
	}
}

func NewComparisonExpression(operator string, left, right *VectorizedExpression) *VectorizedExpression {
	return &VectorizedExpression{
		Type:      COMPARISON_OP,
		Function:  operator,
		Arguments: []*VectorizedExpression{left, right},
	}
}

func NewFunctionExpression(function string, args ...*VectorizedExpression) *VectorizedExpression {
	return &VectorizedExpression{
		Type:      FUNCTION_CALL,
		Function:  function,
		Arguments: args,
	}
}