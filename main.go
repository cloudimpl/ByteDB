package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"bytedb/core"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: bytedb <data_directory>")
		fmt.Println("Example: bytedb ./data")
		os.Exit(1)
	}

	dataPath := os.Args[1]
	engine := core.NewQueryEngine(dataPath)
	defer engine.Close()

	fmt.Println("ByteDB - Simple SQL Query Engine for Parquet Files")
	fmt.Println("Type 'help' for available commands, 'exit' to quit")
	fmt.Println()

	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("bytedb> ")

		if !scanner.Scan() {
			break
		}

		input := strings.TrimSpace(scanner.Text())

		if input == "" {
			continue
		}

		if input == "exit" || input == "quit" {
			fmt.Println("Goodbye!")
			break
		}

		if input == "help" {
			printHelp()
			continue
		}

		if strings.HasPrefix(input, "\\d ") {
			tableName := strings.TrimSpace(input[3:])
			info, err := engine.GetTableInfo(tableName)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Println(info)
			}
			continue
		}

		if input == "\\l" {
			tables, err := engine.ListTables()
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Println("Tables:")
				for _, table := range tables {
					fmt.Printf("  - %s\n", table)
				}
			}
			continue
		}

		if strings.HasPrefix(input, "\\json ") {
			sql := strings.TrimSpace(input[6:])
			result, err := engine.ExecuteToJSON(sql)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Println(result)
			}
			continue
		}

		result, err := engine.ExecuteToTable(input)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
		} else {
			fmt.Println(result)
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading input: %v\n", err)
	}
}

func printHelp() {
	fmt.Println("Available commands:")
	fmt.Println("  SELECT * FROM table_name           - Query a parquet file")
	fmt.Println("  SELECT col1, col2 FROM table_name  - Select specific columns")
	fmt.Println("  SELECT * FROM table_name WHERE col = 'value'  - Filter rows")
	fmt.Println("  SELECT * FROM table_name LIMIT 10  - Limit results")
	fmt.Println()
	fmt.Println("Meta commands:")
	fmt.Println("  \\d table_name                     - Describe table schema")
	fmt.Println("  \\l                                 - List all tables")
	fmt.Println("  \\json <sql>                        - Return results as JSON")
	fmt.Println("  help                               - Show this help")
	fmt.Println("  exit, quit                         - Exit the program")
	fmt.Println()
	fmt.Println("Notes:")
	fmt.Println("  - Table names correspond to .parquet files in the data directory")
	fmt.Println("  - Use table_name.parquet for files in the data directory")
	fmt.Println("  - Supported WHERE operators: =, !=, <, <=, >, >=, LIKE")
}
