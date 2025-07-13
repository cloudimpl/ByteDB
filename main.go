package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"

	"bytedb/core"
)

func main() {
	// Parse command line arguments
	var catalogPath string
	var persistCatalog bool
	
	for i := 1; i < len(os.Args); i++ {
		arg := os.Args[i]
		if arg == "--persist" || arg == "-p" {
			persistCatalog = true
		} else if !strings.HasPrefix(arg, "-") {
			catalogPath = arg
		}
	}

	// Use current working directory as the base for relative paths in queries
	cwd, err := os.Getwd()
	if err != nil {
		fmt.Printf("Warning: Could not determine current directory: %v\n", err)
		cwd = "."
	}

	// Create query engine with current working directory
	engine := core.NewQueryEngine(cwd)
	defer engine.Close()

	// Enable catalog system - default to in-memory for console
	if persistCatalog {
		// Use file-based catalog if persistence is requested
		if catalogPath == "" {
			catalogPath = "."
		}
		catalogConfig := make(map[string]interface{})
		catalogConfig["base_path"] = fmt.Sprintf("%s/.catalog", catalogPath)
		
		if err := engine.EnableCatalog("file", catalogConfig); err != nil {
			fmt.Printf("Warning: Failed to enable file-based catalog: %v\n", err)
			// Fall back to memory catalog
			if err := engine.EnableCatalog("memory", nil); err != nil {
				fmt.Printf("Error: Failed to enable catalog system: %v\n", err)
			}
		} else {
			fmt.Printf("Catalog persistence enabled in %s/.catalog\n", catalogPath)
		}
	} else {
		// Default to in-memory catalog
		if err := engine.EnableCatalog("memory", nil); err != nil {
			fmt.Printf("Warning: Failed to enable in-memory catalog: %v\n", err)
		}
	}

	// Check if there's a table mappings configuration file
	configPaths := []string{"table_mappings.json"}
	if catalogPath != "" {
		configPaths = append(configPaths, fmt.Sprintf("%s/table_mappings.json", catalogPath))
	}
	
	for _, configPath := range configPaths {
		if _, err := os.Stat(configPath); err == nil {
			if err := engine.LoadTableMappings(configPath); err != nil {
				fmt.Printf("Warning: Failed to load table mappings from %s: %v\n", configPath, err)
			} else {
				fmt.Printf("Loaded table mappings from %s\n", configPath)
			}
			break
		}
	}

	fmt.Println("ByteDB - Advanced SQL Query Engine for Parquet Files")
	fmt.Println("Usage: bytedb [options] [catalog_path]")
	fmt.Println()
	fmt.Println("Options:")
	fmt.Println("  --persist, -p    Enable persistent catalog (default: in-memory)")
	fmt.Println()
	fmt.Println("Quick start:")
	fmt.Println("  - Query files directly: SELECT * FROM read_parquet('data/sales.parquet')")
	fmt.Println("  - Create tables: CREATE TABLE sales AS SELECT * FROM read_parquet('sales.parquet')")
	fmt.Println()
	fmt.Println("Type 'help' for more commands, 'exit' to quit")
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
		
		// Catalog commands
		if strings.HasPrefix(input, "\\catalog ") {
			handleCatalogCommand(engine, input[9:])
			continue
		}
		
		if input == "\\dc" {
			listCatalogs(engine)
			continue
		}
		
		if strings.HasPrefix(input, "\\dn") {
			catalogName := "default"
			if len(input) > 3 {
				catalogName = strings.TrimSpace(input[3:])
			}
			listSchemas(engine, catalogName)
			continue
		}
		
		if strings.HasPrefix(input, "\\dt") {
			pattern := "*"
			if len(input) > 3 {
				pattern = strings.TrimSpace(input[3:])
			}
			listCatalogTables(engine, pattern)
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
	fmt.Println()
	fmt.Println("Table Creation (DuckDB-style):")
	fmt.Println("  CREATE TABLE employees AS SELECT * FROM read_parquet('employees.parquet')")
	fmt.Println("  CREATE TABLE IF NOT EXISTS users AS SELECT * FROM read_parquet('/path/to/users.parquet')")
	fmt.Println()
	fmt.Println("Direct File Queries:")
	fmt.Println("  SELECT * FROM read_parquet('data/sales_2024.parquet')  - Query file directly")
	fmt.Println("  SELECT * FROM read_parquet('s3://bucket/file.parquet') - Query from S3 (if supported)")
	fmt.Println()
	fmt.Println("Standard Queries:")
	fmt.Println("  SELECT * FROM table_name           - Query a registered table")
	fmt.Println("  SELECT col1, col2 FROM table_name  - Select specific columns")
	fmt.Println("  SELECT * FROM table_name WHERE col = 'value'  - Filter rows")
	fmt.Println("  SELECT * FROM table_name LIMIT 10  - Limit results")
	fmt.Println()
	fmt.Println("Meta commands:")
	fmt.Println("  \\d table_name                     - Describe table schema")
	fmt.Println("  \\l                                 - List all registered tables")
	fmt.Println("  \\json <sql>                        - Return results as JSON")
	fmt.Println("  help                               - Show this help")
	fmt.Println("  exit, quit                         - Exit the program")
	fmt.Println()
	fmt.Println("Catalog commands:")
	fmt.Println("  \\dc                                - List catalogs")
	fmt.Println("  \\dn [catalog]                      - List schemas in catalog")
	fmt.Println("  \\dt [pattern]                      - List tables matching pattern")
	fmt.Println("  \\catalog register <table> <path>   - Register table in catalog (legacy)")
	fmt.Println("  \\catalog drop <table>              - Drop table from catalog")
	fmt.Println("  \\catalog add-file <table> <path>   - Add parquet file to existing table")
	fmt.Println("  \\catalog remove-file <table> <path> - Remove file from table")
	fmt.Println()
	fmt.Println("Notes:")
	fmt.Println("  - File paths in read_parquet() are relative to current directory")
	fmt.Println("  - Use absolute paths for files outside current directory")
	fmt.Println("  - Supported WHERE operators: =, !=, <, <=, >, >=, LIKE, BETWEEN, IN")
}

func handleCatalogCommand(engine *core.QueryEngine, command string) {
	parts := strings.Fields(command)
	if len(parts) == 0 {
		fmt.Println("Usage: \\catalog <subcommand> [args...]")
		return
	}
	
	switch parts[0] {
	case "enable":
		if len(parts) < 2 {
			fmt.Println("Usage: \\catalog enable <type> [options]")
			fmt.Println("Types: memory, file")
			return
		}
		
		storeType := parts[1]
		config := make(map[string]interface{})
		
		// Parse options
		for i := 2; i < len(parts); i++ {
			if strings.Contains(parts[i], "=") {
				kv := strings.SplitN(parts[i], "=", 2)
				config[kv[0]] = kv[1]
			}
		}
		
		// Set default base path for file store
		if storeType == "file" && config["base_path"] == nil {
			config["base_path"] = ".catalog"
		}
		
		if err := engine.EnableCatalog(storeType, config); err != nil {
			fmt.Printf("Error enabling catalog: %v\n", err)
		} else {
			fmt.Printf("Catalog enabled with %s store\n", storeType)
		}
		
	case "register":
		if len(parts) < 3 {
			fmt.Println("Usage: \\catalog register <table> <path> [format]")
			return
		}
		
		table := parts[1]
		path := parts[2]
		format := "parquet"
		if len(parts) > 3 {
			format = parts[3]
		}
		
		if err := engine.RegisterTableInCatalog(table, path, format); err != nil {
			fmt.Printf("Error registering table: %v\n", err)
		} else {
			fmt.Printf("Table %s registered\n", table)
		}
		
	case "drop":
		if len(parts) < 2 {
			fmt.Println("Usage: \\catalog drop <table>")
			return
		}
		
		table := parts[1]
		if manager := engine.GetCatalogManager(); manager != nil {
			ctx := context.Background()
			if err := manager.DropTable(ctx, table); err != nil {
				fmt.Printf("Error dropping table: %v\n", err)
			} else {
				fmt.Printf("Table %s dropped\n", table)
			}
		} else {
			fmt.Println("Catalog system is not enabled")
		}
		
	case "add-file":
		if len(parts) < 3 {
			fmt.Println("Usage: \\catalog add-file <table> <path> [validate-schema]")
			return
		}
		
		table := parts[1]
		path := parts[2]
		validateSchema := true
		if len(parts) > 3 && parts[3] == "false" {
			validateSchema = false
		}
		
		if manager := engine.GetCatalogManager(); manager != nil {
			ctx := context.Background()
			if err := manager.AddFileToTable(ctx, table, path, validateSchema); err != nil {
				fmt.Printf("Error adding file to table: %v\n", err)
			} else {
				fmt.Printf("File %s added to table %s\n", path, table)
			}
		} else {
			fmt.Println("Catalog system is not enabled")
		}
		
	case "remove-file":
		if len(parts) < 3 {
			fmt.Println("Usage: \\catalog remove-file <table> <path>")
			return
		}
		
		table := parts[1]
		path := parts[2]
		
		if manager := engine.GetCatalogManager(); manager != nil {
			ctx := context.Background()
			if err := manager.RemoveFileFromTable(ctx, table, path); err != nil {
				fmt.Printf("Error removing file from table: %v\n", err)
			} else {
				fmt.Printf("File %s removed from table %s\n", path, table)
			}
		} else {
			fmt.Println("Catalog system is not enabled")
		}
		
	default:
		fmt.Printf("Unknown catalog command: %s\n", parts[0])
	}
}

func listCatalogs(engine *core.QueryEngine) {
	manager := engine.GetCatalogManager()
	if manager == nil {
		fmt.Println("Catalog system is not enabled")
		return
	}
	
	store := manager.GetMetadataStore()
	ctx := context.Background()
	catalogs, err := store.ListCatalogs(ctx)
	if err != nil {
		fmt.Printf("Error listing catalogs: %v\n", err)
		return
	}
	
	fmt.Println("Catalogs:")
	for _, catalog := range catalogs {
		fmt.Printf("  %s", catalog.Name)
		if catalog.Description != "" {
			fmt.Printf(" - %s", catalog.Description)
		}
		fmt.Println()
	}
}

func listSchemas(engine *core.QueryEngine, catalogName string) {
	manager := engine.GetCatalogManager()
	if manager == nil {
		fmt.Println("Catalog system is not enabled")
		return
	}
	
	store := manager.GetMetadataStore()
	ctx := context.Background()
	schemas, err := store.ListSchemas(ctx, catalogName)
	if err != nil {
		fmt.Printf("Error listing schemas: %v\n", err)
		return
	}
	
	fmt.Printf("Schemas in catalog '%s':\n", catalogName)
	for _, schema := range schemas {
		fmt.Printf("  %s", schema.Name)
		if schema.Description != "" {
			fmt.Printf(" - %s", schema.Description)
		}
		fmt.Println()
	}
}

func listCatalogTables(engine *core.QueryEngine, pattern string) {
	manager := engine.GetCatalogManager()
	if manager == nil {
		fmt.Println("Catalog system is not enabled. Using legacy table listing.")
		tables, err := engine.ListTables()
		if err != nil {
			fmt.Printf("Error: %v\n", err)
		} else {
			fmt.Println("Tables:")
			for _, table := range tables {
				fmt.Printf("  - %s\n", table)
			}
		}
		return
	}
	
	ctx := context.Background()
	tables, err := manager.ListTables(ctx, pattern)
	if err != nil {
		fmt.Printf("Error listing tables: %v\n", err)
		return
	}
	
	fmt.Println("Tables:")
	currentCatalog := ""
	currentSchema := ""
	
	for _, table := range tables {
		// Group by catalog and schema
		if table.CatalogName != currentCatalog {
			currentCatalog = table.CatalogName
			fmt.Printf("\nCatalog: %s\n", currentCatalog)
		}
		if table.SchemaName != currentSchema {
			currentSchema = table.SchemaName
			fmt.Printf("  Schema: %s\n", currentSchema)
		}
		
		fmt.Printf("    %s", table.Name)
		if table.Location != "" {
			fmt.Printf(" (%s)", table.Location)
		}
		if table.Description != "" {
			fmt.Printf(" - %s", table.Description)
		}
		fmt.Println()
	}
}
