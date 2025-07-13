package core

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// TableMapping represents a mapping from logical table name to physical file
type TableMapping struct {
	TableName    string                 `json:"table_name"`
	FilePath     string                 `json:"file_path"`
	Schema       *TableSchema           `json:"schema,omitempty"`
	Partitions   []string               `json:"partitions,omitempty"`
	Properties   map[string]interface{} `json:"properties,omitempty"`
}

// TableSchema represents the schema of a table
type TableSchema struct {
	Columns []ColumnSchema `json:"columns"`
}

// ColumnSchema represents a column in the table schema
type ColumnSchema struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
}

// TableRegistry manages table name to file mappings
type TableRegistry struct {
	mappings  map[string]*TableMapping
	basePath  string
	mu        sync.RWMutex
}

// NewTableRegistry creates a new table registry
func NewTableRegistry(basePath string) *TableRegistry {
	return &TableRegistry{
		mappings: make(map[string]*TableMapping),
		basePath: basePath,
	}
}

// RegisterTable registers a table with its file mapping
func (tr *TableRegistry) RegisterTable(tableName string, filePath string) error {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	// If filePath is relative, make it relative to basePath
	if !filepath.IsAbs(filePath) {
		filePath = filepath.Join(tr.basePath, filePath)
	}

	// Verify file exists
	if _, err := os.Stat(filePath); err != nil {
		return fmt.Errorf("file not found: %s", filePath)
	}

	tr.mappings[tableName] = &TableMapping{
		TableName: tableName,
		FilePath:  filePath,
	}

	return nil
}

// RegisterTableWithSchema registers a table with schema information
func (tr *TableRegistry) RegisterTableWithSchema(mapping *TableMapping) error {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	// If filePath is relative, make it relative to basePath
	if !filepath.IsAbs(mapping.FilePath) {
		mapping.FilePath = filepath.Join(tr.basePath, mapping.FilePath)
	}

	// Verify file exists
	if _, err := os.Stat(mapping.FilePath); err != nil {
		return fmt.Errorf("file not found: %s", mapping.FilePath)
	}

	tr.mappings[mapping.TableName] = mapping
	return nil
}

// RegisterPartitionedTable registers a table with multiple partition files
func (tr *TableRegistry) RegisterPartitionedTable(tableName string, partitionPaths []string) error {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	// Verify all partition files exist
	for i, path := range partitionPaths {
		if !filepath.IsAbs(path) {
			partitionPaths[i] = filepath.Join(tr.basePath, path)
		}
		if _, err := os.Stat(partitionPaths[i]); err != nil {
			return fmt.Errorf("partition file not found: %s", path)
		}
	}

	tr.mappings[tableName] = &TableMapping{
		TableName:  tableName,
		Partitions: partitionPaths,
	}

	return nil
}

// GetTablePath returns the file path for a given table name
func (tr *TableRegistry) GetTablePath(tableName string) (string, error) {
	tr.mu.RLock()
	defer tr.mu.RUnlock()

	mapping, exists := tr.mappings[tableName]
	if !exists {
		// Fallback to default behavior (table_name.parquet)
		defaultPath := filepath.Join(tr.basePath, tableName+".parquet")
		if _, err := os.Stat(defaultPath); err == nil {
			return defaultPath, nil
		}
		return "", fmt.Errorf("table not registered: %s", tableName)
	}

	if mapping.FilePath != "" {
		return mapping.FilePath, nil
	}

	// For partitioned tables, return the first partition for now
	// TODO: Implement partition pruning logic
	if len(mapping.Partitions) > 0 {
		return mapping.Partitions[0], nil
	}

	return "", fmt.Errorf("no file path found for table: %s", tableName)
}

// GetTableMapping returns the complete mapping for a table
func (tr *TableRegistry) GetTableMapping(tableName string) (*TableMapping, error) {
	tr.mu.RLock()
	defer tr.mu.RUnlock()

	mapping, exists := tr.mappings[tableName]
	if !exists {
		return nil, fmt.Errorf("table not registered: %s", tableName)
	}

	return mapping, nil
}

// GetAllPartitions returns all partition files for a table
func (tr *TableRegistry) GetAllPartitions(tableName string) ([]string, error) {
	tr.mu.RLock()
	defer tr.mu.RUnlock()

	mapping, exists := tr.mappings[tableName]
	if !exists {
		return nil, fmt.Errorf("table not registered: %s", tableName)
	}

	if len(mapping.Partitions) > 0 {
		return mapping.Partitions, nil
	}

	// Single file table
	if mapping.FilePath != "" {
		return []string{mapping.FilePath}, nil
	}

	return nil, fmt.Errorf("no files found for table: %s", tableName)
}

// ListTables returns all registered table names
func (tr *TableRegistry) ListTables() []string {
	tr.mu.RLock()
	defer tr.mu.RUnlock()

	tables := make([]string, 0, len(tr.mappings))
	for tableName := range tr.mappings {
		tables = append(tables, tableName)
	}
	return tables
}

// LoadFromFile loads table mappings from a JSON configuration file
func (tr *TableRegistry) LoadFromFile(configPath string) error {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("failed to read config file: %w", err)
	}

	var config struct {
		Tables []TableMapping `json:"tables"`
	}

	if err := json.Unmarshal(data, &config); err != nil {
		return fmt.Errorf("failed to parse config file: %w", err)
	}

	for _, mapping := range config.Tables {
		if err := tr.RegisterTableWithSchema(&mapping); err != nil {
			return fmt.Errorf("failed to register table %s: %w", mapping.TableName, err)
		}
	}

	return nil
}

// SaveToFile saves current table mappings to a JSON configuration file
func (tr *TableRegistry) SaveToFile(configPath string) error {
	tr.mu.RLock()
	defer tr.mu.RUnlock()

	mappings := make([]TableMapping, 0, len(tr.mappings))
	for _, mapping := range tr.mappings {
		mappings = append(mappings, *mapping)
	}

	config := struct {
		Tables []TableMapping `json:"tables"`
	}{
		Tables: mappings,
	}

	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	if err := os.WriteFile(configPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}

// Clear removes all table mappings
func (tr *TableRegistry) Clear() {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.mappings = make(map[string]*TableMapping)
}

// GetAllMappings returns all table mappings
func (tr *TableRegistry) GetAllMappings() []TableMapping {
	tr.mu.RLock()
	defer tr.mu.RUnlock()
	
	mappings := make([]TableMapping, 0, len(tr.mappings))
	for _, mapping := range tr.mappings {
		mappings = append(mappings, *mapping)
	}
	
	return mappings
}