package bind

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/params"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

// RewriteQueryArgs transforms SQL queries to optimize for YDB:
// 1. IN clauses with multiple params -> single list param
// 2. INSERT/UPSERT/REPLACE VALUES with multiple tuples -> SELECT FROM AS_TABLE
type RewriteQueryArgs struct{}

func (r RewriteQueryArgs) blockID() blockID {
	return blockYQL
}

func (r RewriteQueryArgs) ToYdb(sql string, args ...any) (yql string, newArgs []any, err error) {
	// Convert args to parameters first
	parameters, err := Params(args...)
	if err != nil {
		return "", nil, xerrors.WithStackTrace(err)
	}

	// Build a map of parameter names to their values
	paramMap := make(map[string]*params.Parameter)
	for _, p := range parameters {
		paramMap[p.Name()] = p
	}

	// Parse and transform the query
	transformed := sql
	newParamMap := make(map[string]*params.Parameter)
	for k, v := range paramMap {
		newParamMap[k] = v
	}

	// Step 1: Transform IN clauses
	transformed, newParamMap, err = r.transformInClauses(transformed, newParamMap)
	if err != nil {
		return "", nil, xerrors.WithStackTrace(err)
	}

	// Step 2: Transform INSERT/UPSERT/REPLACE VALUES
	transformed, newParamMap, err = r.transformInsertValues(transformed, newParamMap)
	if err != nil {
		return "", nil, xerrors.WithStackTrace(err)
	}

	// Convert params back to args
	newArgs = make([]any, 0, len(newParamMap))
	for _, p := range newParamMap {
		newArgs = append(newArgs, p)
	}

	if len(newArgs) > 0 {
		transformed = "-- rewrite query with list/struct args\n" + transformed
	}

	return transformed, newArgs, nil
}

// transformInClauses finds and transforms IN (...) clauses with multiple parameters
func (r RewriteQueryArgs) transformInClauses(
	sql string,
	paramMap map[string]*params.Parameter,
) (string, map[string]*params.Parameter, error) {
	// Pattern to match IN (...) with multiple parameters
	// Match: IN ( $p1, $p2, $p3 ) or IN ($p1,$p2,$p3)
	// We need to handle quoted strings and comments properly

	listCounter := 0
	newParamMap := make(map[string]*params.Parameter)
	for k, v := range paramMap {
		newParamMap[k] = v
	}

	// Regex pattern to find IN clauses with parameter lists
	// This is a simplified approach - match IN followed by parentheses with comma-separated parameters
	inPattern := regexp.MustCompile(`(?i)\bIN\s*\(([^)]+)\)`)

	result := inPattern.ReplaceAllStringFunc(sql, func(match string) string {
		// Extract the content within parentheses
		start := strings.Index(match, "(")
		end := strings.LastIndex(match, ")")
		if start == -1 || end == -1 {
			return match
		}

		content := match[start+1 : end]

		// Find all parameter references in the content
		paramPattern := regexp.MustCompile(`\$[\w]+`)
		paramNames := paramPattern.FindAllString(content, -1)

		// Check if we have multiple parameters and they all exist in the map
		if len(paramNames) <= 1 {
			return match // Not multiple parameters, skip
		}

		// Verify all parameters exist
		var paramValues []value.Value
		for _, paramName := range paramNames {
			if p, exists := newParamMap[paramName]; exists {
				paramValues = append(paramValues, p.Value())
			} else {
				return match // Parameter not found, skip transformation
			}
		}

		// Create a new list parameter
		listParamName := fmt.Sprintf("$argsList%d", listCounter)
		listCounter++

		listParam := params.Named(listParamName, value.ListValue(paramValues...))
		newParamMap[listParamName] = listParam

		// Remove the old parameters from the map
		for _, paramName := range paramNames {
			delete(newParamMap, paramName)
		}

		// Return the transformed IN clause
		return "IN " + listParamName
	})

	return result, newParamMap, nil
}

// transformInsertValues transforms INSERT/UPSERT/REPLACE VALUES clauses
func (r RewriteQueryArgs) transformInsertValues(
	sql string,
	paramMap map[string]*params.Parameter,
) (string, map[string]*params.Parameter, error) {
	// Pattern: INSERT INTO table (col1, col2) VALUES (val1, val2), (val3, val4), ...
	// Transform to: INSERT INTO table SELECT col1, col2 FROM AS_TABLE($valuesList)
	// Similar pattern for UPSERT and REPLACE

	listCounter := 0
	newParamMap := make(map[string]*params.Parameter)
	for k, v := range paramMap {
		newParamMap[k] = v
	}

	// Regex pattern to match INSERT/UPSERT/REPLACE ... VALUES with multiple tuples
	// This matches: INSERT INTO table (cols) VALUES (params), (params), ...
	valuesPattern := regexp.MustCompile(
		`(?i)\b(INSERT|UPSERT|REPLACE)\s+INTO\s+(\S+)\s*\(([^)]+)\)\s+VALUES\s+(.+)`,
	)

	result := valuesPattern.ReplaceAllStringFunc(sql, func(match string) string {
		return r.replaceInsertValues(match, valuesPattern, &listCounter, newParamMap)
	})

	return result, newParamMap, nil
}

// replaceInsertValues is a helper function to replace INSERT VALUES patterns
func (r RewriteQueryArgs) replaceInsertValues(
	match string,
	valuesPattern *regexp.Regexp,
	listCounter *int,
	newParamMap map[string]*params.Parameter,
) string {
	// Parse the match
	matches := valuesPattern.FindStringSubmatch(match)
	if len(matches) != 5 {
		return match
	}

	command := matches[1]   // INSERT/UPSERT/REPLACE
	tableName := matches[2] // table name
	columnList := matches[3]
	valuesClause := matches[4]

	// Extract column names
	columns := r.extractColumns(columnList)

	// Parse value tuples
	tuples := r.extractTuples(valuesClause)
	if len(tuples) <= 1 {
		return match // Only one tuple, no transformation needed
	}

	// Build struct values from tuples
	structValues := r.buildStructValues(tuples, columns, newParamMap)
	if structValues == nil {
		return match // Failed to build struct values
	}

	// Create list parameter with struct values
	listParamName := fmt.Sprintf("$valuesList%d", *listCounter)
	*listCounter++

	listParam := params.Named(listParamName, value.ListValue(structValues...))
	newParamMap[listParamName] = listParam

	// Build the transformed query
	return fmt.Sprintf(
		"%s INTO %s SELECT %s FROM AS_TABLE(%s)",
		command, tableName, columnList, listParamName,
	)
}

// extractColumns extracts column names from column list
func (r RewriteQueryArgs) extractColumns(columnList string) []string {
	columns := strings.Split(columnList, ",")
	for i := range columns {
		columns[i] = strings.TrimSpace(columns[i])
	}

	return columns
}

// extractTuples extracts tuples from VALUES clause
func (r RewriteQueryArgs) extractTuples(valuesClause string) [][]string {
	tuplePattern := regexp.MustCompile(`\(([^)]+)\)`)
	tuples := tuplePattern.FindAllStringSubmatch(valuesClause, -1)

	return tuples
}

// buildStructValues builds struct values from tuples and parameters
func (r RewriteQueryArgs) buildStructValues(
	tuples [][]string,
	columns []string,
	newParamMap map[string]*params.Parameter,
) []value.Value {
	structValues := make([]value.Value, 0, len(tuples))
	paramPattern := regexp.MustCompile(`\$[\w]+`)

	for _, tuple := range tuples {
		if len(tuple) < 2 {
			return nil
		}
		tupleContent := tuple[1]
		paramNames := paramPattern.FindAllString(tupleContent, -1)

		if len(paramNames) != len(columns) {
			return nil // Parameter count doesn't match column count
		}

		// Create struct value for this tuple
		fields := make([]value.StructValueField, len(columns))
		for i, paramName := range paramNames {
			if p, exists := newParamMap[paramName]; exists {
				fields[i] = value.StructValueField{
					Name: columns[i],
					V:    p.Value(),
				}
				delete(newParamMap, paramName)
			} else {
				return nil // Parameter not found
			}
		}

		structValues = append(structValues, value.StructValue(fields...))
	}

	return structValues
}
