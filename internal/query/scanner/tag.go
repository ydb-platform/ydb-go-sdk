package scanner

import (
	"fmt"
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

// YDB Type Annotations
//
// This package supports type annotations in struct field tags to validate that database
// column types match the expected types. Type annotations are optional but provide:
//   - Runtime validation of type compatibility
//   - Better code documentation
//   - Early detection of schema changes
//
// Syntax:
//   type MyStruct struct {
//       Name string `sql:"column_name,type:Text"`
//       Age  uint64 `sql:"age,type:Uint64"`
//   }
//
// Supported type formats:
//   - Primitive types: Bool, Int8, Int16, Int32, Int64, Uint8, Uint16, Uint32, Uint64,
//     Float, Double, Date, Datetime, Timestamp, Text, Bytes, JSON, UUID, etc.
//   - List types: List<T> where T is any valid type
//   - Optional types: Optional<T> where T is any valid type
//   - Dict types: Dict<K,V> where K and V are valid types
//   - Nested types: List<Optional<Text>>, Optional<List<Uint64>>, etc.
//
// Example:
//   type Product struct {
//       ID       uint64    `sql:"product_id,type:Uint64"`
//       Name     string    `sql:"name,type:Text"`
//       Tags     []string  `sql:"tags,type:List<Text>"`
//       Rating   *float64  `sql:"rating,type:Optional<Double>"`
//   }

// structFieldTag represents parsed struct field tag information
type structFieldTag struct {
	columnName string
	ydbType    string // YDB type annotation, e.g., "List<Text>", "Optional<Uint64>"
}

// parseFieldTag parses a struct field tag value and extracts column name and type annotation
// Supported formats:
//   - "column_name" - just the column name
//   - "column_name,type:List<Text>" - column name with type annotation
//   - "-" - skip this field
func parseFieldTag(tagValue string) structFieldTag {
	if tagValue == "" || tagValue == "-" {
		return structFieldTag{columnName: tagValue}
	}

	// Find first top-level comma (outside angle brackets)
	commaPos := findTopLevelComma(tagValue)
	
	var columnName, typeAnnotation string
	if commaPos == -1 {
		// No comma found, just column name
		columnName = strings.TrimSpace(tagValue)
	} else {
		columnName = strings.TrimSpace(tagValue[:commaPos])
		// Parse options after the comma
		options := strings.TrimSpace(tagValue[commaPos+1:])
		if strings.HasPrefix(options, "type:") {
			typeAnnotation = strings.TrimSpace(strings.TrimPrefix(options, "type:"))
		}
	}

	return structFieldTag{
		columnName: columnName,
		ydbType:    typeAnnotation,
	}
}

// parseYDBType parses a YDB type string and returns the corresponding types.Type
// Examples: "Text", "Uint64", "List<Text>", "Optional<Uint64>", "List<Optional<Text>>"
func parseYDBType(typeStr string) (types.Type, error) {
	typeStr = strings.TrimSpace(typeStr)
	if typeStr == "" {
		return nil, xerrors.WithStackTrace(fmt.Errorf("empty type string"))
	}

	// Handle Optional<...>
	if strings.HasPrefix(typeStr, "Optional<") && strings.HasSuffix(typeStr, ">") {
		innerTypeStr := typeStr[len("Optional<") : len(typeStr)-1]
		innerType, err := parseYDBType(innerTypeStr)
		if err != nil {
			return nil, err
		}
		return types.NewOptional(innerType), nil
	}

	// Handle List<...>
	if strings.HasPrefix(typeStr, "List<") && strings.HasSuffix(typeStr, ">") {
		itemTypeStr := typeStr[len("List<") : len(typeStr)-1]
		itemType, err := parseYDBType(itemTypeStr)
		if err != nil {
			return nil, err
		}
		return types.NewList(itemType), nil
	}

	// Handle Dict<...,...>
	if strings.HasPrefix(typeStr, "Dict<") && strings.HasSuffix(typeStr, ">") {
		innerStr := typeStr[len("Dict<") : len(typeStr)-1]
		// Find the comma that separates key and value types
		// Need to handle nested types properly
		commaPos := findTopLevelComma(innerStr)
		if commaPos == -1 {
			return nil, xerrors.WithStackTrace(fmt.Errorf("invalid Dict type format: %s", typeStr))
		}
		keyTypeStr := strings.TrimSpace(innerStr[:commaPos])
		valueTypeStr := strings.TrimSpace(innerStr[commaPos+1:])
		keyType, err := parseYDBType(keyTypeStr)
		if err != nil {
			return nil, err
		}
		valueType, err := parseYDBType(valueTypeStr)
		if err != nil {
			return nil, err
		}
		return types.NewDict(keyType, valueType), nil
	}

	// Handle primitive types
	return parsePrimitiveYDBType(typeStr)
}

// findTopLevelComma finds the position of a comma that is not inside angle brackets
func findTopLevelComma(s string) int {
	depth := 0
	for i, ch := range s {
		switch ch {
		case '<':
			depth++
		case '>':
			depth--
		case ',':
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

// parsePrimitiveYDBType parses primitive YDB types
func parsePrimitiveYDBType(typeStr string) (types.Type, error) {
	switch typeStr {
	case "Bool":
		return types.Bool, nil
	case "Int8":
		return types.Int8, nil
	case "Uint8":
		return types.Uint8, nil
	case "Int16":
		return types.Int16, nil
	case "Uint16":
		return types.Uint16, nil
	case "Int32":
		return types.Int32, nil
	case "Uint32":
		return types.Uint32, nil
	case "Int64":
		return types.Int64, nil
	case "Uint64":
		return types.Uint64, nil
	case "Float":
		return types.Float, nil
	case "Double":
		return types.Double, nil
	case "Date":
		return types.Date, nil
	case "Date32":
		return types.Date32, nil
	case "Datetime":
		return types.Datetime, nil
	case "Datetime64":
		return types.Datetime64, nil
	case "Timestamp":
		return types.Timestamp, nil
	case "Timestamp64":
		return types.Timestamp64, nil
	case "Interval":
		return types.Interval, nil
	case "Interval64":
		return types.Interval64, nil
	case "TzDate":
		return types.TzDate, nil
	case "TzDatetime":
		return types.TzDatetime, nil
	case "TzTimestamp":
		return types.TzTimestamp, nil
	case "String", "Bytes":
		return types.Bytes, nil
	case "Utf8", "Text":
		return types.Text, nil
	case "Yson", "YSON":
		return types.YSON, nil
	case "Json", "JSON":
		return types.JSON, nil
	case "Uuid", "UUID":
		return types.UUID, nil
	case "JsonDocument", "JSONDocument":
		return types.JSONDocument, nil
	case "DyNumber":
		return types.DyNumber, nil
	default:
		return nil, xerrors.WithStackTrace(fmt.Errorf("unknown YDB type: %s", typeStr))
	}
}
