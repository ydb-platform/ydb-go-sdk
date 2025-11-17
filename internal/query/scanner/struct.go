package scanner

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type scanStructSettings struct {
	TagName                       string
	AllowMissingColumnsFromSelect bool
	AllowMissingFieldsInStruct    bool
}

type StructScanner struct {
	data *Data
}

func Struct(data *Data) StructScanner {
	return StructScanner{
		data: data,
	}
}

func fieldName(f reflect.StructField, tagName string) string { //nolint:gocritic
	if tagValue, has := f.Tag.Lookup(tagName); has {
		tag := parseFieldTag(tagValue)
		return tag.columnName
	}

	return f.Name
}

func fieldTag(f reflect.StructField, tagName string) structFieldTag { //nolint:gocritic
	if tagValue, has := f.Tag.Lookup(tagName); has {
		return parseFieldTag(tagValue)
	}

	return structFieldTag{columnName: f.Name}
}

func (s StructScanner) ScanStruct(dst interface{}, opts ...ScanStructOption) (err error) {
	settings := scanStructSettings{
		TagName:                       "sql",
		AllowMissingColumnsFromSelect: false,
		AllowMissingFieldsInStruct:    false,
	}
	for _, opt := range opts {
		if opt != nil {
			opt.applyScanStructOption(&settings)
		}
	}
	ptr := reflect.ValueOf(dst)
	if ptr.Kind() != reflect.Pointer {
		return xerrors.WithStackTrace(fmt.Errorf("%w: '%s'", errDstTypeIsNotAPointer, ptr.Kind().String()))
	}
	if ptr.Elem().Kind() != reflect.Struct {
		return xerrors.WithStackTrace(fmt.Errorf("%w: '%s'", errDstTypeIsNotAPointerToStruct, ptr.Elem().Kind().String()))
	}
	tt := ptr.Elem().Type()
	missingColumns := make([]string, 0, len(s.data.columns))
	existingFields := make(map[string]struct{}, tt.NumField())
	for i := 0; i < tt.NumField(); i++ {
		tag := fieldTag(tt.Field(i), settings.TagName)
		if tag.columnName == "-" {
			continue
		}

		v, err := s.data.seekByName(tag.columnName)
		if err != nil {
			missingColumns = append(missingColumns, tag.columnName)
		} else {
			// Validate type if type annotation is present
			if tag.ydbType != "" {
				expectedType, err := parseYDBType(tag.ydbType)
				if err != nil {
					return xerrors.WithStackTrace(fmt.Errorf("invalid type annotation for field '%s': %w", tag.columnName, err))
				}
				actualType := types.TypeFromYDB(v.Type().ToYDB())
				if !types.Equal(expectedType, actualType) {
					return xerrors.WithStackTrace(fmt.Errorf(
						"type mismatch for field '%s': expected %s, got %s",
						tag.columnName,
						expectedType.String(),
						actualType.String(),
					))
				}
			}

			if err = value.CastTo(v, ptr.Elem().Field(i).Addr().Interface()); err != nil {
				return xerrors.WithStackTrace(fmt.Errorf("scan error on struct field name '%s': %w", tag.columnName, err))
			}
			existingFields[tag.columnName] = struct{}{}
		}
	}

	if !settings.AllowMissingColumnsFromSelect && len(missingColumns) > 0 {
		return xerrors.WithStackTrace(
			fmt.Errorf("%w: '%v'", ErrColumnsNotFoundInRow, strings.Join(missingColumns, "','")),
		)
	}

	if !settings.AllowMissingFieldsInStruct {
		missingFields := make([]string, 0, tt.NumField())
		for _, c := range s.data.columns {
			if _, has := existingFields[c.GetName()]; !has {
				missingFields = append(missingFields, c.GetName())
			}
		}
		if len(missingFields) > 0 {
			return xerrors.WithStackTrace(
				fmt.Errorf("%w: '%v'", ErrFieldsNotFoundInStruct, strings.Join(missingFields, "','")),
			)
		}
	}

	return nil
}
