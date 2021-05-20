package ledger

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// SupportedType is a pseudo-enumerator for generalizing supported field types.
type SupportedType uint

// ...
const (
	Float SupportedType = iota
	Int
	Bool
	String
	Date
)

// Recast parses true types from string datasets.
func Recast(inData map[string]interface{}) map[string]interface{} {
	outData := make(map[string]interface{})
	for id, inRecord := range inData {
		outRecord := make(map[string]interface{})

		for field, inValue := range inRecord.(map[string]interface{}) {
			if outValue, err := strconv.ParseInt(inValue.(string), 10, 64); err == nil {
				outRecord[field] = outValue
				continue
			}
			if outValue, err := strconv.ParseFloat(inValue.(string), 64); err == nil {
				outRecord[field] = outValue
				continue
			}
			if outValue, err := strconv.ParseBool(inValue.(string)); err == nil {
				outRecord[field] = outValue
				continue
			}
			if outValue, err := time.Parse("1/2/2006", inValue.(string)); err == nil {
				outRecord[field] = outValue
				continue
			}

			outRecord[field] = inValue
		}

		outData[id] = outRecord
	}

	return outData
}

// GetFieldTypes inspects a dataset and determines field typing based on the most common present value types.
func GetFieldTypes(data map[string]interface{}) map[string]SupportedType {
	fieldsAndCounts := make(map[string]map[SupportedType]int)
	for _, record := range data {
		for field, value := range record.(map[string]interface{}) {
			if fieldsAndCounts[field] == nil {
				fieldsAndCounts[field] = make(map[SupportedType]int)
			}
			switch reflect.ValueOf(value).Kind() {
			case reflect.Int:
				fieldsAndCounts[field][Int]++
			case reflect.Int8:
				fieldsAndCounts[field][Int]++
			case reflect.Int16:
				fieldsAndCounts[field][Int]++
			case reflect.Int32:
				fieldsAndCounts[field][Int]++
			case reflect.Int64:
				fieldsAndCounts[field][Int]++
			case reflect.Float32:
				fieldsAndCounts[field][Float]++
			case reflect.Float64:
				fieldsAndCounts[field][Float]++
			case reflect.Bool:
				fieldsAndCounts[field][Bool]++
			default:
				if _, is := value.(time.Time); is {
					fieldsAndCounts[field][Date]++
				} else {
					fieldsAndCounts[field][String]++
				}
			}
		}
	}

	fieldTypes := make(map[string]SupportedType)
	for field, counts := range fieldsAndCounts {
		var highestCount int
		var mostCommonType SupportedType
		for kind, count := range counts {
			if count > highestCount {
				highestCount = count
				mostCommonType = kind
			}
		}
		fieldTypes[field] = mostCommonType
	}

	return fieldTypes
}

// CreateFieldDefinitionClause generates a SQL field definition clause from a map of field names with their types.
func CreateFieldDefinitionClause(fieldTypes map[string]SupportedType) string {
	fields := make([]string, 0)
	for field, kind := range fieldTypes {
		switch kind {
		case Int:
			fields = append(fields, fmt.Sprintf("%s integer", field))
		case Float:
			fields = append(fields, fmt.Sprintf("%s decimal", field))
		case Bool:
			fields = append(fields, fmt.Sprintf("%s boolean", field))
		case Date:
			fields = append(fields, fmt.Sprintf("%s date", field))
		default:
			fields = append(fields, fmt.Sprintf("%s varchar", field))
		}
	}

	return strings.Join(fields, ",")
}
