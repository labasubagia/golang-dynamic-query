package utils

import (
	"errors"
	"reflect"
	"sort"
)

func StructToMap(payload any, tag string) (map[string]any, error) {
	result := map[string]any{}
	v := reflect.ValueOf(payload)
	if tag == "" {
		return result, errors.New("tag is required")
	}
	if v.Kind() != reflect.Struct {
		return result, errors.New("payload need to be struct")
	}
	for i := 0; i < v.NumField(); i++ {
		valueField := v.Field(i)
		typeField := v.Type().Field(i)

		fieldName := typeField.Tag.Get(tag)
		if fieldName == "" {
			continue
		}

		if valueField.Kind() == reflect.Ptr {
			if valueField.IsNil() {
				continue
			}
			valueField = valueField.Elem()
		}
		result[fieldName] = valueField.Interface()
	}
	return result, nil
}

func SortMapKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
