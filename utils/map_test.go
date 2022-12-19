package utils

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type input struct {
	Field1 string `json:"f1" db:"db1"`
	Field2 string `json:"f2" db:"db2"`
	Field3 int    `json:"f3" db:"db3"`
	Field4 *int   `json:"f4" db:"db4"`
	Field5 *int   `json:"f5"`
}

func TestMapToStruct(t *testing.T) {

	t.Run("success", func(t *testing.T) {

		testCases := []struct {
			Input    input
			UsedTag  string
			Expected map[string]any
		}{
			{
				Input:    input{Field1: "v1", Field2: "v2", Field3: 1},
				UsedTag:  "json",
				Expected: map[string]any{"f1": "v1", "f2": "v2", "f3": 1},
			},
			{
				Input:    input{Field1: "v1", Field2: "v2", Field3: 2, Field4: new(int)},
				UsedTag:  "db",
				Expected: map[string]any{"db1": "v1", "db2": "v2", "db3": 2, "db4": 0},
			},
		}

		for i, tc := range testCases {
			t.Run(fmt.Sprintf("case %v", i), func(t *testing.T) {
				actual, err := StructToMap(tc.Input, tc.UsedTag)
				assert.Nil(t, err)
				assert.Equal(t, tc.Expected, actual)
			})
		}
	})

	t.Run("failed tag", func(t *testing.T) {
		_, err := StructToMap(1, "")
		assert.NotNil(t, err)
	})

	t.Run("failed payload", func(t *testing.T) {
		_, err := StructToMap(1, "db")
		assert.NotNil(t, err)
	})

}

func TestSortMapKeys(t *testing.T) {
	data := map[string]any{"c": 3, "b": 2, "a": 1}
	expected := []string{"a", "b", "c"}
	actual := SortMapKeys(data)
	assert.Equal(t, expected, actual)
}
