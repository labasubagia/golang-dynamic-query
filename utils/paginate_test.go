package utils

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPaginate(t *testing.T) {
	t.Run("get offset", func(t *testing.T) {
		testCases := []struct {
			Paginate Paginate
			Expected int
		}{
			{
				Paginate: Paginate{Page: 1, Limit: 10},
				Expected: 0,
			},
			{
				Paginate: Paginate{Page: -2, Limit: 10},
				Expected: 0,
			},
			{
				Paginate: Paginate{Page: 0, Limit: 10},
				Expected: 0,
			},
			{
				Paginate: Paginate{Page: 3, Limit: 10},
				Expected: 20,
			},
		}

		for i, tc := range testCases {
			t.Run(fmt.Sprintf("case %v", i), func(t *testing.T) {
				actual := tc.Paginate.GetOffset()
				assert.Equal(t, tc.Expected, actual)
			})
		}
	})
}
