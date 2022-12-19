package sql

import (
	"bulk/utils"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

type payload struct {
	Field1 *string `db:"f1"`
	Field2 *string `db:"f2"`
	Field3 *string `db:"f3"`
}

type condition struct {
	Field1 *string   `db:"f1"`
	Field2 *int      `db:"f2"`
	Field3 *[]string `db:"f3"`
}

type expected struct {
	Query string
	Bind  map[string]any
}

func TestBuildBulkUpdateQuery(t *testing.T) {

	t.Run("failed", func(t *testing.T) {
		_, _, err := BuildBulkUpdateQuery("", []Update[int, int]{{1, 1}})
		assert.NotNil(t, err)
	})

	t.Run("success", func(t *testing.T) {
		table := "table"
		v1 := "v1"
		v2 := "v2"
		v3 := "v3"
		c1 := "v1"
		c2 := 12
		c3 := []string{"v3_1", "v3_2", "v3_3"}
		c3Empty := []string{}

		testCases := []struct {
			Table    string
			Payload  []Update[payload, condition]
			Expected expected
		}{
			{
				Table: table,
				Payload: []Update[payload, condition]{
					{
						Payload:   payload{Field1: &v1, Field2: &v2},
						Condition: condition{Field1: &c1},
					},
					{
						Payload:   payload{Field3: &v3},
						Condition: condition{Field3: &c3},
					},
					{
						Payload:   payload{Field2: &v2},
						Condition: condition{Field2: &c2, Field3: &c3Empty},
					},
				},
				Expected: expected{
					Query: `
						START TRANSACTION;
						UPDATE table SET f1=:idx0_val_f1, f2=:idx0_val_f2 WHERE f1=:idx0_cond_f1;
						UPDATE table SET f3=:idx1_val_f3 WHERE f3 IN (:idx1_cond_f3);
						UPDATE table SET f2=:idx2_val_f2 WHERE f2=:idx2_cond_f2;
						COMMIT;
					`,
					Bind: map[string]any{
						"idx0_val_f1": v1, "idx0_val_f2": v2, "idx0_cond_f1": c1,
						"idx1_val_f3": v3, "idx1_cond_f3": c3,
						"idx2_val_f2": v2, "idx2_cond_f2": c2,
					},
				},
			},
		}

		for i, tc := range testCases {
			t.Run(fmt.Sprintf("case %v", i), func(t *testing.T) {
				query, bind, err := BuildBulkUpdateQuery(tc.Table, tc.Payload)
				expectedQuery := strings.ReplaceAll(strings.TrimSpace(tc.Expected.Query), "\t", "")
				assert.Nil(t, err)
				assert.Equal(t, expectedQuery, query)
				assert.Equal(t, tc.Expected.Bind, bind)
			})
		}
	})
}

func TestBuildUpdateQuery(t *testing.T) {

	t.Run("failed", func(t *testing.T) {
		// Payload invalid
		_, _, err := BuildUpdateQuery("", 1, 1, "")
		assert.NotNil(t, err)

		// Condition invalid
		_, _, err = BuildUpdateQuery("", payload{}, 1, "")
		assert.NotNil(t, err)

		// Condition empty
		c3Empty := []string{}
		_, _, err = BuildUpdateQuery("", payload{}, condition{Field3: &c3Empty}, "")
		assert.NotNil(t, err)
	})

	t.Run("success", func(t *testing.T) {
		table := "table"
		v1 := "v1"
		v2 := "v2"
		v3 := "v3"
		c1 := "v1"
		c2 := 12
		c3 := []string{"v3_1", "v3_2", "v3_3"}
		c3Empty := []string{}

		testCases := []struct {
			Table     string
			PrefixID  string
			Update    payload
			Condition condition
			Expected  expected
		}{
			{
				Table:     table,
				Update:    payload{Field1: &v1, Field2: &v2, Field3: &v3},
				Condition: condition{Field1: &c1, Field2: &c2, Field3: &c3},
				Expected: expected{
					Query: "UPDATE table SET f1=:val_f1, f2=:val_f2, f3=:val_f3 WHERE f1=:cond_f1 AND f2=:cond_f2 AND f3 IN (:cond_f3)",
					Bind:  map[string]any{"val_f1": v1, "val_f2": v2, "val_f3": v3, "cond_f1": c1, "cond_f2": c2, "cond_f3": c3},
				},
			},
			{
				Table:     table,
				PrefixID:  "1",
				Update:    payload{Field1: &v1, Field2: &v2, Field3: &v3},
				Condition: condition{Field1: &c1, Field2: &c2, Field3: &c3Empty},
				Expected: expected{
					Query: "UPDATE table SET f1=:idx1_val_f1, f2=:idx1_val_f2, f3=:idx1_val_f3 WHERE f1=:idx1_cond_f1 AND f2=:idx1_cond_f2",
					Bind:  map[string]any{"idx1_val_f1": v1, "idx1_val_f2": v2, "idx1_val_f3": v3, "idx1_cond_f1": c1, "idx1_cond_f2": c2},
				},
			},
		}

		for i, tc := range testCases {
			t.Run(fmt.Sprintf("case %v", i), func(t *testing.T) {
				query, bind, err := BuildUpdateQuery(tc.Table, tc.Update, tc.Condition, tc.PrefixID)
				assert.Nil(t, err)
				assert.Equal(t, tc.Expected.Query, query)
				assert.Equal(t, tc.Expected.Bind, bind)
			})
		}
	})
}

func TestBuildCreateQuery(t *testing.T) {

	t.Run("failed", func(t *testing.T) {
		_, _, err := BuildCreateQuery("", 1)
		assert.NotNil(t, err)
	})

	t.Run("success", func(t *testing.T) {
		table := "table"
		v1 := "v1"
		v2 := "v2"
		v3 := "v3"

		testCases := []struct {
			Table    string
			Input    payload
			Expected expected
		}{
			{
				Table: table,
				Input: payload{Field1: &v1},
				Expected: expected{
					Query: "INSERT INTO table (f1) VALUES (:f1)",
					Bind:  map[string]any{"f1": v1},
				},
			},
			{
				Table: table,
				Input: payload{Field1: &v1, Field2: &v2},
				Expected: expected{
					Query: "INSERT INTO table (f1, f2) VALUES (:f1, :f2)",
					Bind:  map[string]any{"f1": v1, "f2": v2},
				},
			},
			{
				Table: table,
				Input: payload{Field1: &v1, Field2: &v2, Field3: &v3},
				Expected: expected{
					Query: "INSERT INTO table (f1, f2, f3) VALUES (:f1, :f2, :f3)",
					Bind:  map[string]any{"f1": v1, "f2": v2, "f3": v3},
				},
			},
		}

		for i, tc := range testCases {
			t.Run(fmt.Sprintf("case %v", i), func(t *testing.T) {
				query, bind, err := BuildCreateQuery(tc.Table, tc.Input)
				assert.Nil(t, err)
				assert.Equal(t, tc.Expected.Query, query)
				assert.Equal(t, tc.Expected.Bind, bind)
			})
		}
	})
}

func TestBuildSelectQuery(t *testing.T) {
	t.Run("failed", func(t *testing.T) {
		// Empty select
		_, _, err := BuildSelectQuery("", []string{}, new(map[string]any), nil)
		assert.NotNil(t, err)

		// Invalid condition
		_, _, err = BuildSelectQuery("", []string{"*"}, new(map[string]any), nil)
		assert.NotNil(t, err)
	})

	t.Run("success", func(t *testing.T) {
		table := "table"
		c1 := "v1"
		c2 := 12
		c3 := []string{"v3_1", "v3_2", "v3_3"}
		c3Empty := []string{}

		testCases := []struct {
			Table     string
			Fields    []string
			Condition *condition
			Paginate  *utils.Paginate
			Expected  expected
		}{
			{
				Table:     table,
				Fields:    []string{"*"},
				Condition: &condition{Field1: &c1, Field2: &c2, Field3: &c3},
				Paginate:  &utils.Paginate{Page: 3, Limit: 10},
				Expected: expected{
					Query: "SELECT * FROM table WHERE f1=:cond_f1 AND f2=:cond_f2 AND f3 IN (:cond_f3) LIMIT :paginate_limit OFFSET :paginate_offset",
					Bind:  map[string]any{"cond_f1": c1, "cond_f2": c2, "cond_f3": c3, "paginate_offset": 20, "paginate_limit": 10},
				},
			},
			{
				Table:     table,
				Fields:    []string{"*"},
				Condition: &condition{Field3: &c3Empty},
				Paginate:  &utils.Paginate{Page: 4, Limit: 10},
				Expected: expected{
					Query: "SELECT * FROM table LIMIT :paginate_limit OFFSET :paginate_offset",
					Bind:  map[string]any{"paginate_offset": 30, "paginate_limit": 10},
				},
			},
			{
				Table:  table,
				Fields: []string{"username", "email"},
				Expected: expected{
					Query: "SELECT email, username FROM table",
					Bind:  map[string]any{},
				},
			},
			{
				Table:     table,
				Fields:    []string{"username", "email"},
				Condition: &condition{Field1: &c1},
				Expected: expected{
					Query: "SELECT email, username FROM table WHERE f1=:cond_f1",
					Bind:  map[string]any{"cond_f1": c1},
				},
			},
		}
		for i, tc := range testCases {
			t.Run(fmt.Sprintf("case %v", i), func(t *testing.T) {
				query, bind, err := BuildSelectQuery(tc.Table, tc.Fields, tc.Condition, tc.Paginate)
				assert.Nil(t, err)
				assert.Equal(t, tc.Expected.Query, query)
				assert.Equal(t, tc.Expected.Bind, bind)
			})
		}
	})
}

func TestBuildCountQuery(t *testing.T) {
	t.Run("failed", func(t *testing.T) {
		// Invalid payload
		_, _, err := BuildCountQuery("", new(map[string]any))
		assert.NotNil(t, err)
	})

	t.Run("success", func(t *testing.T) {
		table := "table"
		c1 := "v1"
		c2 := 12
		c3 := []string{"v3_1", "v3_2", "v3_3"}
		c3Empty := []string{}

		testCases := []struct {
			Table     string
			Condition *condition
			Expected  expected
		}{
			{
				Table: table,
				Expected: expected{
					Query: "SELECT COUNT(*) FROM table",
					Bind:  map[string]any{},
				},
			},
			{
				Table:     table,
				Condition: &condition{Field1: &c1, Field3: &c3Empty},
				Expected: expected{
					Query: "SELECT COUNT(*) FROM table WHERE f1=:cond_f1",
					Bind:  map[string]any{"cond_f1": c1},
				},
			},
			{
				Table:     table,
				Condition: &condition{Field2: &c2, Field3: &c3},
				Expected: expected{
					Query: "SELECT COUNT(*) FROM table WHERE f2=:cond_f2 AND f3 IN (:cond_f3)",
					Bind:  map[string]any{"cond_f2": c2, "cond_f3": c3},
				},
			},
		}

		for i, tc := range testCases {
			t.Run(fmt.Sprintf("case %v", i), func(t *testing.T) {
				query, bind, err := BuildCountQuery(tc.Table, tc.Condition)
				assert.Nil(t, err)
				assert.Equal(t, tc.Expected.Query, query)
				assert.Equal(t, tc.Expected.Bind, bind)
			})
		}
	})
}

func TestBuildDeleteQuery(t *testing.T) {

	t.Run("failed", func(t *testing.T) {
		// Invalid payload
		_, _, err := BuildDeleteQuery("", 1)
		assert.NotNil(t, err)

		// Condition empty
		c3Empty := []string{}
		_, _, err = BuildDeleteQuery("", condition{Field3: &c3Empty})
		assert.NotNil(t, err)
	})

	t.Run("success", func(t *testing.T) {
		table := "table"
		v1 := "v1"
		v2 := 12
		v3 := []string{"v3_1", "v3_2", "v3_3"}
		v3Empty := []string{}

		testCases := []struct {
			Table     string
			Condition condition
			PrefixID  string
			Expected  expected
		}{
			{
				Table:     table,
				Condition: condition{Field1: &v1, Field2: &v2, Field3: &v3},
				Expected: expected{
					Query: "DELETE FROM table WHERE f1=:cond_f1 AND f2=:cond_f2 AND f3 IN (:cond_f3)",
					Bind:  map[string]any{"cond_f1": v1, "cond_f2": v2, "cond_f3": v3},
				},
			},
			{
				Table:     table,
				Condition: condition{Field1: &v1, Field2: &v2, Field3: &v3Empty},
				Expected: expected{
					Query: "DELETE FROM table WHERE f1=:cond_f1 AND f2=:cond_f2",
					Bind:  map[string]any{"cond_f1": v1, "cond_f2": v2},
				},
			},
		}

		for i, tc := range testCases {
			t.Run(fmt.Sprintf("case %v", i), func(t *testing.T) {
				query, bind, err := BuildDeleteQuery(tc.Table, tc.Condition)
				assert.Nil(t, err)
				assert.Equal(t, tc.Expected.Query, query)
				assert.Equal(t, tc.Expected.Bind, bind)
			})
		}
	})
}

func TestBuildCondition(t *testing.T) {

	t.Run("failed", func(t *testing.T) {
		_, _, err := BuildCondition(1, "")
		assert.NotNil(t, err)
	})

	t.Run("success", func(t *testing.T) {

		v1 := "v1"
		v2 := 12
		v3 := []string{"v3_1", "v3_2", "v3_3"}
		v3Empty := []string{}

		testCases := []struct {
			Condition condition
			PrefixID  string
			Expected  expected
		}{
			{
				Condition: condition{Field1: &v1, Field2: &v2, Field3: &v3},
				Expected: expected{
					Query: "f1=:cond_f1 AND f2=:cond_f2 AND f3 IN (:cond_f3)",
					Bind:  map[string]any{"cond_f1": v1, "cond_f2": v2, "cond_f3": v3},
				},
			},
			{
				Condition: condition{Field1: &v1, Field2: &v2, Field3: &v3Empty},
				Expected: expected{
					Query: "f1=:cond_f1 AND f2=:cond_f2",
					Bind:  map[string]any{"cond_f1": v1, "cond_f2": v2},
				},
			},
			{
				Condition: condition{Field1: &v1, Field2: &v2},
				PrefixID:  "1",
				Expected: expected{
					Query: "f1=:idx1_cond_f1 AND f2=:idx1_cond_f2",
					Bind:  map[string]any{"idx1_cond_f1": v1, "idx1_cond_f2": v2},
				},
			},
		}

		for i, tc := range testCases {
			t.Run(fmt.Sprintf("case %v", i), func(t *testing.T) {
				query, bind, err := BuildCondition(tc.Condition, tc.PrefixID)
				assert.Nil(t, err)
				assert.Equal(t, tc.Expected.Query, query)
				assert.Equal(t, tc.Expected.Bind, bind)
			})
		}

	})
}

func TestBindNamedQuery(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		testCases := []struct {
			Query       string
			Param       map[string]any
			ResultQuery string
			Bind        []any
		}{
			{
				Query:       "INSERT INTO table (a,b,c) VALUES (:a,:b,:c)",
				Param:       map[string]any{"a": 1, "b": 2, "c": 3},
				ResultQuery: "INSERT INTO table (a,b,c) VALUES (?,?,?)",
				Bind:        []any{1, 2, 3},
			},
		}
		for i, tc := range testCases {
			t.Run(fmt.Sprintf("case %v", i), func(t *testing.T) {
				query, bind, err := BindNamedQuery(tc.Query, tc.Param)
				assert.Nil(t, err)
				assert.Equal(t, tc.ResultQuery, query)
				assert.Equal(t, tc.Bind, bind)
			})
		}
	})
}
