package sql

import (
	"bulk/utils"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/jmoiron/sqlx"
)

const Tag = "db"

type Update[Payload any, Condition any] struct {
	Payload   Payload
	Condition Condition
}

func BuildBulkUpdateQuery[Payload any, Condition any](table string, inputs []Update[Payload, Condition]) (query string, bind map[string]any, err error) {
	bind = map[string]any{}
	queryArr := []string{}
	for idx, input := range inputs {
		prefixIdx := strconv.Itoa(idx)
		itemQuery, itemBind, err := BuildUpdateQuery(table, input.Payload, input.Condition, prefixIdx)
		if err != nil {
			return "", map[string]any{}, fmt.Errorf("failed to build bulk update query: %w", err)
		}
		queryArr = append(queryArr, fmt.Sprintf("%s;", itemQuery))
		for key, val := range itemBind {
			bind[key] = val
		}
	}
	query = strings.Join(queryArr, "\n")
	query = fmt.Sprintf("START TRANSACTION;\n%s\nCOMMIT;", query)
	return query, bind, nil
}

func BuildUpdateQuery[Payload any, Condition any](table string, payload Payload, condition Condition, prefixIdx string) (query string, binds map[string]any, err error) {

	// Field
	binds = make(map[string]any)
	fields := []string{}
	fieldMap, err := utils.StructToMap(payload, Tag)
	if err != nil {
		return query, binds, fmt.Errorf("failed to build update query, make field map: %w", err)
	}
	fieldKeys := utils.SortMapKeys(fieldMap)
	for _, key := range fieldKeys {
		keyBind := fmt.Sprintf("val_%s", key)
		if prefixIdx != "" {
			keyBind = fmt.Sprintf("idx%s_val_%s", prefixIdx, key)
		}
		val := fieldMap[key]
		fields = append(fields, fmt.Sprintf("%s=:%s", key, keyBind))
		binds[keyBind] = val
	}

	// Condition
	condQuery, condBind, err := BuildCondition(condition, prefixIdx)
	if err != nil {
		return query, binds, fmt.Errorf("failed to build update query, make condition map: %w", err)
	}
	if condQuery == "" {
		return query, binds, fmt.Errorf("make sure conditional not empty: %w", err)
	}
	for key, val := range condBind {
		binds[key] = val
	}

	// Query
	query = fmt.Sprintf("UPDATE %s SET %s WHERE %s", table, strings.Join(fields, ", "), condQuery)
	return query, binds, nil
}

func BuildCreateQuery[Payload any](table string, input Payload) (query string, binds map[string]any, err error) {
	binds = map[string]any{}
	fields := []string{}
	placeholders := []string{}
	fieldMap, err := utils.StructToMap(input, Tag)
	if err != nil {
		return "", map[string]any{}, fmt.Errorf("failed to build create query, make field map: %w", err)
	}
	fieldKeys := utils.SortMapKeys(fieldMap)
	for _, key := range fieldKeys {
		val := fieldMap[key]
		fields = append(fields, key)
		placeholders = append(placeholders, fmt.Sprintf(":%s", key))
		binds[key] = val
	}
	query = fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		table,
		strings.Join(fields, ", "),
		strings.Join(placeholders, ", "),
	)
	return query, binds, nil
}

func BuildSelectQuery[Condition any](table string, fields []string, condition *Condition, paginate *utils.Paginate) (query string, bind map[string]any, err error) {
	bind = map[string]any{}
	if len(fields) == 0 {
		return "", map[string]any{}, errors.New("fields required")
	}
	sort.Strings(fields)
	query = fmt.Sprintf("SELECT %s FROM %s", strings.Join(fields, ", "), table)

	// Condition
	if condition != nil {
		condQuery, condBind, err := BuildCondition(*condition, "")
		if err != nil {
			return "", map[string]any{}, fmt.Errorf("failed to build field map: %w", err)
		}
		if condQuery != "" {
			query = fmt.Sprintf("%s WHERE %s", query, condQuery)
			for k, v := range condBind {
				bind[k] = v
			}
		}
	}

	// Pagination
	if paginate != nil {
		query = fmt.Sprintf("%s LIMIT :paginate_limit OFFSET :paginate_offset", query)
		bind["paginate_offset"] = paginate.GetOffset()
		bind["paginate_limit"] = paginate.Limit
	}

	return query, bind, nil
}

func BuildCountQuery[Condition any](table string, condition *Condition) (query string, bind map[string]any, err error) {
	bind = map[string]any{}
	query = fmt.Sprintf("SELECT COUNT(*) FROM %s", table)

	// Condition
	if condition != nil {
		condQuery, condBind, err := BuildCondition(*condition, "")
		if err != nil {
			return "", map[string]any{}, fmt.Errorf("failed to build field map: %w", err)
		}
		if condQuery != "" {
			query = fmt.Sprintf("%s WHERE %s", query, condQuery)
			for k, v := range condBind {
				bind[k] = v
			}
		}
	}

	return query, bind, nil
}

func BuildDeleteQuery[Condition any](table string, condition Condition) (query string, bind map[string]any, err error) {
	condQuery, condBind, err := BuildCondition(condition, "")
	if err != nil {
		return "", map[string]any{}, fmt.Errorf("failed build condition: %w", err)
	}
	if condQuery == "" {
		return "", map[string]any{}, fmt.Errorf("make sure condition param not empty: %w", err)
	}
	query = fmt.Sprintf("DELETE FROM %s WHERE %s", table, condQuery)
	return query, condBind, nil
}

func BuildCondition[Condition any](condition Condition, prefixIdx string) (query string, bind map[string]any, err error) {
	bind = map[string]any{}
	cond := []string{}
	condMap, err := utils.StructToMap(condition, Tag)
	if err != nil {
		return "", map[string]any{}, fmt.Errorf("failed build condition: %w", err)
	}
	for _, key := range utils.SortMapKeys(condMap) {
		val := condMap[key]
		kind := reflect.TypeOf(val).Kind()
		bindKey := fmt.Sprintf("cond_%s", key)
		if prefixIdx != "" {
			bindKey = fmt.Sprintf("idx%s_cond_%s", prefixIdx, key)
		}
		str := ""
		if kind == reflect.Array || kind == reflect.Slice {
			if reflect.ValueOf(val).Len() == 0 {
				continue
			}
			str = fmt.Sprintf("%s IN (:%s)", key, bindKey)
		} else {
			str = fmt.Sprintf("%s=:%s", key, bindKey)
		}
		cond = append(cond, str)
		bind[bindKey] = val
	}
	return strings.Join(cond, " AND "), bind, nil
}

func BindNamedQuery(namedQuery string, namedParam map[string]any) (query string, args []any, err error) {
	query, args, err = sqlx.Named(namedQuery, namedParam)
	if err != nil {
		return "", []any{}, fmt.Errorf("failed bind named: %w", err)
	}
	query, args, err = sqlx.In(query, args...)
	if err != nil {
		return "", []any{}, fmt.Errorf("failed bindVar: %w", err)
	}
	return query, args, nil
}
