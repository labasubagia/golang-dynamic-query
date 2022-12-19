package repo

import (
	"bulk/db/sql"
	"bulk/utils"
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"
)

const ProductTable = "products"

type ProductModel struct {
	ID    *int     `db:"id"`
	SKU   *string  `db:"sku"`
	Name  *string  `db:"name"`
	Price *float64 `db:"price"`
	Qty   *int     `db:"qty"`
}

type ProductPayload struct {
	ID    *int     `db:"id"`
	SKU   *string  `db:"sku"`
	Name  *string  `db:"name"`
	Price *float64 `db:"price"`
	Qty   *int     `db:"qty"`
}

type ProductCondition struct {
	ID   *int      `db:"id"`
	IDs  *[]int    `db:"id"`
	SKU  *string   `db:"sku"`
	SKUs *[]string `db:"sku"`
}

type ProductRepo interface {
	Table() string
	Select(fields []string, condition *ProductCondition, paginate *utils.Paginate) (utils.Result[ProductModel], error)
	Create(payload ProductPayload) error
	CreateBulk(payload []ProductPayload) (fails []ProductPayload, err error)
	Update(payload ProductPayload, condition ProductCondition) error
	UpdateBulk(payload []sql.Update[ProductPayload, ProductCondition]) (fails []sql.Update[ProductPayload, ProductCondition], err error)
	Delete(condition ProductCondition) error
}

type repo struct {
	db *sqlx.DB
}

func NewProductSQLRepo(db *sqlx.DB) ProductRepo {
	return &repo{db: db}
}

func (r *repo) Table() string {
	return ProductTable
}

func (r *repo) Select(fields []string, condition *ProductCondition, paginate *utils.Paginate) (result utils.Result[ProductModel], err error) {
	empty := utils.Result[ProductModel]{Data: []ProductModel{}}

	// Result data
	query, param, err := sql.BuildSelectQuery(r.Table(), fields, condition, paginate)
	if err != nil {
		return empty, fmt.Errorf("failed build query: %w", err)
	}
	query, args, err := sql.BindNamedQuery(query, param)
	if err != nil {
		return empty, fmt.Errorf("failed bind named query: %w", err)
	}
	data := []ProductModel{}
	if err := r.db.Select(&data, query, args...); err != nil {
		return empty, fmt.Errorf("failed select db: %w", err)
	}

	// Total
	query, param, err = sql.BuildCountQuery(r.Table(), condition)
	if err != nil {
		return empty, fmt.Errorf("failed build count query: %w", err)
	}
	query, args, err = sql.BindNamedQuery(query, param)
	if err != nil {
		return empty, fmt.Errorf("failed bind count named query: %w", err)
	}
	total := 0
	if err := r.db.Get(&total, query, args...); err != nil {
		return empty, fmt.Errorf("failed count data: %w", err)
	}

	return utils.Pagination(data, total, paginate), nil
}

func (r *repo) Create(payload ProductPayload) error {
	query, param, err := sql.BuildCreateQuery(r.Table(), payload)
	if err != nil {
		return fmt.Errorf("failed build query: %w", err)
	}
	query, args, err := sql.BindNamedQuery(query, param)
	if err != nil {
		return fmt.Errorf("failed bind named query: %w", err)
	}
	if _, err := r.db.Exec(query, args...); err != nil {
		return fmt.Errorf("failed insert db: %w", err)
	}
	return nil
}

func (r *repo) CreateBulk(payload []ProductPayload) (fails []ProductPayload, err error) {
	empty := []ProductPayload{}
	if len(payload) <= 0 {
		return empty, errors.New("payload is required")
	}
	query, _, err := sql.BuildCreateQuery(r.Table(), payload[0])
	if err != nil {
		return empty, fmt.Errorf("failed build query: %w", err)
	}
	if _, err := r.db.NamedExec(query, payload); err != nil {
		return empty, fmt.Errorf("failed insert db: %w", err)
	}
	if len(fails) > 0 {
		return empty, errors.New("input fails")
	}
	return fails, nil
}

func (r *repo) UpdateBulk(payload []sql.Update[ProductPayload, ProductCondition]) (fails []sql.Update[ProductPayload, ProductCondition], err error) {
	fails = []sql.Update[ProductPayload, ProductCondition]{}
	for _, v := range payload {
		if err := r.Update(v.Payload, v.Condition); err != nil {
			fails = append(fails, v)
		}
	}
	if len(fails) > 0 {
		return fails, errors.New("update bulk fail")
	}
	return nil, nil
}

func (r *repo) Update(payload ProductPayload, condition ProductCondition) error {
	query, param, err := sql.BuildUpdateQuery(r.Table(), payload, condition, "")
	if err != nil {
		return fmt.Errorf("failed build query: %w", err)
	}
	query, args, err := sql.BindNamedQuery(query, param)
	if err != nil {
		return fmt.Errorf("failed bind named query: %w", err)
	}
	if _, err := r.db.Exec(query, args...); err != nil {
		return fmt.Errorf("failed update db: %w", err)
	}
	return nil
}

func (r *repo) Delete(condition ProductCondition) error {
	query, param, err := sql.BuildDeleteQuery(r.Table(), condition)
	if err != nil {
		return fmt.Errorf("failed build query: %w", err)
	}
	query, args, err := sql.BindNamedQuery(query, param)
	if err != nil {
		return fmt.Errorf("failed bind named query: %w", err)
	}
	if _, err := r.db.Exec(query, args...); err != nil {
		return fmt.Errorf("failed delete db: %w", err)
	}
	return nil
}
