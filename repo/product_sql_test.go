package repo

import (
	"bulk/db/sql"
	"fmt"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
)

type productSQLSuite struct {
	db   *sqlx.DB
	repo ProductRepo
}

func NewProductSQLSuite() *productSQLSuite {
	db, err := sqlx.Connect("mysql", "root:root@tcp(localhost:3307)/tmp?multiStatements=true")
	if err != nil {
		panic(err)
	}
	return &productSQLSuite{
		db:   db,
		repo: NewProductSQLRepo(db),
	}
}

func (s *productSQLSuite) Teardown() {
	s.db.Exec("DELETE FROM products")
}

func BenchmarkProductSQLRepo(b *testing.B) {
	test := NewProductSQLSuite()
	defer test.Teardown()

	length := 1000
	inputs := []ProductPayload{}
	updates := []sql.Update[ProductPayload, ProductCondition]{}
	for i := 1; i <= length; i++ {
		SKU := fmt.Sprintf("sku_%v", i)
		name := fmt.Sprintf("product_%v", i)
		price := 10.5
		qty := 10

		payload := ProductPayload{SKU: &SKU, Name: &name, Price: &price, Qty: &qty}
		condition := ProductCondition{SKU: &SKU}

		inputs = append(inputs, payload)
		updates = append(updates, sql.Update[ProductPayload, ProductCondition]{Payload: payload, Condition: condition})
	}

	b.Run("create bulk", func(b *testing.B) {
		test.repo.CreateBulk(inputs)
	})

	b.Run("update bulk", func(b *testing.B) {
		test.repo.UpdateBulk(updates)
	})
}

func TestProductSQLRepo(t *testing.T) {
	test := NewProductSQLSuite()
	defer test.Teardown()

	length := 15_000
	inputs := []ProductPayload{}
	updates := []sql.Update[ProductPayload, ProductCondition]{}
	SKUs := []string{}
	for i := 1; i <= length; i++ {
		SKU := fmt.Sprintf("sku_%v", i)
		name := fmt.Sprintf("product_%v", i)
		price := 10.5
		qty := 10

		payload := ProductPayload{SKU: &SKU, Name: &name, Price: &price, Qty: &qty}
		condition := ProductCondition{SKU: &SKU}

		inputs = append(inputs, payload)
		updates = append(updates, sql.Update[ProductPayload, ProductCondition]{Payload: payload, Condition: condition})
		SKUs = append(SKUs, SKU)
	}

	t.Run("create", func(t *testing.T) {
		err := test.repo.Create(inputs[0])
		assert.Empty(t, err)
	})

	t.Run("create bulk", func(t *testing.T) {
		fails, err := test.repo.CreateBulk(inputs[1:])
		assert.Nil(t, err)
		assert.Empty(t, fails)
	})

	t.Run("select", func(t *testing.T) {

		t.Run("all", func(t *testing.T) {
			data, err := test.repo.Select([]string{"id"}, nil, nil)
			assert.Nil(t, err)
			assert.Equal(t, length, data.Total)
		})
	})

	t.Run("update bulk", func(t *testing.T) {
		fails, err := test.repo.UpdateBulk(updates[:5])
		assert.Nil(t, err)
		assert.Empty(t, fails)
	})

	t.Run("delete", func(t *testing.T) {
		err := test.repo.Delete(ProductCondition{SKUs: &SKUs})
		assert.Nil(t, err)
	})
}
