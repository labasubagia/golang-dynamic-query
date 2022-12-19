package main

import (
	"bulk/db/sql"
	"bulk/repo"
	"fmt"
	"log"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

func main() {
	// Database
	sqlDB, err := sqlx.Connect("mysql", "root:root@tcp(localhost:3307)/tmp?multiStatements=true")
	if err != nil {
		err = fmt.Errorf("failed to connect database: %w", err)
		log.Fatalln(err)
	}

	productRepo := repo.NewProductSQLRepo(sqlDB)

	length := 15_000
	inputs := []sql.Update[repo.ProductPayload, repo.ProductCondition]{}
	for i := 1; i <= length; i++ {
		SKU := fmt.Sprintf("sku_%v", i)
		name := fmt.Sprintf("product_%v", i)
		qty := 5
		price := 10_000.0
		payload := repo.ProductPayload{SKU: &SKU, Name: &name, Qty: &qty, Price: &price}
		condition := repo.ProductCondition{SKU: &SKU}
		item := sql.Update[repo.ProductPayload, repo.ProductCondition]{Payload: payload, Condition: condition}
		inputs = append(inputs, item)
	}
	fails, err := productRepo.UpdateBulk(inputs)
	if err != nil {
		err = fmt.Errorf("failed to create product: %w", err)
		log.Fatalln(err, fails)
	}

	// READ
	// SKUs := []string{"c134"}
	// result, err := productRepo.Select(
	// 	[]string{"id", "sku", "name", "price", "qty"},
	// 	&repo.ProductCondition{SKUs: &SKUs},
	// 	// nil,
	// 	&utils.Paginate{Page: 1, Limit: 2},
	// )
	// if err != nil {
	// 	err = fmt.Errorf("failed to create product: %w", err)
	// 	log.Fatalln(err)
	// }

	// v, _ := json.MarshalIndent(result, "", "  ")
	// fmt.Println(string(v))
}
