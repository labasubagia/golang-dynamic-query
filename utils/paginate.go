package utils

const (
	DefaultPage  = 1
	DefaultLimit = 10
)

type Paginate struct {
	Page  int
	Limit int
}

func (p *Paginate) GetOffset() int {
	if p.Page < 1 {
		return 0
	}
	return (p.Page - 1) * p.Limit
}

type Result[T any] struct {
	Data  []T `json:"data"`
	Page  int `json:"page"`
	Limit int `json:"limit"`
	Total int `json:"total"`
}

func Pagination[T any](data []T, total int, paginate *Paginate) Result[T] {
	result := Result[T]{
		Data:  data,
		Page:  DefaultPage,
		Limit: total,
		Total: total,
	}
	if paginate == nil {
		return result
	}

	result.Page = paginate.Page
	result.Limit = paginate.Limit

	return result
}
