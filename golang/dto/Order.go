package dto

type OrderDto struct {
	ID        string `json:"id"`
	ProductId string `json:"product_id"`
	Amount    int    `json:"amount"`
}
