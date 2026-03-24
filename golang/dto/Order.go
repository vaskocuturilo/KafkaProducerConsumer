package dto

type OrderDto struct {
	ID        string `json:"id"`
	UserId    string `json:"user_id"`
	ProductId string `json:"product_id"`
	Amount    int    `json:"amount"`
}
