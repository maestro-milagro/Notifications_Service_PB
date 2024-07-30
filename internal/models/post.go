package models

type Post struct {
	PostID int    `json:"post_id" db:"post_id"`
	Email  string `json:"email" db:"email"`
}
