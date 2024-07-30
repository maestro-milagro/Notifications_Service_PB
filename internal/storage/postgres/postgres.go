package postgres

import (
	"context"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"github.com/maestro-milagro/Notifications_Service_PB/internal/models"
	"github.com/maestro-milagro/Notifications_Service_PB/internal/storage"
)

type Storage struct {
	db *sqlx.DB
}

type Config struct {
	Host     string
	Port     string
	Username string
	Password string
	DBName   string
	SSLMode  string
}

func New(cfg Config) (*Storage, error) {
	const op = "storage.postgres.New"

	db, err := sqlx.Open("postgres", fmt.Sprintf("host=%s port=%s user=%s dbname=%s password=%s sslmode=%s",
		cfg.Host, cfg.Port, cfg.Username, cfg.DBName, cfg.Password, cfg.SSLMode))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return &Storage{db: db}, nil
}

func (s *Storage) WhoSubbedDB(ctx context.Context, email string) ([]int, error) {
	const op = "Storage/postgres/WhoSubbedDB"

	var subs []int

	createListQuery := fmt.Sprintf("SELECT s.sub_id FROM subscriptions AS s LEFT JOIN users AS u ON s.uid = u.id WHERE u.email = $1")

	if err := s.db.Select(&subs, createListQuery, email); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return subs, nil
}

func (s *Storage) GetAllDB(ctx context.Context) ([]models.Post, error) {
	const op = "Storage/postgres/GetAllDB"

	var posts []models.Post

	createListQuery := fmt.Sprintf("SELECT posts.post_id, posts.email FROM posts")

	if err := s.db.Select(&posts, createListQuery); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return posts, nil
}

func (s *Storage) PostSaveDB(ctx context.Context, post models.Post) (int64, error) {
	const op = "Storage/postgres/PostSaveDB"

	tx, err := s.db.Begin()
	if err != nil {
		return 0, fmt.Errorf("%s: %w", op, err)
	}

	createListQuery := fmt.Sprintf("INSERT INTO posts (post_id, email) VALUES ($1, $2)")
	if _, err := tx.Exec(createListQuery, post.PostID, post.Email); err != nil {
		switch e := err.(type) {
		case *pq.Error:
			switch e.Code {
			case "23505":
				// p-key constraint violation
				tx.Rollback()
				return 0, fmt.Errorf("%s: %w", op, storage.ErrPostAlreadyExists)
			default:
				tx.Rollback()
				return 0, fmt.Errorf("%s: %w", op, err)
			}
		}
	}

	return int64(post.PostID), tx.Commit()
}
