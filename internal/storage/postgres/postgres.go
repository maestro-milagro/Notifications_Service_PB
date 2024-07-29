package postgres

import (
	"context"
	"fmt"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
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
