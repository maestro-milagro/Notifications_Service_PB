package service

import (
	"context"
	"errors"
	"fmt"
	"github.com/maestro-milagro/Notifications_Service_PB/internal/lib/sl"
	"log/slog"
)

var (
	ErrNoFollowers = errors.New("no followers found")
)

type Service struct {
	log         *slog.Logger
	dbWhoSubbed DBWhoSubbed
}

func New(log *slog.Logger, dbWhoSubbed DBWhoSubbed) *Service {
	return &Service{
		log:         log,
		dbWhoSubbed: dbWhoSubbed,
	}
}

type DBWhoSubbed interface {
	WhoSubbedDB(ctx context.Context, email string) ([]int, error)
}

func (s *Service) WhoSubbed(ctx context.Context, email string) ([]int, error) {
	const op = "service.WhoSubbedDB"

	log := s.log.With(
		slog.String("op", op),
	)

	log.Info("searching for subbs")

	subbs, err := s.dbWhoSubbed.WhoSubbedDB(ctx, email)
	if err != nil {
		log.Error("error while searching for subbs", sl.Err(err))

		return []int{}, fmt.Errorf("%s: %w", op, err)
	}
	if len(subbs) == 0 {
		log.Warn("no followers were found")
	}
	return subbs, nil
}
