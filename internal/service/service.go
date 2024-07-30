package service

import (
	"context"
	"errors"
	"fmt"
	"github.com/maestro-milagro/Notifications_Service_PB/internal/lib/sl"
	"github.com/maestro-milagro/Notifications_Service_PB/internal/models"
	"github.com/maestro-milagro/Notifications_Service_PB/internal/storage"
	"log/slog"
)

var (
	ErrNoFollowers       = errors.New("no followers found")
	ErrPostAlreadyExists = errors.New("post already exists")
)

type Service struct {
	log         *slog.Logger
	dbWhoSubbed DBWhoSubbed
	dbAllGetter DBAllGetter
	dbPostSaver DBPostSaver
}

func New(log *slog.Logger, dbWhoSubbed DBWhoSubbed, dbAllGetter DBAllGetter, dbPostSaver DBPostSaver) *Service {
	return &Service{
		log:         log,
		dbWhoSubbed: dbWhoSubbed,
		dbAllGetter: dbAllGetter,
		dbPostSaver: dbPostSaver,
	}
}

type DBAllGetter interface {
	GetAllDB(ctx context.Context) ([]models.Post, error)
}

type DBWhoSubbed interface {
	WhoSubbedDB(ctx context.Context, email string) ([]int, error)
}

type DBPostSaver interface {
	PostSaveDB(ctx context.Context, user models.Post) (int64, error)
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
		log.Warn("no followers were found", ErrNoFollowers)
	}
	return subbs, nil
}

func (s *Service) GetAll(ctx context.Context) ([]models.Post, error) {
	const op = "service.GetAll"

	log := s.log.With(
		slog.String("op", op),
	)

	log.Info("getting all")

	userPost, err := s.dbAllGetter.GetAllDB(ctx)
	if err != nil {
		log.Error("error while getting all", sl.Err(err))

		return []models.Post{}, fmt.Errorf("%s: %w", op, err)
	}
	if len(userPost) == 0 {
		return []models.Post{}, fmt.Errorf("%s: %w", op, ErrNoFollowers)
	}
	return userPost, nil
}

func (s *Service) SavePost(ctx context.Context, post models.Post) (int64, error) {
	const op = "service.SavePost"

	log := s.log.With(
		slog.String("op", op),
		slog.Int("user", post.PostID),
	)

	log.Info("saving post")

	id, err := s.dbPostSaver.PostSaveDB(ctx, post)
	if err != nil {
		if errors.Is(err, storage.ErrPostAlreadyExists) {
			log.Error("duplicate post is send", ErrPostAlreadyExists)
		}
		log.Error("error while saving post", sl.Err(err))

		return 0, fmt.Errorf("%s: %w", op, err)
	}
	return id, nil
}
