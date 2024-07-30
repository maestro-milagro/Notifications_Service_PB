package storage

import "errors"

var (
	ErrNoFollowers       = errors.New("no followers found")
	ErrUserNotFound      = errors.New("user not found")
	ErrPostAlreadyExists = errors.New("post already exists")
)
