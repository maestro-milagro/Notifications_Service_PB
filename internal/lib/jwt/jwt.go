package jwt

import (
	"fmt"
	"github.com/golang-jwt/jwt/v5"
	"github.com/maestro-milagro/Notifications_Service_PB/internal/lib/sl"
	"log/slog"
)

func VerifyToken(log *slog.Logger, secret string, token string) (string, error) {
	const op = "lib.jwt.VerifyToken"

	tok := jwt.New(jwt.SigningMethodHS256)

	claims := tok.Claims.(jwt.MapClaims)

	keyFunc := func(t *jwt.Token) (interface{}, error) {
		// Проверяем, что используется ожидаемый метод подписи
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("Unexpected signature method: %v", t.Header["alg"])
		}
		// Возвращаем секретный ключ для jwt токена, в формате []byte, совпадающий с ключом, использованным для подписи ранее
		return []byte(secret), nil
	}

	parsedToken, err := jwt.ParseWithClaims(token, claims, keyFunc)
	if err != nil {
		log.Error("failed to parse token", sl.Err(err))

		return "", fmt.Errorf("%s: %w", op, err)
	}

	if !parsedToken.Valid {
		log.Error("token is invalid", sl.Err(err))

		return "", fmt.Errorf("%s: %w", op, err)
	}

	return claims["email"].(string), nil
}
