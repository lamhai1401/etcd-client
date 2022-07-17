package client

import (
	"context"
	"time"
)

func GetCtxTimeout(second int) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), time.Duration(second)*time.Second)
}
