package consul

import (
	"github.com/Symantec/scotty/store"
	"log"
)

func NewCoordinator(logger *log.Logger) (store.Coordinator, error) {
	return newCoordinator(logger)
}
