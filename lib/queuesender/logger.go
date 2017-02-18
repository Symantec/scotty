package queuesender

import (
	"log"
)

// loggerType logs errors.
type loggerType struct {
	logger *log.Logger
	// the prefix for each log
	name string
}

// Log logs the error. Calling log on a nil receiver does nothing.
func (l *loggerType) Log(err error) {
	if l == nil {
		return
	}
	l.logger.Printf("%s:%v", l.name, err)
}
