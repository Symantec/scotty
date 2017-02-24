package queuesender

import (
	"sync"
)

type interruptType struct {
	lock sync.Mutex
	id   uint64
}

func (i *interruptType) Id() uint64 {
	i.lock.Lock()
	defer i.lock.Unlock()
	return i.id
}

func (i *interruptType) Interrupt() {
	i.lock.Lock()
	defer i.lock.Unlock()
	i.id++
}
