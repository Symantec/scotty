package keyedqueue

func (q *Queue) length() int {
	q.lock.Lock()
	defer q.lock.Unlock()
	return q.queue.Len()
}

func (q *Queue) add(element Element) {
	key := element.Key()
	q.lock.Lock()
	defer q.lock.Unlock()
	posit := q.byKey[key]
	if posit != nil {
		posit.Value = element
		return
	}
	posit = q.queue.PushBack(element)
	q.byKey[key] = posit
	q.elementsOn.Broadcast()
}

func (q *Queue) remove() Element {
	q.lock.Lock()
	defer q.lock.Unlock()
	for q.queue.Len() == 0 {
		q.elementsOn.Wait()
	}
	removedElement := q.queue.Remove(q.queue.Front()).(Element)
	delete(q.byKey, removedElement.Key())
	return removedElement
}
