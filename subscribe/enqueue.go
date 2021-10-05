package subscribe

import (
	"time"

	"github.com/oleiade/lane"
)

// Enqueue ensures at most n-number of calls to a function are queued up
// This prevents concurrent calls from queueing up past a certain number
// typeically 1, and allows for at least 1 call to be queued up if it is
// made during another call's execution
func Enqueue(queue *lane.Deque, id interface{}, workFunc func(id interface{}, args ...interface{}) error, args ...interface{}) error {
	if id == "" {
		id = time.Now().Format("2006-01-02T15:04:05.999999999Z07:00")
	}

	if queue.Full() {
		return nil
	}

	added := queue.Prepend(id)
	if !added {
		return nil
	}

	err := workFunc(id, args...)
	queue.Pop()

	if !queue.Empty() {
		requeue := queue.Pop()
		if err := Enqueue(queue, requeue, workFunc, args...); err != nil {
			return err
		}
	}

	if err != nil {
		return err
	}

	return nil
}
