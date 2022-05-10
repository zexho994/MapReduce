package mr

import (
	"log"
	"time"
)

const READE = 1
const RUNNING = 2
const DONE = 3

type taskPoll struct {
	nCompleted int
	tasks      []*task
}

func (tp *taskPoll) Size() int {
	return len(tp.tasks)
}

func (tp *taskPoll) IsAllCompleted() bool {
	return tp.nCompleted == tp.Size()
}

func TaskPoll(size int) *taskPoll {
	ts := make([]*task, size)
	for idx := range ts {
		ts[idx] = Task(idx)
	}
	tp := &taskPoll{
		tasks: ts,
	}
	go checkExpiredTask(tp)

	return tp
}

func checkExpiredTask(tp *taskPoll) {
	for !tp.IsAllCompleted() {
		log.Printf("check expired task")
		for _, t := range tp.tasks {
			if t.IsExpired() {
				log.Printf("rollback task state")
				t.SetPreState()
			}
		}
		time.Sleep(2 * time.Second)
	}
}

func (tp *taskPoll) Commit(tid int) {
	for _, t := range tp.tasks {
		if t.Id == tid {
			t.setNextState()
			tp.nCompleted++
		}
	}

	log.Printf("commit task. tid = %v \n", tid)
}

func (tp *taskPoll) GetTask() *task {
	for tp.nCompleted < tp.Size() {
		for _, t := range tp.tasks {
			if t.IsReady() {
				t.setNextState()
				t.StartTime = time.Now().UTC()
				return t
			}
		}
		time.Sleep(1000)
	}
	return nil
}

type task struct {
	Id        int
	State     int
	StartTime time.Time
}

func Task(id int) *task {
	return &task{Id: id, State: READE}
}

func (t *task) SetPreState() {
	if t.State == RUNNING {
		t.State = READE
	} else {
		panic("task state is not RUNNING")
	}
}

func (t *task) setNextState() {
	if t.State == READE {
		t.State = RUNNING
	} else if t.State == RUNNING {
		t.State = DONE
	} else {
		panic("task state is not READY or RUNNING")
	}
}

func (t *task) IsReady() bool {
	return t.State == READE
}

func (t *task) IsExpired() bool {
	expired_time, _ := time.ParseDuration("-10s")
	return t.State == RUNNING && time.Now().Add(expired_time).After(t.StartTime)
}
