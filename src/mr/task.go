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

func TaskPoll(size int) *taskPoll {
	ts := make([]*task, size)
	for idx := range ts {
		ts[idx] = Task(idx)
	}
	return &taskPoll{
		tasks: ts,
	}
}

func (tp *taskPoll) Commit(tid int) {
	tp.nCompleted++
	for _, t := range tp.tasks {
		if t.Id == tid {
			t.setNextState()
		}
	}

	log.Printf("commit task. tid = %v \n", tid)
}

func (tp *taskPoll) GetTask() *task {
	for tp.nCompleted < len(tp.tasks) {
		for _, t := range tp.tasks {
			if t.IsReady() {
				t.setNextState()
				return t
			}
		}
		time.Sleep(1000)
	}
	return nil
}

type task struct {
	Id    int
	State int
}

func Task(id int) *task {
	return &task{Id: id, State: READE}
}

func (t *task) setNextState() {
	if t.State == READE {
		t.State = RUNNING
	} else if t.State == RUNNING {
		t.State = DONE
	}
}

func (t *task) IsReady() bool {
	return t.State == READE
}
