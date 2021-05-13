package mr

import "sync"
import "fmt"
import "time"
import "os"
import "strconv"
import "log"

const (
	EXCEPTION int = -1
	MAPTASK int = 1 // start from 0 to avoid worker having no response.
	REDUCETASK int = 2
	WAIT int = 3
	FINISHED int = 4

	IDLE int = 0
	INPROGRESS int = 1
	TASKFINISHED int = 2
)

type Task struct {
	TaskType int
	TaskId int
	TaskStatus int
	TaskFileName string
	LastModified time.Time
}

type TaskQueue struct {
	TaskList []Task
	NumberOfTaskLeft int
    Mut sync.Mutex // avoid generating multiple same thread for same task
}

// Consider as over time if no response for more than 30 seconds.
func (t *Task) Overtime() bool{
	return (*t).LastModified.Sub(time.Now()) > time.Duration(50) * time.Second
} 

func (tq *TaskQueue) Lock() {
	(*tq).Mut.Lock()
}

func (tq *TaskQueue) UnLock() {
	(*tq).Mut.Unlock()
}

func (tq *TaskQueue) Pop() Task {
	tq.Lock()
	defer tq.UnLock()

	if tq.NumberOfTaskLeft == 0 {
		return Task{TaskType:EXCEPTION}
	}

	
	// res := tq.TaskList[0]
	// tq.TaskList = tq.TaskList[1:len(tq.TaskList)]
	res := Task{TaskType:EXCEPTION}
	for i, task := range (*tq).TaskList {
		if task.TaskStatus == IDLE || task.Overtime()	{
			res = task
			// task.TaskStatus = INPROGRESS
			(*tq).TaskList[i].TaskStatus = INPROGRESS
			(*tq).TaskList[i].LastModified = time.Now()
			break	
		}
	}


	// All tasks are currently in progress or need to be recorvery
	if res.TaskType == EXCEPTION  {
		res = Task{TaskType:WAIT}
	}
	return res
}

func (tq *TaskQueue) Submit(task Task) bool{
	tq.Lock()
	defer tq.UnLock()

	if (*tq).TaskList[task.TaskId].TaskStatus != INPROGRESS {
		fmt.Printf("Task %d status is not in progress!\n", task.TaskId)
		return false
	}
	(*tq).TaskList[task.TaskId].TaskStatus = TASKFINISHED
	(*tq).NumberOfTaskLeft -= 1
	return true
}

func (tq *TaskQueue) Add(task Task) {
	tq.Lock()
	defer tq.UnLock()

	tq.TaskList = append(tq.TaskList, task)	
	tq.NumberOfTaskLeft += 1
}

func (tq *TaskQueue) Restore(task Task) bool{
	taskIndex := task.TaskId
	if taskIndex >= len(tq.TaskList) {
		return false
	}

	tq.TaskList[taskIndex].TaskStatus = IDLE
	tq.TaskList[taskIndex].LastModified = time.Now()	
	return true
}

func InitTaskQueue(tasks []Task) TaskQueue {
	res := TaskQueue{TaskList: tasks}
	res.NumberOfTaskLeft = len(tasks)
	return res
}

func (tq *TaskQueue) IsEmpty() bool {
	return len(tq.TaskList) == 0
}

func (tq *TaskQueue) AllTaskFinished() bool {
	tq.Lock()
	defer tq.UnLock()

	return tq.NumberOfTaskLeft == 0
}

func (tq *TaskQueue) RenameTmpFiles(task Task, nReduce int) error {
	tq.Lock()
	defer tq.UnLock()

	// rename corresponding tmp files.
	for i := 0; i < nReduce; i++ {
		ofname := "tmp-mr-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i)
		newname := "mr-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i)
		err := os.Rename(ofname, newname)
		if err != nil {
			log.Fatal("Fail to rename file ", err)
			return err
		}
	}
	return nil
}