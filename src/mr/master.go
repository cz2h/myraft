package mr

import "fmt"
import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"


type Master struct {
	// Your definitions here.
    InputFiles []string
    NumberOfMaps int
    NumberOfReduce int

	MapTaskQueue TaskQueue
	ReduceTaskQueue TaskQueue

}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
 

func (m *Master) AssignJob(args *JobRequest, reply *JobReply) error {
	if ! m.MapTaskQueue.AllTaskFinished() {
		reply.AssignedTask = (*m).MapTaskQueue.Pop()
	} else if ! m.ReduceTaskQueue.AllTaskFinished() {
		reply.AssignedTask = (*m).ReduceTaskQueue.Pop()
	} else {
		reply.AssignedTask = Task{TaskType:FINISHED}
	}
	

	reply.NumberOfReduce = (*m).NumberOfReduce
	reply.NumberOfFiles = (*m).NumberOfMaps
	return nil
}

func (m *Master) SubmitJob(args *JobRequest, reply *JobReply) error {
	submitedJob := args.AssignedTask
	if submitedJob.TaskType == MAPTASK {
		(*m).MapTaskQueue.Submit(submitedJob)
		// TODO: MODIFY FILE NAME 
		// (*m).MapTaskQueue.RenameTmpFiles(submitedJob, m.NumberOfReduce)
	} else if submitedJob.TaskType == REDUCETASK {
		(*m).ReduceTaskQueue.Submit(submitedJob)
	} else {
		fmt.Printf("master: Unexpected job type", submitedJob, "\n")
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	return m.ReduceTaskQueue.AllTaskFinished()
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
// mainly used for initialization.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	// Your code here.
	m.InputFiles = files
	m.NumberOfMaps = len(files)
	m.NumberOfReduce = nReduce

	mapTasks := make([]Task, len(files))
	for i, fileName := range m.InputFiles {
		mapTasks[i] = Task{TaskType:MAPTASK, TaskId:i, TaskFileName:fileName, LastModified:time.Now()}
	}
	m.MapTaskQueue = InitTaskQueue(mapTasks)

	reduceTask := make([]Task, nReduce)
	for j := 0; j < nReduce; j++ {
		reduceTask[j] = Task{TaskType:REDUCETASK, TaskId:j, LastModified:time.Now()}
	}
	m.ReduceTaskQueue = InitTaskQueue(reduceTask)

	m.server()
	fmt.Println("Start m.server()")
	return &m
}
