package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "sort"
import "os"
import "io/ioutil"
import "strconv"
import "encoding/json"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue


type TaskProcessor struct {
	AssignedTask Task
	NumReduce int
	NumberOfFiles int
	mapf func(string, string) []KeyValue
	reducef func(string, []string) string
}

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	taskProcessor := TaskProcessor{}
	// uncomment to send the Example RPC to the master.
	// CallExample()
	taskProcessor.mapf = mapf
	taskProcessor.reducef = reducef
	for {
		(&taskProcessor).getJob()
		switch taskProcessor.AssignedTask.TaskType {
			case MAPTASK:
				// fmt.Printf("! Map task %d: %s\n", taskProcessor.AssignedTask.TaskId, taskProcessor.AssignedTask.TaskFileName)
				(&taskProcessor).doJob()
			case REDUCETASK:
				// fmt.Printf("! Reduce task %d: %s\n", taskProcessor.AssignedTask.TaskId, taskProcessor.AssignedTask.TaskFileName)
				(&taskProcessor).doJob()
			case WAIT:
				// wait here
				// fmt.Printf("Wait request\n")
				
			case FINISHED:
				// fmt.Printf("Job Finished. Worker terminates\n")
				return
			default:
				log.Fatal("!!! Unexpected job type !!!")
				return
		}
	}

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//

func (t *TaskProcessor) getJob() {
	args := JobRequest{}
	reply := JobReply{}

	if ! call("Master.AssignJob", &args, &reply) {
		(*t).AssignedTask.TaskType = EXCEPTION
		return
	}
	(*t).AssignedTask = reply.AssignedTask
	t.NumReduce = reply.NumberOfReduce
	t.NumberOfFiles = reply.NumberOfFiles
}

func (t *TaskProcessor) doJob() bool{
	var err error
	if (*t).AssignedTask.TaskType == MAPTASK {
		err = t.doMap()
		err = t.submitJob()
	} else if (*t).AssignedTask.TaskType == REDUCETASK {
		err = t.doReduce()
		err = t.submitJob()
	} else {
		err = nil
	}
	
	if err != nil {
		log.Fatal("Worker doJob() err :", err)
		return false
	}

	return true
}

func (t *TaskProcessor) doReduce() error {
	// fmt.Println("Do reduce")
	kva := []KeyValue{}
	// read all intermediate files
	for i := 0; i < t.NumberOfFiles; i++ {
		ifilename := "mr-" +  strconv.Itoa(i) + "-"  + strconv.Itoa((*t).AssignedTask.TaskId)
  		ifile, err := os.Open(ifilename)
		defer ifile.Close()
		if err != nil {
			log.Fatal("Reduce task failed to open file:", ifilename)
		}
		dec := json.NewDecoder(ifile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}	
	}

	sort.Sort(ByKey(kva))
	ofilename := "mr-out-" + strconv.Itoa(t.AssignedTask.TaskId)
	ofile,err := os.Create(ofilename)
	if err != nil{
  		log.Fatalf("Creat Open File Error.")
 	}
 	defer ofile.Close()	
	i := 0
	for i < len(kva) {
  		j := i + 1
  		for j < len(kva) && kva[j].Key == kva[i].Key {
   			j++
  		}
		values := []string{}
		for k:= i; k < j ; k++ {
			values = append(values,kva[k].Value)
		}
		output := t.reducef(kva[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	return nil	
}
 
func (t *TaskProcessor) doMap() error {
	filename := t.AssignedTask.TaskFileName
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return err
	}
	file.Close()
	kva := t.mapf(filename, string(content))

	outputfiles := make([]*os.File, t.NumReduce)
	for i:= 0; i < t.NumReduce; i++ {
		ofname := "mr-" + strconv.Itoa(t.AssignedTask.TaskId) + "-" + strconv.Itoa(i)
		outputfiles[i], _ = os.Create(ofname)
		defer outputfiles[i].Close()
	}

	for _, kv := range kva {
		reduceId := ihash(kv.Key) % t.NumReduce
		enc := json.NewEncoder(outputfiles[reduceId])
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatal("Can not write to mr-%d-%d with %v", t.AssignedTask.TaskId, reduceId, outputfiles[reduceId])
		}
	}


	// open files and write the kva into corresponding subfiles.
	// fmt.Printf("Do map task\n")
	return nil
}

func (t *TaskProcessor) submitJob() error {
	args := JobRequest{AssignedTask:t.AssignedTask}
	reply := JobReply{}

	if ! call("Master.SubmitJob", &args, &reply) {
		fmt.Println("Fail at submitting job")
		return nil
	}
	return nil
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
