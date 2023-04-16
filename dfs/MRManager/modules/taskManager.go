package modules

import (
	"dfs/utility"
	"fmt"
	"log"
	"net"
	"plugin"
	"strconv"
	"strings"
)

type Task interface {
	Init(args []string)
	Map(index int, text string) ([]byte, []byte)
	Reduce(key []byte, values [][]byte) [][]byte
	GetNumOfReducer() int
}

type TaskStatus struct {
	Status string // idle, completed
	Worker string // host:worker_port
}

var (
	MapTasks    map[string]TaskStatus //key: chunk name,   value: TaskStatus
	ReduceTasks map[string]TaskStatus //key: p1,  value: TaskStatus
)

// get a map of (chunk : 3 nodes)
// transfer it into (node : chunks store on it)
// always assign the node with the least number of choices(chunks)
// do not assign the node if it already has more works than average
// return a map of each node and their map chunks， key: orion03:26000,  value: [chunk1, chunk3, ...]
func assignNodeWithChunks(mapredReq *utility.MapRedReq) *map[string][]string {
	// test_4.bin_2-15.cnk:	orion02:26521,orion07:26526,orion06:26525
	chunkNodeList := mapredReq.GetInputFile().GetChunkNodeList()
	// log.Println("LOG: Received chunk-node list: ", chunkNodeList) // FIXME: testing only
	chunkNum := len(chunkNodeList)
	// key: test_4.bin_2-15.cnk , value: [orion02:26521, orion07:26526, orion06:26525]

	chunkNodeMap := make(map[string][]string)
	for i := 0; i < len(chunkNodeList); i++ {
		rowElems := strings.Split(chunkNodeList[i], ":\t")
		chunkName := rowElems[0][:len(rowElems[0])]
		var workerList []string
		nodeList := strings.Split(rowElems[1], ",")
		for index := range nodeList {
			host := strings.Split(nodeList[index], ":")[0]
			workerList = append(workerList, Workers[host])
		}
		chunkNodeMap[chunkName] = workerList
	}
	// log.Println("LOG: chunkNodeMap: ", chunkNodeMap) // FIXME: testing only

	// turn into a (node : chunks) map
	// key: orion02:26521, value: [test_4.bin_2-15.cnk, test_4.bin_3-15.cnk, test_4.bin_4-15.cnk]
	nodeChunkMap := make(map[string][]string)
	for key, value := range chunkNodeMap {
		for i := 0; i < len(value); i++ {
			if _, ok := nodeChunkMap[value[i]]; !ok {
				nodeChunkMap[value[i]] = []string{}
			}
			nodeChunkMap[value[i]] = append(nodeChunkMap[value[i]], key)
		}
	}
	// log.Println("LOG: nodeChunkMap: ", nodeChunkMap) // FIXME: testing only

	nodeNum := len(nodeChunkMap)

	// always assign the node with least options
	// key: orion02:26521, value : assigned chunk array
	mapAssignment := make(map[string][]string)
	assignedChunkNum := 0
	for assignedChunkNum < chunkNum {
		// find the node with least number of unsigned chunks
		// and the node's assigned chunk number is not greater than avarage
		var minNode string
		minLen := chunkNum
		for key, value := range nodeChunkMap {
			nodeAssignedChunkNum := 0
			if _, ok := mapAssignment[key]; ok {
				nodeAssignedChunkNum = len(mapAssignment[key])
			}
			// if the node has been assigne more chunks than average
			// skip it
			if nodeAssignedChunkNum > assignedChunkNum/nodeNum {
				continue
			}
			if len(value) < minLen {
				minLen = len(value)
				minNode = key
			}
		}
		log.Println("LOG: minNode: ", minNode)

		// assign the first chunk to that node
		if _, ok := mapAssignment[minNode]; !ok {
			mapAssignment[minNode] = []string{}
		}
		chunkName := nodeChunkMap[minNode][0]
		log.Printf("LOG: assign chunk(%s) to node(%s): \n", chunkName, minNode)
		mapAssignment[minNode] = append(mapAssignment[minNode], chunkName)
		assignedChunkNum += 1
		// delete all duplicates of the chunk from the nodeChunkMap
		nodeList := chunkNodeMap[chunkName]
		for _, node := range nodeList {
			log.Printf("LOG: delete chunk(%s) from node(%s): ", chunkName, node) //
			for index, chunk := range nodeChunkMap[node] {
				if chunk == chunkName {
					if index == len(nodeChunkMap[node])-1 {
						nodeChunkMap[node] = nodeChunkMap[node][:index]
					} else {
						nodeChunkMap[node] = append(nodeChunkMap[node][:index], nodeChunkMap[node][index+1:]...)
					}
					// delete emply node
					if len(nodeChunkMap[node]) == 0 {
						delete(nodeChunkMap, node)
					}

				}
			}
		}
		// log.Println("LOG: after delete duplicates, nodeChunkMap: ", nodeChunkMap) // FIXME: testing only
	}
	// write detailed assignment to log
	log.Println("LOG: Assign each node with these chunks")
	log.Println("LOG: Node Name\tMap Task #\t Chunk List")
	for key, value := range mapAssignment {
		log.Printf("LOG: %s\t%d\t%s", key, len(value), value)
	}

	return &mapAssignment
}

// insert all keys of MapTasks and ReduceTasks
func initTasks(mapredReq *utility.MapRedReq, filePath string) error {
	// clear map
	MapTasks = make(map[string]TaskStatus)
	ReduceTasks = make(map[string]TaskStatus)
	// test_4.bin_2-15.cnk: orion02:26521,orion07:26526,orion06:26525
	chunkNodeList := mapredReq.GetInputFile().GetChunkNodeList()
	// insert all the key of mapper
	for _, chunkNode := range chunkNodeList {
		temp := strings.Split(chunkNode, ":\t")
		chunkName := temp[0]
		MapTasks[chunkName] = TaskStatus{}
	}
	// insert all the key of reducer
	// TODO: get the reducer number - DONE by steven
	r, err := getNumOfRedTask(filePath)
	if err != nil {
		return err
	}
	for i := 1; i <= r; i++ {
		reducerName := "p" + strconv.Itoa(i)
		ReduceTasks[reducerName] = TaskStatus{}
	}
	return nil
}

func getNumOfRedTask(soFilePath string) (int, error) {
	plug, err := plugin.Open(soFilePath)
	if err != nil {
		log.Println("ERROR: Can not open so file. ", err)
		return 0, err
	}
	symTask, err := plug.Lookup("Task")
	if err != nil {
		log.Println("ERROR: ", err)
		return 0, err
	}
	task, ok := symTask.(Task)
	if !ok {
		fmt.Println("ERROR: nexpected type from module symbol. ")
		return 0, fmt.Errorf("nexpected type from module symbol. ")
	}
	task.Init([]string{})
	log.Println("LOG: task init finished")
	r := task.GetNumOfReducer()
	return r, nil
}

// update which worker will process each chunk， set the map status to idle
func updateMapTasks(mapAssignment *map[string][]string) {
	log.Println("Start populating MapTasks.")
	for workerName, chunkList := range *mapAssignment {
		for _, chunkName := range chunkList {
			status := MapTasks[chunkName]
			status.Worker = workerName
			status.Status = "idle"
			MapTasks[chunkName] = status
			log.Printf("%s  :  {%s,  %s}\n", chunkName, status.Worker, status.Status)
		}
	}
	log.Println("Finish populating MapTasks.")
}

// assign reduce jobs to the mappers that did most amount of map tasks, so maximize locality
func updateReduceTasks(mapAssignment *map[string][]string) {
	log.Println("Start populating ReduceTasks.")
	// map: key-worker, value- # of map tasks
	mapCount := make(map[string]int)
	for worker, taskList := range *mapAssignment {
		mapCount[worker] = len(taskList)
	}

	// always assign the reduce tasks to the worker with the most map tasks
	for reduceTask := range ReduceTasks {
		maxMapNum := 0
		maxWorker := ""
		for worker, assignedNum := range mapCount {
			if assignedNum > maxMapNum {
				maxMapNum = assignedNum
				maxWorker = worker
			}
		}
		status := ReduceTasks[reduceTask]
		status.Status = "idle"
		status.Worker = maxWorker
		ReduceTasks[reduceTask] = status
		delete(mapCount, maxWorker)
		log.Printf("%s  :  {%s,  %s}\n", reduceTask, status.Worker, status.Status)
	}
	log.Println("Finish populating ReduceTasks.")
}

func assignMapTasks(mapAssignment *map[string][]string, soChunk *utility.Chunk, parameters []string) {
	for nodeHost, chunkList := range *mapAssignment {
		// create connection
		conn, err := createConnection(nodeHost)
		if err != nil {
			continue
		}
		msgHandler := utility.NewMessageHandler(conn)
		// wrap MapTaskReq
		reqMsg := utility.Request{
			Req: &utility.Request_MapTaskReq{
				MapTaskReq: &utility.MapTaskReq{
					SoChunk:    soChunk,
					InputList:  chunkList,
					Parameters: parameters,
				},
			},
		}
		wrapper := &utility.Wrapper{
			Msg: &utility.Wrapper_RequestMsg{
				RequestMsg: &reqMsg,
			},
		}
		// send task
		err = msgHandler.Send(wrapper)
		if err != nil {
			log.Printf("ERROR: Fail to send MapTaskReq to worker(%s). Err msg: %s \n", nodeHost, err)
			continue
		}
		msgHandler.Close()
	}
}

func createConnection(host string) (net.Conn, error) {
	conn, err := net.Dial("tcp", host)
	if err != nil {
		log.Printf("ERROR: Can't establish connection with workers. Please check host name(%s). \n", host)
		return nil, err
	}
	return conn, nil
}
