package modules

import (
	"dfs/utility"
	"fmt"
	"log"
	"math"
	"net"
	"plugin"
	"strconv"
	"strings"
	"sync"
)

type Task interface {
	Init(args []string)
	Map(index int, text string) ([][]byte, [][]byte)
	Reduce(key []byte, values [][]byte) []byte
	Compare(a, b string) bool
	GetNumOfReducer() int
}

type TaskStatus struct {
	Status string // idle, completed, fail
	Worker string // host:worker_port
}

var (
	mapMutex       sync.RWMutex
	MapTasks       map[string]TaskStatus // key: chunk name,   value: TaskStatus
	ReduceTasks    map[string]TaskStatus // key: p1,  value: TaskStatus
	finishedMapNum int                   // count the number of finished map tasks
	numLock        sync.RWMutex          // lock finishedMapNum
	reportFreq     int                   // >= 1, report to client once every "reportFreq" map tasks are done
	isJobFailed    bool                  // true: job failed, false: all good
	nodeCount      int                   // how many storage nodes are involved
)

func getFinishedMapNum() int {
	numLock.RLock()
	defer numLock.RUnlock()
	return finishedMapNum
}

func increFinishedMapNum() {
	numLock.Lock()
	finishedMapNum++
	numLock.Unlock()
}

func getMapTasks(key string) TaskStatus {
	mapMutex.RLock()
	status := MapTasks[key]
	mapMutex.RUnlock()
	return status
}

func setMapTasks(key string, value TaskStatus) {
	mapMutex.Lock()
	MapTasks[key] = value
	mapMutex.Unlock()
}

func getReduceTasks(key string) TaskStatus {
	mapMutex.RLock()
	status := ReduceTasks[key]
	mapMutex.RUnlock()
	return status
}

func setReduceTasks(key string, value TaskStatus) {
	mapMutex.Lock()
	ReduceTasks[key] = value
	mapMutex.Unlock()
}

func checkRedJobDone() bool {
	mapMutex.RLock()
	defer mapMutex.RUnlock()
	for taskId := range ReduceTasks {
		status := ReduceTasks[taskId]
		if status.Status != "completed" {
			return false
		}
	}
	return true
}

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
	nodeCount = len(nodeChunkMap)
	// log.Println("LOG: nodeChunkMap: ", nodeChunkMap) // FIXME: testing only

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
			// if the node has been assigned more chunks than average
			// skip it
			if nodeAssignedChunkNum > getAvgJobNumOnNodes(nodeChunkMap, mapAssignment) {
				continue
			}
			if len(value) <= minLen {
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
	// clear count
	finishedMapNum = 0
	isJobFailed = false

	// test_4.bin_2-15.cnk: orion02:26521,orion07:26526,orion06:26525
	chunkNodeList := mapredReq.GetInputFile().GetChunkNodeList()
	// insert all the key of mapper
	for _, chunkNode := range chunkNodeList {
		temp := strings.Split(chunkNode, ":\t")
		chunkName := temp[0]
		MapTasks[chunkName] = TaskStatus{}
	}

	// set report frequency to the ceil of total map num / 10
	reportFreq = int(math.Ceil(float64(len(MapTasks)) / float64(10)))

	// get reducer number
	r, err := getNumOfRedTask(filePath)
	if err != nil {
		return err
	}
	// reducer number must <= associated storage nodes number
	if r > nodeCount {
		r = nodeCount
	}
	// insert all the key of reducer
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
	log.Println("LOG: Start populating MapTasks.")
	for workerName, chunkList := range *mapAssignment {
		for _, chunkName := range chunkList {
			status := MapTasks[chunkName]
			status.Worker = workerName
			status.Status = "idle"
			MapTasks[chunkName] = status
			log.Printf("LOG: %s  :  {%s,  %s}\n", chunkName, status.Worker, status.Status)
		}
	}
	log.Println("LOG: Finish populating MapTasks.")
}

// assign reduce jobs to the mappers that did most amount of map tasks, so maximize locality
func updateReduceTasks(mapAssignment *map[string][]string) {
	log.Println("LOG: Start populating ReduceTasks.")
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
		log.Printf("LOG: %s  :  {%s,  %s}\n", reduceTask, status.Worker, status.Status)
	}
	log.Println("LOG: Finish populating ReduceTasks.")
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
		fmt.Printf("Assign map tasks to node(%s). \n", nodeHost)
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

func handleTaskReport(req *utility.Request_GeneralReq) {
	switch req.GeneralReq.ReqType {
	case "map_task_report":
		handleMapTaskReport(req.GeneralReq.Args)
	case "red_task_report":
		handleRedTaskReport(req.GeneralReq.Args)
	}
}

func handleMapTaskReport(args []string) {
	res := args[0]
	mapTaskId := args[1]
	// make sure mapReduce job is not failed
	if isJobFailed {
		return
	}
	// report all reducer, this map task is finished.
	if res == "completed" {
		log.Printf("LOG: Map task(%s) complete, report: %s. \n", mapTaskId, res)
		taskStatus := getMapTasks(mapTaskId)
		taskStatus.Status = "completed"
		setMapTasks(mapTaskId, taskStatus)

		increFinishedMapNum()
		sendReportToClient()

		mapMutex.Lock()
		for reducerId, reducerInfo := range ReduceTasks {
			conn, err := createConnection(reducerInfo.Worker)
			if err != nil {
				log.Println("ERROR: Fail to create connection with reducer. ", err)
				continue
			}
			msgHandler := utility.NewMessageHandler(conn)
			reqMsg := utility.Request{
				Req: &utility.Request_RedTaskReq{
					RedTaskReq: &utility.RedTaskReq{
						MapTaskId:      mapTaskId,
						RedTaskId:      reducerId,
						MapperHost:     MapTasks[mapTaskId].Worker,
						NumOfChunks:    uint32(len(MapTasks)),
						ControllerHost: controllerHost,
						OutputName:     OutputFileName,
					},
				},
			}
			wrapper := &utility.Wrapper{
				Msg: &utility.Wrapper_RequestMsg{
					RequestMsg: &reqMsg,
				},
			}
			err = msgHandler.Send(wrapper)
			if err != nil {
				log.Printf("ERROR: Fail to send reduce task request to %s. %s ", reducerInfo.Worker, err)
				continue
			}
			log.Printf("LOG: Send reduce task request to node(%s). ", reducerInfo.Worker)
			msgHandler.Close()
		}
		mapMutex.Unlock()
	} else {
		log.Printf("ERROR: Map task(%s) failes, report: %s. \n", mapTaskId, res)
		taskStatus := getMapTasks(mapTaskId)
		taskStatus.Status = "fail"
		setMapTasks(mapTaskId, taskStatus)
		isJobFailed = true
	}
}

func handleRedTaskReport(args []string) {
	res := args[0]
	redTaskId := args[1]
	if res == "completed" {
		log.Printf("LOG: Reduce task(%s) complete, report: %s. \n", redTaskId, res)
		taskStatus := getReduceTasks(redTaskId)
		taskStatus.Status = "completed"
		setReduceTasks(redTaskId, taskStatus)
	} else {
		log.Printf("LOG: Reduce task(%s) failes, report: %s. \n", redTaskId, res)
		taskStatus := getReduceTasks(redTaskId)
		taskStatus.Status = "fail"
		setReduceTasks(redTaskId, taskStatus)
	}
	if checkRedJobDone() {
		log.Printf("LOG: All reduce tasks completed. \n")
		fmt.Printf("MapReduce job completed. \n")
		sendGeneralResponse("complete")
	}
}

// return the average number of job that has already been assigned to
// the node which still have unsigned chunks,
// i.e. get the node lists of nodeChunMap, and calculate their average assigned job numbers
func getAvgJobNumOnNodes(nodeChunkMap map[string][]string, mapAssignment map[string][]string) int {
	count := len(nodeChunkMap)
	sum := 0
	for key := range nodeChunkMap {
		if _, ok := mapAssignment[key]; !ok {
			continue
		}
		sum += len(mapAssignment[key])
	}
	return sum / count
}

// if [finished map numbers] mod reportFreq == 0, then report the process
// when all map finished, report map task completed
func sendReportToClient() {
	// map complete report
	if getFinishedMapNum() == len(MapTasks) {
		sendGeneralResponseWithArgs("report", []string{"Map tasks completed."})
		return
	}
	// intermediate report
	if getFinishedMapNum()%reportFreq == 0 {
		resContext := fmt.Sprintf("Completed Map Tasks: %d / %d", getFinishedMapNum(), len(MapTasks))
		sendGeneralResponseWithArgs("report", []string{resContext})
	}
}
