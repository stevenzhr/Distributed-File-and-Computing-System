package modules

import (
	"dfs/utility"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
)

var (
	flag bool
)

func ListenClientConn(wg *sync.WaitGroup) {
	flag = true
	listener, err := net.Listen("tcp", ":"+os.Args[2])
	if err != nil {
		log.Println(err.Error())
		return
	}
	for flag {
		if conn, err := listener.Accept(); err == nil {
			msgHandler := utility.NewMessageHandler(conn)
			go handleClientRequest(msgHandler)
			if !flag {
				break
			}
		}
	}
}

func handleClientRequest(msgHandler *utility.MessageHandler) {
	defer msgHandler.Close()
	for flag {
		// start receive client request wrapper
		wrapper, err := msgHandler.Receive()
		if err != nil {
			log.Println(err.Error())
			return
		}
		// parse request
		var resWrapper *utility.Wrapper
		if msg, ok := wrapper.Msg.(*utility.Wrapper_RequestMsg); ok {
			if req, ok := msg.RequestMsg.Req.(*utility.Request_MapredReq); ok {
				resWrapper = handleMapredRequest(req.MapredReq)
			}
		} else {
			log.Println("WARNING: receive invalid request. ")
			return
		}
		if resWrapper != nil {
			msgHandler.Send(resWrapper)
			log.Println("LOG: Send response. ")
		}
	}
}

func handleMapredRequest(mapredReq *utility.MapRedReq) *utility.Wrapper {
	// assume accept response for now
	generalRes := utility.GeneralRes{
		ResType: "accept",
	}
	// save so file to temp
	soData := mapredReq.GetSoChunk().GetDataStream()
	fileName := mapredReq.GetInputFile().GetFilename() + "_mr.so"
	filePath := "temp/" + fileName
	err := os.MkdirAll("temp/", os.ModePerm)
	if err != nil {
		log.Println("ERROR: Can't create or open temp folder. ", err)
		fmt.Println("ERROR: Can't create or open temp folder. ", err)
		return nil
	}
	err = ioutil.WriteFile(filePath, soData, 0666)
	if err != nil {
		log.Println("ERROR: Can't create .so file. ", err)
		fmt.Println("ERROR: Can't create .so file. ", err)
		generalRes = utility.GeneralRes{
			ResType: "deny",
		}
		return nil
	}
	// check the checksum of .so file
	_, checksum, _ := utility.FileInfo(filePath)
	if checksum != mapredReq.GetSoChunk().GetChecksum() {
		os.Remove(filePath)
		log.Println("WARNING: Checksum unmatched, delete local .so file. ")
		log.Printf("LOG: Checksum: local(%s) vs req(%s) \n", checksum, mapredReq.GetSoChunk().GetChecksum())
		generalRes = utility.GeneralRes{
			ResType: "deny",
		}
	} else {
		log.Printf("LOG: Successful retrieve .so file(%s). \n", fileName)
	}

	mapAssignment := assignNodeWithChunks(mapredReq)

	// init map tasks
	initTasks(mapredReq)
	updateMapTasks(mapAssignment)
	updateReduceTasks(mapAssignment)

	// TODO: add process map & reduce

	// new a connection with workers
	// send request to workers
	return &utility.Wrapper{
		Msg: &utility.Wrapper_ResponseMsg{
			ResponseMsg: &utility.Response{
				Res: &utility.Response_GeneralRes{
					GeneralRes: &generalRes,
				},
			},
		},
	}
}

// get a map of (chunk : 3 nodes)
// transfer it into (node : chunks store on it)
// always assign the node with the least number of choices(chunks)
// do not assign the node if it already has more works than average
// return a map of each node and their map chunks， key: orion03:26000,  value: [chunk1, chunk3, ...]
func assignNodeWithChunks(mapredReq *utility.MapRedReq) *map[string][]string {
	// test_4.bin_2-15.cnk:	orion02:26521,orion07:26526,orion06:26525
	chunkNodeList := mapredReq.GetInputFile().GetChunkNodeList()
	chunkNum := len(chunkNodeList)
	// key: test_4.bin_2-15.cnk , value: [orion02:26521, orion07:26526, orion06:26525]

	chunkNodeMap := make(map[string][]string)
	for i := 0; i < len(chunkNodeList); i++ {
		list1 := strings.Split(chunkNodeList[i], ":\t")
		chunkName := list1[0][:len(list1[0])]
		nodeList := strings.Split(list1[1], ",")
		chunkNodeMap[chunkName] = nodeList
	}

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

	// translate storage node port to worker node port in key
	tempMap := make(map[string][]string)
	for key, value := range nodeChunkMap {
		hostName := strings.Split(key, ":")[0]
		workerName := Workers[hostName]
		tempMap[workerName] = value
	}
	nodeChunkMap = tempMap

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

		// assign the first chunk to that node
		if _, ok := mapAssignment[minNode]; !ok {
			mapAssignment[minNode] = []string{}
		}
		chunkName := nodeChunkMap[minNode][0]
		mapAssignment[minNode] = append(mapAssignment[minNode], chunkName)
		assignedChunkNum += 1
		// delete all duplicates of the chunk from the nodeChunkMap
		nodeList := chunkNodeMap[chunkName]
		for _, node := range nodeList {
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
	}
	// write detailed assignment to log
	log.Println("LOG: Assign each node with these chunks")
	log.Println("Node Name\tMap Task #\t Chunk List")
	for key, value := range mapAssignment {
		log.Printf("%s\t%d\t%s", key, len(value), value)
	}

	return &mapAssignment
}

// insert all keys of MapTasks and ReduceTaks
func initTasks(mapredReq *utility.MapRedReq) {
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
	// TODO: get the reducer number
	r := 3
	for i := 1; i <= r; i++ {
		reducerName := "p" + strconv.Itoa(i)
		ReduceTasks[reducerName] = TaskStatus{}
	}

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
