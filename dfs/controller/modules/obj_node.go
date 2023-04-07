package ControllerModules

import (
	"crypto/sha256"
	"dfs/config"
	"dfs/utility"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

type Node struct {
	NodeStatus        string   `json:"NodeStatus"`
	NodeHost          string   `json:"NodeHost"`
	AvailableSpace    uint64   `json:"AvailableSpace"`
	RequestCount      uint64   `json:"RequestCount"`
	ChunkList         []string `json:"ChunkList"`
	ResQueue          []string `json:"ResQueue"` //FIXME:["deleteCnk,chunk1.cnk,chunk2.cnk,chunk3.cnk", "copyCnk,chunk_a.cnk"]
	appendantChunkMap map[string]uint64
	lastHeartBeatTime string
	// ChunkRequestList  map[string]string  // FIXME:
}

var (
	nodeMutex sync.RWMutex
	NodeMap   map[string]Node
	flag      bool
	maxNode   int
	runConfig config.Config
)

func init() {
	flag = true
	maxNode = 0
	NodeMap = make(map[string]Node)
}

func NodeAliveCheck(wg *sync.WaitGroup) {
	defer wg.Done()
	for flag {
		fmt.Println("Checking dead node...")
		log.Println("LOG: Checking dead node.")
		nodeMutex.Lock()
		for nodeName := range NodeMap {
			timestemp, _ := time.Parse(config.TIME_FORMAT, NodeMap[nodeName].lastHeartBeatTime)
			currentTime, _ := time.Parse(config.TIME_FORMAT, time.Now().Format(config.TIME_FORMAT))
			duration := currentTime.Sub(timestemp)
			if duration.Seconds() > config.REPORT_GAP_THRESHOLD {
				go handleNodeDead(nodeName)
			}
		}
		if length := len(NodeMap); length > maxNode {
			maxNode = length
		}
		SaveNodesMap()
		nodeMutex.Unlock()
		fileMutex.RLock()
		SaveFileMap()
		fileMutex.RUnlock()
		// wait for a while to re-beat
		time.Sleep(2 * config.HEART_BEAT_RATE * time.Second)
	}
}

func handleNodeDead(nodeName string) {
	fmt.Printf("WARNING: Find a dead node(%s), implement recovery process...\n", nodeName)
	log.Printf("WARNING: Find a dead node(%s), implement recovery process.\n", nodeName)
	nodeMutex.Lock()
	node := NodeMap[nodeName]
	nodeHost := node.NodeHost
	// get chunk list str="chunkName,checkSum"
	chunkList := node.ChunkList
	appendantChunkList := make([]string, len(node.appendantChunkMap))
	i := 0
	// get appendant chunk list srt="chunkName"
	for key := range node.appendantChunkMap {
		appendantChunkList[i] = key
		i++
	}
	delete(NodeMap, nodeName)
	aliveNodeNum := len(NodeMap)
	nodeMutex.Unlock()
	fmt.Printf("LOG: Node(%s) has been removed. Current nodes status: %d/%d, %d nodes disconnect. \n", nodeName, aliveNodeNum, maxNode, maxNode-aliveNodeNum)
	log.Printf("LOG: Node(%s) has been removed. Current nodes status: %d/%d, %d nodes disconnect. \n", nodeName, aliveNodeNum, maxNode, maxNode-aliveNodeNum)
	if aliveNodeNum < 6 {
		alarmProcess(aliveNodeNum)
	}
	if aliveNodeNum < 3 {
		//Shutdown process
		log.Println("ALARM: No enough nodes. SYSTEM FAILED, Matthew FINALLY KILL ME!!! ")
		fmt.Println("ALARM: No enough nodes. SYSTEM FAILED, Matthew FINALLY KILL ME!!! ")
		fmt.Println("ALARM: No enough nodes. SYSTEM FAILED, Matthew FINALLY KILL ME!!! ")
		fmt.Println("ALARM: No enough nodes. SYSTEM FAILED, Matthew FINALLY KILL ME!!! ")
		flag = false
		deleteFileMap()
		deleteNodesMap()
		return
	}
	// process chunkList
	for i = 0; i < len(chunkList); i++ {
		// parse filename by chunkname
		chunkNameChkSm := chunkList[i] // str="chunkName,checkSum"
		index := strings.LastIndex(chunkNameChkSm, "_")
		fileName := chunkNameChkSm[0:index]
		// remove dead node from chunkMap
		fileMutex.Lock()
		file := FileMap[fileName]
		nodeList := strings.Split(file.ChunkMap[chunkNameChkSm], ",")
		nodeList = removeStrFromArray(nodeHost, nodeList)
		file.ChunkMap[chunkNameChkSm] = stringArrayToString(nodeList) //TODO: Check if need to detect ChunkMap is empty
		FileMap[fileName] = file
		fileMutex.Unlock()
		// log.Printf("LOG: Remove node(%s) form chunk(%s)'s node list. ", nodeHost, strings.Split(chunkName, ",")[0])
		// process reassign chunks
		reassignAChunk(chunkNameChkSm, nodeList)
	}
	log.Printf("LOG: All chunks(total: %d) form dead node's chunkList has been reassigned. \n", i)

	// process appendantChunkList
	if len(appendantChunkList) != 0 {
		waitChkSumStr := "waitChkSum," + stringArrayToString(appendantChunkList)
		// assign all appendant chunks to most empty node, wait for controller reassign
		candidateNode := getNodeListBySpace()[0]
		fileMutex.Lock()
		node = NodeMap[candidateNode]
		node.ResQueue = append(node.ResQueue, waitChkSumStr)
		NodeMap[candidateNode] = node
		fileMutex.Unlock()
		log.Printf("LOG: Add waitChkSum(%s) task to node(%s)'s ResQueue. \n", waitChkSumStr, candidateNode)
	}
}

// Reassign a chunk by randomly choose a node who doesn't have same chunk. The nodeList contains the nodes has the replica.
func reassignAChunk(chunkNameChkSm string, nodeList []string) {
	rand.Seed(time.Now().UnixNano())
	i := 0
	nodeMutex.RLock()
	candidateNodeNameList := make([]string, len(NodeMap))
	// get all available node list
	for nodeName, _ := range NodeMap {
		candidateNodeNameList[i] = nodeName
		i++
	}
	nodeMutex.RUnlock()
	flag := false
	for len(candidateNodeNameList) > 0 {
		ranIndex := rand.Intn(len(candidateNodeNameList))
		// randomly find the first node doesn't have current chunk in chunkList or appendantChunkList
		nodeMutex.RLock()
		cl := NodeMap[candidateNodeNameList[ranIndex]].ChunkList
		acl := getAclMapKeyArray(NodeMap[candidateNodeNameList[ranIndex]].appendantChunkMap)
		nodeMutex.RUnlock()
		if contains(cl, chunkNameChkSm) || contains(acl, strings.Split(chunkNameChkSm, ",")[0]) {
			candidateNodeNameList = removeStrFromArray(candidateNodeNameList[ranIndex], candidateNodeNameList)
			continue
		} else { // found it
			// create copyCnk str, ask candidateNodeList[j] to retrieve chunk from nodeList
			copyCnkStr := "copyCnk," + chunkNameChkSm + "," + stringArrayToString(nodeList)
			fileMutex.Lock()
			node := NodeMap[candidateNodeNameList[ranIndex]]
			node.ResQueue = append(node.ResQueue, copyCnkStr)
			NodeMap[candidateNodeNameList[ranIndex]] = node
			fileMutex.Unlock()
			flag = true
			log.Printf("LOG: <reassignChunkTask>Add copyCnk(%s) task to node(%s)'s ResQueue. \n", copyCnkStr, candidateNodeNameList[ranIndex])
			break
		}

	}
	if !flag {
		log.Printf("WARNING: <reassignChunkTask>Fail to reassign chunk(%s) to all node. Chunk may lost. \n", strings.Split(chunkNameChkSm, ",")[0])
	}
}

// get node list sorted by empty space
func getNodeListBySpace() []string {
	nodeMutex.RLock()
	// create container
	nodeList := make([]string, len(NodeMap))
	i := 0
	// get key collection for NodeMap
	for key := range NodeMap {
		nodeList[i] = key
		i++
	}
	sort.Slice(nodeList, func(m, n int) bool {
		return getNodeReservedSapce(NodeMap[nodeList[m]]) < getNodeReservedSapce(NodeMap[nodeList[n]])
	})
	nodeMutex.RUnlock()
	return nodeList
}

func ListenNodesConn() {
	flag := true
	listener, err := net.Listen("tcp", ":"+os.Args[1])
	if err != nil {
		log.Fatalln(err.Error())
		return
	}
	for flag {
		if conn, err := listener.Accept(); err == nil {
			msgHandler := utility.NewMessageHandler(conn)
			go handleNodeRequest(msgHandler)
		}
	}
}

func handleNodeRequest(msgHandler *utility.MessageHandler) {
	defer msgHandler.Close()
	wrapper, err := msgHandler.Receive()
	if err != nil {
		log.Fatalln(err.Error())
		return
	}

	if msg, ok := wrapper.Msg.(*utility.Wrapper_RequestMsg); ok {
		if req, ok := msg.RequestMsg.Req.(*utility.Request_JoinReq); ok {
			handleJoin(req.JoinReq.NodeHostPort, msgHandler)
		}
	} else if msg, ok := wrapper.Msg.(*utility.Wrapper_HeartbeatMsg); ok {
		handleHeartBeat(msg.HeartbeatMsg, msgHandler)
	} else {
		fmt.Println("Invalid request. ")
	}
}

func handleHeartBeat(heartBeatMsg *utility.Heartbeat, msgHandler *utility.MessageHandler) {
	nodeName := heartBeatMsg.NodeId
	// wrapper := &utility.Wrapper{}
	var generalRes utility.GeneralRes
	// status := "denial"
	// var resQueue []string
	nodeMutex.Lock()
	node, exist := NodeMap[nodeName]
	if exist {
		// update node info
		node.NodeStatus = heartBeatMsg.NodeStatus
		node.AvailableSpace = heartBeatMsg.AvailableSpace
		node.lastHeartBeatTime = heartBeatMsg.Timestemp
		node.RequestCount = heartBeatMsg.RequestCount
		for _, chunkStr := range heartBeatMsg.LastAddedFileList { // chunkStr: [chunkName,checksum]
			// add new chunk to node's ChunkList
			node.ChunkList = append(node.ChunkList, chunkStr)
			// update file's chunkMap
			addToChunkMap(chunkStr, node.NodeHost)
			chunkName := strings.Split(chunkStr, ",")[0]
			// remove appendantChunkMap
			delete(node.appendantChunkMap, chunkName)
		}
		// Check fail_task_list if needed
		failTaskList := heartBeatMsg.FailTaskList
		if len(failTaskList) > 0 {
			log.Printf("WARNING: Node(%s) has %d unfinished task. \n", nodeName, len(failTaskList))
			go handleFailTaskList(nodeName, node.NodeHost, failTaskList)
		}
		// copy and clear resQueue in node
		resQueue := make([]string, len(node.ResQueue))
		copy(resQueue, node.ResQueue)
		node.ResQueue = make([]string, 0)
		// put node back to map
		NodeMap[nodeName] = node
		// fmt.Printf("Receive heartBeat report from node(%s).\n", nodeName)
		log.Printf("LOG: Receive heartBeat report from node %s(%s).\n", node.NodeHost, nodeName)
		// Decision for resQueue
		if len(resQueue) == 0 {
			generalRes = utility.GeneralRes{
				ResType: "accept",
			}
		} else {
			generalRes = utility.GeneralRes{
				ResType:     "queue",
				ResponseArg: resQueue,
			}
		}
	} else {
		generalRes = utility.GeneralRes{
			ResType: "deny",
		}
	}
	nodeMutex.Unlock()
	wrapper := &utility.Wrapper{
		Msg: &utility.Wrapper_ResponseMsg{
			ResponseMsg: &utility.Response{
				Res: &utility.Response_GeneralRes{
					GeneralRes: &generalRes,
				},
			},
		},
	}
	msgHandler.Send(wrapper)
}

func handleJoin(hostPort string, msgHandler *utility.MessageHandler) {
	fmt.Println("receive a join request: ", hostPort)
	nodeName := gethashStr(hostPort) // hash("orion02.cs.usfca.edu:28020")
	wrapper := &utility.Wrapper{}
	nodeMutex.RLock()
	_, exist := NodeMap[nodeName]
	nodeMutex.RUnlock()
	if exist {
		log.Printf("WARNING: Receive a exist node(%s) join request. Request denied. \n", hostPort)
		// exist node in net, send join denied response.
		res := utility.Response{
			Res: &utility.Response_GeneralRes{
				GeneralRes: &utility.GeneralRes{
					ResType: "denial",
				},
			},
		}
		wrapper.Msg = &utility.Wrapper_ResponseMsg{
			ResponseMsg: &res,
		}
		msgHandler.Send(wrapper)
	} else {
		nodeMutex.Lock()
		NodeMap[nodeName] = Node{
			NodeHost:          hostPort,
			NodeStatus:        "alive",
			AvailableSpace:    0,
			ChunkList:         make([]string, 0),
			ResQueue:          make([]string, 0),
			lastHeartBeatTime: time.Now().Format(config.TIME_FORMAT),
			appendantChunkMap: make(map[string]uint64),
		}
		nodeMutex.Unlock()
		res := utility.Response{
			Res: &utility.Response_GeneralRes{
				GeneralRes: &utility.GeneralRes{
					ResType:     "accept",
					ResponseArg: []string{nodeName},
				},
			},
		}
		wrapper.Msg = &utility.Wrapper_ResponseMsg{
			ResponseMsg: &res,
		}
		msgHandler.Send(wrapper)
		fmt.Printf("New node(%s) joined. \n", nodeName)
		log.Printf("LOG: New node(%s) joined. \n", nodeName)
	}
}

func gethashStr(str string) string {
	h := sha256.Sum256([]byte(str))
	return hex.EncodeToString(h[:])[:8]
}

// Save current nodesMap to local json
func SaveNodesMap() {
	jsonBytes, err := json.Marshal(NodeMap)
	if err != nil {
		log.Printf("WARNING: Fail to convert nodeMaps data. System may has not backup. ")
	}
	err = ioutil.WriteFile("NodesData.json", jsonBytes, 0644)
	if err != nil {
		log.Printf("WARNING: Can not save nodeMaps data. System may has not backup. ")
	}
}

// Load nodesMap form local json
func LoadNodesMap() {
	file, err := os.Open("NodesData.json")
	if err != nil {
		log.Printf("WARNING: Fail to open local nodes data. ")
		return
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	nodeMutex.Lock()
	err = decoder.Decode(&NodeMap)
	for nodeName := range NodeMap {
		node := NodeMap[nodeName]
		// update load nodes to latest heartbeat time
		node.lastHeartBeatTime = time.Now().Format(config.TIME_FORMAT)
		node.appendantChunkMap = make(map[string]uint64)
		// node.ResQueue = make([]string, 0)
		// node.ChunkRequestList = make(map[string]string)
		NodeMap[nodeName] = node
	}
	nodeMutex.Unlock()
}

func deleteNodesMap() {
	err := os.Remove("NodesData.json")
	if err != nil {
		log.Println("WARNING: fail to delete local running config file. ")
		return
	}
	return
}

func handleFailTaskList(nodeName string, nodeHost string, failTaskList []string) {
	for i := 0; i < len(failTaskList); i++ {
		taskStr := strings.Split(failTaskList[i], ",")
		switch taskStr[0] {
		case "cnkCrupt":
			// example str: "cnkCrupt,test.txt_3-3.cnk,test.txt_1-3.cnk"
			handleCnkCrupt(taskStr, nodeName, nodeHost)
		case "waitChkSum":
			// TODO:check every chunk in list if all of them has checksum, reAssign them. If not, send it back!
		default:
			// TODO: complete this
			log.Printf("LOG: Receive node(%s)'s failed task(%s). \n", nodeName, taskStr[0])
		}
	}
}

func handleCnkCrupt(taskStr []string, nodeName string, nodeHost string) {
	for i := 1; i < len(taskStr); i++ {
		chunkName := taskStr[i]
		log.Printf("LOG: Process node(%s)'s corrupted chunk(%s) report, total %d/%d. \n", nodeName, chunkName, i, len(taskStr)-1)
		// parse filename by chunkname
		index := strings.LastIndex(chunkName, "_")
		fileName := chunkName[0:index]
		// get chunkName's node
		var nodeList []string
		fileMutex.Lock()
		for key, value := range FileMap[fileName].ChunkMap {
			if chunkName == strings.Split(key, ",")[0] {
				// replace chunkName to chunkName+checkSum
				chunkName = key
				nodeList = strings.Split(value, ",")
				break
			}
		}
		// remove current node from nodeList
		if nodeList != nil {
			nodeList = removeStrFromArray(nodeHost, nodeList)
		}
		// update ChunkMap
		FileMap[fileName].ChunkMap[chunkName] = stringArrayToString(nodeList)
		fileMutex.Unlock()
		// create copyCnk str (example: “copyCnk,[chunkName,checkSum],[node1HostPort],[node2HostPort]”)
		copyCnkStr := "copyCnk," + chunkName + "," + stringArrayToString(nodeList)
		// update NodeMap ResQueue
		nodeMutex.Lock()
		node := NodeMap[nodeName]
		node.ChunkList = removeStrFromArray(chunkName, node.ChunkList)
		node.ResQueue = append(node.ResQueue, copyCnkStr)
		NodeMap[nodeName] = node
		nodeMutex.Unlock()
		log.Printf("LOG: Append new copyCnk(%s) to node(%s)'s ResQueue. \n", copyCnkStr, nodeName)
	}
}

// helper func
func removeStrFromArray(str string, strArr []string) []string {
	for i := 0; i < len(strArr); i++ {
		if strArr[i] == str {
			temp := strArr[0]
			strArr[0] = strArr[i]
			strArr[i] = temp
			if len(strArr) > 1 {
				return strArr[1:]
			} else {
				return []string{}
			}
		}
	}
	return strArr
}

// helper func
func stringArrayToString(nodeList []string) string {
	length := len(nodeList)
	if length == 0 {
		return ""
	} else {
		str := nodeList[0]
		for i := 1; i < length; i++ {
			str += "," + nodeList[i]
		}
		return str
	}
}

// helper func
func contains(arr []string, target string) bool {
	for _, s := range arr {
		if s == target {
			return true
		}
	}
	return false
}

// helper func
func getAclMapKeyArray(aclMap map[string]uint64) []string {
	resStrArr := make([]string, len(aclMap))
	i := 0
	for key := range aclMap {
		resStrArr[i] = key
		i++
	}
	return resStrArr
}

// When alive node num less than 6, start alram
func alarmProcess(aliveNodeNum int) {
	switch aliveNodeNum {
	case 5:
		log.Println("ALARM: System crash risk increases. ")
	case 4:
		log.Println("ALARM: System damaged, may crash down. ")
		log.Println("ALARM: System damaged, may crash down. ")
	case 3:
		log.Println("ALARM: SYSTEM CRITICAL DAMAGED, Call Matthew STOP!!! ")
		log.Println("ALARM: SYSTEM CRITICAL DAMAGED, Call Matthew STOP!!! ")
		log.Println("ALARM: SYSTEM CRITICAL DAMAGED, Call Matthew STOP!!! ")
	}
}
