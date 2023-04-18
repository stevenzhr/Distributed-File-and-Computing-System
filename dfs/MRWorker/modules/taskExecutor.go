package workerModules

import (
	"bufio"
	"dfs/config"
	"dfs/utility"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
	"plugin"
	"strings"
	"sync"
)

type context struct {
	resList [][]string // key, vaule
}

type Task interface {
	Init(args []string)
	Map(index int, text string) ([]byte, []byte)
	Reduce(key []byte, values [][]byte) [][]byte
	GetNumOfReducer() int
}

var (
	mapMutex          sync.RWMutex
	mapRedTask        Task
	partitionFileList []string
)

func addPartitionFile(filename string) {
	mapMutex.Lock()
	partitionFileList = append(partitionFileList, filename)
	mapMutex.Unlock()
}

// save plugin so file use request info
func saveSoFile(soFile *utility.Chunk) (string, error) {
	soFilePath := config.VAULT_PATH + "ws/" + soFile.GetFileName()
	err := ioutil.WriteFile(soFilePath, soFile.GetDataStream(), 0666)
	if err != nil {
		log.Println("ERROR: Can not write so file. ", err)
		return "", err
	}
	_, checksum, err := utility.FileInfo(soFilePath)
	if err != nil {
		log.Println("ERROR: Fail to open so file. ", err)
		return "", err
	}
	if checksum != soFile.GetChecksum() {
		os.Remove(soFilePath)
		log.Println("ERROR: Checksum unmatched, delete local .so file. ")
		log.Printf("LOG: Checksum: local(%s) vs req(%s) \n", checksum, soFile.GetChecksum())
		return "", fmt.Errorf("ERROR: Checksum not match. ")
	}
	log.Println("LOG: Successfully save so file. Start loading plugin methods. ")
	return soFilePath, nil
}

// load local plugin so file
func loadPlugin(soFilePath string, args []string) error {
	plug, err := plugin.Open(soFilePath)
	if err != nil {
		log.Println("ERROR: Can not open so file. ", err)
		return err
	}
	symTask, err := plug.Lookup("Task")
	if err != nil {
		log.Println("ERROR: ", err)
		return err
	}
	task, ok := symTask.(Task)
	if !ok {
		fmt.Println("ERROR: nexpected type from module symbol. ")
		return fmt.Errorf("nexpected type from module symbol. ")
	}
	mapRedTask = task
	mapRedTask.Init(args)
	log.Println("LOG: Successfully load plugin methods. ")
	return nil
}

// handle map tasks base on chunk list in request
func handleMapTasks(req *utility.Request_MapTaskReq, msgHandler *utility.MessageHandler) {
	log.Println("LOG: Receive map tasks, start save so file. ")
	soFilePath, err := saveSoFile(req.MapTaskReq.SoChunk)
	if err != nil {
		log.Println("ERROR: Map task failed. ", err)
		return
	}
	err = loadPlugin(soFilePath, req.MapTaskReq.Parameters)
	if err != nil {
		log.Println("ERROR: Map task failed. ", err)
		return
	}
	// run task one by one
	chunkList := req.MapTaskReq.InputList
	for i := 0; i < len(chunkList); i++ {
		log.Printf("LOG: Run map task on chunk(%s). ", chunkList[i])
		res := handleOneMapTask(chunkList[i])
		// Report complete res to host := os.Args[1]
		args := []string{res, chunkList[i]}
		err := sendGeneralReq("map_task_report", args) //TODO: mananger receive report
		if err != nil {
			return
		}
		log.Printf("LOG: Finish map task on chunk(%s), res: %s. ", chunkList[i], res)
	}
	os.Remove(soFilePath)
}

// send general request to host
func sendGeneralReq(req_type string, args []string) error {
	conn, err := createConnection(os.Args[1])
	if err != nil {
		log.Printf("ERROR: Connection error when send general req(%s) to manager. Error msg: %s \n", req_type, err)
		return err
	}
	msgHandler := utility.NewMessageHandler(conn)
	defer msgHandler.Close()
	reqMsg := utility.Request{
		Req: &utility.Request_GeneralReq{
			GeneralReq: &utility.GeneralReq{
				ReqType: req_type,
				Args:    args,
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
		log.Printf("ERROR: Fail to send general req(%s) to manager. Err msg: %s \n", req_type, err)
		return err
	}
	return nil
}

// handle one map task. ChunkName is input file name, read it line by line.
// Send each line to plugin's map method and do partation on output key-value pair.
func handleOneMapTask(chunkName string) string {
	// create output content
	r := mapRedTask.GetNumOfReducer()
	partitionList := make([]context, r)
	// read chunk
	file, err := os.Open(config.VAULT_PATH + chunkName)
	if err != nil {
		log.Printf("WORNING: Can not open file(%s). %s\n", chunkName, err)
		fmt.Printf("Can not open file(%s). %s\n", chunkName, err)
		return "fail"
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	i := 0
	for scanner.Scan() {
		line := scanner.Text()
		key, value := mapRedTask.Map(i, line)
		doPartition(key, value, partitionList)
		i++
	}
	err = savePartitionFile(chunkName, partitionList)
	if err != nil {
		log.Printf("WORNING: Can not save partition file. %s\n", err)
		fmt.Printf("Can not save partition file. %s\n", err)
		return "fail"
	}
	return "completed"
}

// use hash(key)%r to do partition
func doPartition(key, value []byte, partitionList []context) {
	hash := fnv.New32a()
	hash.Write(key)
	p := int(hash.Sum32()) % len(partitionList)
	keyValuePair := []string{string(key), string(value)}
	partitionList[p].resList = append(partitionList[p].resList, keyValuePair)
}

// Save map result to local disk file as json file
func savePartitionFile(filename string, partitionList []context) error {
	for i := 0; i < len(partitionList); i++ {
		filepath := fmt.Sprintf("%sws/%s_p%d", config.VAULT_PATH, filename, i+1)
		dataStream := strArrToByteArr(partitionList[i].resList)
		// store partition file
		err := ioutil.WriteFile(filepath, dataStream, 0666)
		if err != nil {
			return err
		}
	}
	return nil
}

func strArrToByteArr(s [][]string) []byte {
	var resArr []byte
	for lineIndex := range s {
		line := s[lineIndex]
		lineStr := fmt.Sprintf("%s-->%s\n", line[0], line[1])
		resArr = append(resArr, []byte(lineStr)...)
	}
	return resArr
}

func handleReduceTasks(req *utility.Request_RedTaskReq, msgHandler *utility.MessageHandler) {
	mapTaskId := req.RedTaskReq.MapTaskId
	redTaskId := req.RedTaskReq.RedTaskId
	mapperHost := req.RedTaskReq.MapperHost
	totalChunkNum := int(req.RedTaskReq.NumOfChunks)
	log.Printf("LOG: Receive reduce task:%s, mapTaskId: %s, mapperHost: %s, totalChunkNum: %d \n", redTaskId, mapTaskId, mapperHost, totalChunkNum)
	err := getPartitionFile(mapTaskId, redTaskId, mapperHost)
	if err != nil {
		err := sendGeneralReq("red_task_report", []string{"fail", redTaskId})
		if err != nil {
			return
		}
	}
	// wait until all map task has finished.
	if len(partitionFileList) >= totalChunkNum {
		res := runReduceTask()
		err := sendGeneralReq("red_task_report", []string{res, redTaskId})
		if err != nil {
			return
		}
	}
}

func getPartitionFile(mapTaskId, redTaskId, mapperHost string) error {
	partitionFileName := fmt.Sprintf("%s_%s", mapTaskId, redTaskId)
	localhost, _ := os.Hostname()
	localhost = strings.ReplaceAll(localhost, ".cs.usfca.edu", "")
	log.Printf("LOG: Reducer(%s) start get partitionFile form mapper(%s). \n", localhost, strings.Split(mapperHost, ":")[0])
	if localhost != strings.Split(mapperHost, ":")[0] {
		conn, err := createConnection(mapperHost)
		if err != nil {
			log.Printf("ERROR: Fail to create connection with mapper(%s) to get partition file of (%s). \n", mapperHost, mapTaskId)
			return err
		}
		msgHandler := utility.NewMessageHandler(conn)
		defer msgHandler.Close()
		// retrieve partition file
		reqMsg := utility.Request{
			Req: &utility.Request_ChunkReq{
				ChunkReq: &utility.ChunkReq{
					GetReq: true,
					ChunkData: &utility.Chunk{
						FileName: partitionFileName,
					},
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
			log.Printf("ERROR: Fail to send get partition req(%s) to node(%s). Err msg: %s \n", partitionFileName, mapperHost, err)
			return err
		}
		log.Printf("LOG: Send get partition req(%s) to node(%s). \n", partitionFileName, mapperHost)
		resWrapper, err := msgHandler.Receive()
		if err != nil {
			log.Printf("WARNING: Fail to receive res of get partition req(%s) to node(%s). Err msg: %s \n", partitionFileName, mapperHost, err)
			return err
		}
		// extract response type: true / false
		resType := resWrapper.GetResponseMsg().GetChunkRes().GetStatus()
		if !resType {
			log.Printf("WARNING: Mapper(%s) refused to send file(%s). \n", mapperHost, partitionFileName)
			return fmt.Errorf("Mapper refused. ")
		}
		fileData := resWrapper.GetResponseMsg().GetChunkRes().GetChunkData().GetDataStream()
		filepath := fmt.Sprintf("%sws/%s", config.VAULT_PATH, partitionFileName)
		err = ioutil.WriteFile(filepath, fileData, 0666)
		if err != nil {
			log.Println("WARNING: Can't write partition file. ", err.Error())
			return err
		}
		log.Printf("LOG: Successful receive and save partition req(%s) to node(%s). \n", partitionFileName, mapperHost)
	}
	addPartitionFile(partitionFileName)
	return nil
}

func runReduceTask() string {
	log.Println("All map tasks have been completed. Start reduce tasks. ")
	//

	return "fail"
}
