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
	mapRedTask Task
)

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
	return nil
}

// handle map tasks base on chunk list in request
func handleMapTasks(req *utility.Request_MapTaskReq, msgHandler *utility.MessageHandler) {
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
		res := handleOneMapTask(chunkList[i])
		// Report complete res to host := os.Args[1]
		args := []string{res, chunkList[i]}
		err := sendGeneralReq("map_task_report", args) //TODO: mananger receive report
		if err != nil {
			return
		}
	}
	os.Remove(soFilePath)
}

// send general request to host
func sendGeneralReq(req_type string, args []string) error {
	conn, err := createConnection()
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
	}
	return "done"
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
		// filepath := fmt.Sprintf("vault/ws/%s_p%d", filename, i+1) //FIXME: for testing only, re-comment when deploy
		dataStream := strArrToByteArr(partitionList[i].resList)
		// if err != nil {
		// 	return err
		// }
		// store partition file
		err := ioutil.WriteFile(filepath, dataStream, 0666)
		if err != nil {
			return err
		}
	}
	return nil
}

func strArrToByteArr(s [][]string) []byte {
	// jsonString, err := json.Marshal(s)
	// if err != nil {
	// 	log.Printf("WORNING: Fail to conver key-value pair to json. %s\n", err)
	// 	return []byte{}, err
	// }
	// return []byte(jsonString), nil
	var resArr []byte
	for lineIndex := range s {
		line := s[lineIndex]
		lineStr := fmt.Sprintf("%s-->%s\n", line[0], line[1])
		resArr = append(resArr, []byte(lineStr)...)
	}
	return resArr
}

func handleReduceTask(req *utility.Request_RedTaskReq, msgHandler *utility.MessageHandler) {
	panic("unimplemented")
}
