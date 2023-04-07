package ClientModules

import (
	"bufio"
	"dfs/config"
	"dfs/utility"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

var (
	msgHandler *utility.MessageHandler
	wg         sync.WaitGroup
)

func SetMsgHandler(handler *utility.MessageHandler) {
	msgHandler = handler
}

// store file process
func StoreFile(reqStr []string) bool {
	// Read local file & get size
	path := reqStr[1]
	chunkSize := config.CHUNK_SIZE
	var err error
	if len(reqStr) == 4 {
		unit := reqStr[3]
		switch unit {
		case "byte":
			chunkSize, err = strconv.Atoi(reqStr[2])
			if err != nil {
				log.Println("WARNING: ", err.Error())
				fmt.Println("WARNING: Fail to parse customized chunk size, using default value 64MB")
				chunkSize = config.CHUNK_SIZE
			}
		case "KB":
			chunkSize, err = strconv.Atoi(reqStr[2])
			chunkSize *= 1024
			if err != nil {
				log.Println("WARNING: ", err.Error())
				fmt.Println("WARNING: Fail to parse customized chunk size, using default value 64MB")
				chunkSize = config.CHUNK_SIZE
			}
		case "MB":
			chunkSize, err = strconv.Atoi(reqStr[2])
			chunkSize *= 1024 * 1024
			if err != nil {
				log.Println("WARNING: ", err.Error())
				fmt.Println("WARNING: Fail to parse customized chunk size, using default value 64MB")
				chunkSize = config.CHUNK_SIZE
			}
		}
	} else if len(reqStr) == 3 || len(reqStr) > 4 {
		fmt.Println("Please input chunksize unit with a space(byte, KB, MB). ")
		return true
	}

	filename := filepath.Base(path)
	fileSize, checksum, err := utility.FileInfo(path)
	if err != nil {
		log.Println("WARNING: ", err.Error())
		fmt.Println("No such file, please check again. ")
		return true
	}
	log.Printf("LOG: Prepare save file: %s(%d MB)[%s]\n", filename, fileSize/1024/1024, checksum)
	fmt.Printf("Prepare save file: %s(%d MB)[%s]\n", filename, fileSize/1024/1024, checksum)
	// Send store file request
	reqMsg := utility.Request{
		Req: &utility.Request_FileReq{
			FileReq: &utility.FileReq{
				ReqType: "put",
				FileInfo: &utility.File{
					Filename:  filename,
					Checksum:  checksum,
					FileSize:  fileSize,
					ChunkSize: uint64(chunkSize),
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
		fmt.Println("Connection error when send request to controller. Please exit.")
		log.Printf("WARNING: Connection error when send request to controller. (%s)\n", err.Error())
		return false
	}
	log.Println("LOG: Successfully sent file put request. ")
	//  chunk-list response
	resWrapper, err := msgHandler.Receive()
	if err != nil {
		fmt.Println("Connection error when unpackage form controller. Please exit. ")
		log.Printf("WARNING: Connection error when unpackage form controller. (%s)\n", err.Error())
		return false
	}
	resType := resWrapper.GetResponseMsg().GetGeneralRes().GetResType()
	switch resType {
	case "accept":
		responseArgs := resWrapper.GetResponseMsg().GetGeneralRes().GetResponseArg()
		log.Println("LOG: accept response. Args: ", responseArgs)
		// Split file and send chunks to node
		sendFile(path, fileSize, chunkSize, responseArgs)
	case "deny":
		resStr := resWrapper.GetResponseMsg().GetGeneralRes().GetResponseArg()[0]
		log.Printf("WARNING: Request denied by controller.[%s] \n", resStr)
		fmt.Printf("Request denied by controller.[%s] \n", resStr)
	default:
		log.Println("WARNING: Unknown error when package from controller.(Can't parse response type) ")
		fmt.Println("Unknown error when package from controller.(Can't parse response type) ")
	}
	return true
}

// splite file to chunks and send them by response node list
func sendFile(path string, fileSize uint64, chunkSize int, responseArgs []string) {
	file, err := os.Open(path)
	if err != nil {
		log.Fatalln(err.Error())
		return
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	numOfChunk := int(math.Ceil(float64(fileSize) / float64(chunkSize)))

	formattedChunkSize := ""
	if chunkSize > 1024*1024 {
		formattedChunkSize = fmt.Sprintf("%d MB", chunkSize/1024/1024)
	} else if chunkSize > 1024 {
		formattedChunkSize = fmt.Sprintf("%d KB", chunkSize/1024)
	} else {
		formattedChunkSize = fmt.Sprintf("%d byte", chunkSize)
	}

	// numStr := strconv.FormatInt(int64(chunkSize), 10)
	// formattedChunkSize := ""

	// for i, digit := range numStr {
	// 	if i > 0 && (len(numStr)-i)%3 == 0 {
	// 		formattedChunkSize += ","
	// 	}
	// 	formattedChunkSize += string(digit)
	// }

	ch := make(chan bool, numOfChunk)
	// split to chunk and send
	fmt.Printf("Start sending %d chunks, each of size %s byte.\n", numOfChunk, formattedChunkSize)
	for i := 0; i < numOfChunk; i++ {
		log.Printf("LOG: Start send chunks: %d/%d to Node: %s \n", i+1, numOfChunk, strings.Split(responseArgs[i], ",")[0])
		// fmt.Printf("Start send chunks: %d/%d to Node: %s \n", i+1, numOfChunk, strings.Split(responseArgs[i], ",")[0])
		chunkData := make([]byte, chunkSize)
		n, err := reader.Read(chunkData)
		if err != nil && err.Error() != "EOF" {
			log.Println("WARNING: File read error when split file. ", err.Error())
			fmt.Println("WARNING: File read error when split file. ")
			return
		}
		if n < chunkSize {
			chunkData = chunkData[:n]
		}
		// process chunk and send
		fileName := fmt.Sprintf("%s_%d-%d", filepath.Base(path), i+1, numOfChunk) // if need change, also have to change in controller
		wg.Add(1)
		pipingList := strings.Split(responseArgs[i], ",")
		go processChunk(fileName, chunkData, pipingList, ch)
		log.Println("LOG: Process chunk end. ")
	}
	wg.Wait()
	//check if all results in channel are true
	for i := 0; i < numOfChunk; i++ {
		value, _ := <-ch
		if !value {
			DeleteFile([]string{"delete", filepath.Base(path)})
			break
		}
	}
	log.Println("LOG: Send file end. ")
	fmt.Println("LOG: Send file successfully.")
}

// send chunk by piping list
func processChunk(fileName string, chunkData []byte, pipingList []string, ch chan<- bool) {
	defer wg.Done()
	// prepare chunk
	fileName += ".cnk"
	filePath := "temp/" + fileName
	err := os.MkdirAll("temp/", os.ModePerm)
	if err != nil {
		log.Println("ERROR: Can't create or open temp folder. ", err)
		fmt.Println("ERROR: Can't create or open temp folder. ", err)
		ch <- false
		return
	}
	err = ioutil.WriteFile(filePath, chunkData, 0666)
	if err != nil {
		log.Println("ERROR: Can't create chunkfile. ", err)
		fmt.Println("ERROR: Can't create chunkfile. ", err)
		ch <- false
		return
	}
	_, checksum, err := utility.FileInfo(filePath)
	err = os.Remove(filePath)
	if err != nil {
		log.Println("ERROR: Can't delete temp chunkfile. ", err)
		fmt.Println("ERROR: Can't delete temp chunkfile. ", err)
		ch <- false
		return
	}
	// connect to node
	host := pipingList[0]
	log.Println("trying to connect to: ", host)
	conn, err := net.Dial("tcp", host)
	if err != nil {
		log.Printf("WARNING: Can't establish connection with node(%s). Try next candiate. \n", host)
		// fmt.Printf("WARNING: Can't establish connection with node(%s). Try next candiate. \n", host)
		if len(pipingList) > 1 {
			pipingList = pipingList[1:]
			wg.Add(1)
			processChunk(fileName, chunkData, pipingList, ch)
			return
		} else {
			log.Println("ERROR: All candidate nodes failed. Upload file failed. ")
			fmt.Println("ERROR: All candidate nodes failed. Upload file failed. ")
			ch <- false
			return
		}
	}
	defer conn.Close()
	nodeMsgHandler := utility.NewMessageHandler(conn)
	// package chunk
	chunk := utility.Chunk{
		FileName:   fileName,
		Checksum:   checksum,
		Size:       uint64(len(chunkData)),
		PipingList: pipingList,
		DataStream: chunkData,
	}
	wrapper := &utility.Wrapper{
		Msg: &utility.Wrapper_RequestMsg{
			RequestMsg: &utility.Request{
				Req: &utility.Request_ChunkReq{
					ChunkReq: &utility.ChunkReq{
						GetReq:    false,
						ChunkData: &chunk,
					},
				},
			},
		},
	}
	err = nodeMsgHandler.Send(wrapper)
	log.Printf("LOG: Sent put chunk request to node(%s). \n", host)
	if err != nil {
		//try the next node on the list
		log.Printf("WARNING: Chunk transmission failed with node(%s). Try next candiate. \n", host)
		fmt.Printf("WARNING: Chunk transmission failed with node(%s). Try next candiate. \n", host)
		if len(pipingList) > 1 {
			pipingList = pipingList[1:]
			wg.Add(1)
			processChunk(fileName, chunkData, pipingList, ch)
			return
		} else {
			log.Println("ERROR: All candidate nodes failed. Upload file failed. ")
			fmt.Println("ERROR: All candidate nodes failed. Upload file failed. ")
			ch <- false
			return
		}
	}
	//  general response from node
	resWrapper, err := nodeMsgHandler.Receive()
	log.Printf("LOG: Receive response. \n")
	if err != nil {
		log.Println("WARNING: Unknown error when response from node. ", err.Error())
		fmt.Println("WARNING: Unknown error when response from node. ", err.Error())
		return
	}
	resType := resWrapper.GetResponseMsg().GetGeneralRes().GetResType()
	switch resType {
	case "accept":
		ch <- true
	default:
		ch <- false
	}
}

func DeleteFile(reqStrs []string) bool {
	filename := filepath.Base(reqStrs[1])
	// Send delete file request
	reqMsg := utility.Request{
		Req: &utility.Request_FileReq{
			FileReq: &utility.FileReq{
				ReqType: "delete",
				FileInfo: &utility.File{
					Filename: filename,
				},
			},
		},
	}
	wrapper := &utility.Wrapper{
		Msg: &utility.Wrapper_RequestMsg{
			RequestMsg: &reqMsg,
		},
	}
	err := msgHandler.Send(wrapper)
	if err != nil {
		fmt.Println("Connection error when send request to controller. Please exit. ")
		log.Printf("WARNING: Connection error when send request to controller.(%s)", err.Error())
		return false
	}
	log.Println("LOG: Successfully sent file delete request. ")
	//  chunk-list response
	resWrapper, err := msgHandler.Receive()
	if err != nil {
		fmt.Println("Connection error when unpackage from controller. Please exit. ")
		log.Printf("WARNING: Connection error when unpackage from controller.(%s)", err.Error())
		return false
	}
	resType := resWrapper.GetResponseMsg().GetGeneralRes().GetResType()
	switch resType {
	case "accept":
		log.Println("LOG: Delete success. ")
		fmt.Println("Delete success. ")
	case "deny":
		resStr := resWrapper.GetResponseMsg().GetGeneralRes().GetResponseArg()[0]
		log.Printf("WARNING: Request denied by controller.[%s] \n", resStr)
		fmt.Printf("Request denied by controller.[%s] \n", resStr)
	default:
		log.Printf("WARNING: Unknown error when package from controller. Can't parse response type(%s). \n", resType)
		fmt.Printf("WARNING: Unknown error when package from controller. Can't parse response type(%s). \n", resType)
	}
	return true
}

func GetFile(reqStrs []string) bool {
	// fmt.Println("get file: ", reqStrs[1])
	filename := filepath.Base(reqStrs[1])
	//Send get file request
	reqMsg := utility.Request{
		Req: &utility.Request_FileReq{
			FileReq: &utility.FileReq{
				ReqType: "get",
				FileInfo: &utility.File{
					Filename: filename,
				},
			},
		},
	}
	wrapper := &utility.Wrapper{
		Msg: &utility.Wrapper_RequestMsg{
			RequestMsg: &reqMsg,
		},
	}
	err := msgHandler.Send(wrapper)
	if err != nil {
		fmt.Println("Connection error when send get request to controller. Please exit. ")
		log.Printf("WARNING: Connection error when send get request to controller.(%s)", err.Error())
		return false
	}
	log.Println("LOG: Send get file request.")

	//get response
	resWrapper, err := msgHandler.Receive()
	if err != nil {
		fmt.Println("Connection error when unpackage from controller. Please exit. ")
		log.Printf("WARNING: Connection error when unpackage from controller.(%s)", err.Error())
		return false
	}
	fileResStatus := resWrapper.GetResponseMsg().GetFileRes().GetStatus()
	switch fileResStatus {
	case true:
		fmt.Printf("Start retriving file %s. \n", filename)
		fileData := resWrapper.GetResponseMsg().GetFileRes().GetFileData()
		getChunks(filename, fileData.Checksum, int64(fileData.FileSize), int64(fileData.ChunkSize), fileData.ChunkNodeList)
		log.Println("LOG: Get file request done. ")
	case false:
		log.Printf("WARNING: No such File. Request denied by controller. \n")
		fmt.Printf("No such File. Request denied by controller. \n")
	default:
		log.Println("WARNING: Unknown error when package from controller.(Can't parse response type) ")
		fmt.Println("Unknown error when package from controller.(Can't parse response type) ")
	}
	return true
}

// retrieve all chunks for a file, and put them together locally
func getChunks(fileName string, fileChecksum string, fileSize int64, chunkSize int64, chunkList []string) {

	numOfChunk := int(math.Ceil(float64(fileSize) / float64(chunkSize)))
	// gather the execution results of each thread
	ch := make(chan bool, numOfChunk)
	var chunkNameList []string
	// extract chunk name, checksum, piping list from chunkNodeList, and assign a thread to get the chunk
	for _, chunkInfo := range chunkList {
		info := strings.Split(chunkInfo, ",")
		chunkName := info[0]
		chunkChecksum := info[1]
		pipingList := info[2:]

		chunkNameList = append(chunkNameList, chunkName)

		wg.Add(1)
		go getOneChunk(chunkName, chunkChecksum, chunkSize, pipingList, ch)
	}
	wg.Wait()
	fmt.Println("All threads finished getting chunks.")
	defer removeChunkFromTemp(chunkNameList)

	success := true
	// process fails if any of the thread fails
	for i := 0; i < numOfChunk; i++ {
		value, _ := <-ch
		if !value {
			success = false
			log.Printf("WARNING: Can't get file %s. Please try later. \n", fileName)
			fmt.Printf("WARNING: Can't get file %s. Please try later. \n", fileName)
			break
		}
	}

	// delete all temp chunks if fails
	if !success {
		return
	}

	// create file

	file, err := os.Create(fileName)
	if err != nil {
		log.Printf("ERROR: Can't create file %s.\n", fileName)
		fmt.Printf("Can't create file %s.\n", fileName)
		return
	}
	file.Close()

	file, err = os.OpenFile(fileName, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		log.Printf("ERROR: Can't open file %s.\n", fileName)
		fmt.Printf("Can't open file %s.\n", fileName)
		return
	}
	defer file.Close()

	// build file
	for i := 0; i < len(chunkNameList); i++ {
		chunkNameStr := fmt.Sprintf("%s_%d-%d.cnk", fileName, i+1, len(chunkNameList))
		data, err := ioutil.ReadFile("temp/" + chunkNameStr)
		if err != nil {
			log.Printf("ERROR: Can't read chunk %s.\n", chunkNameStr)
			fmt.Printf("Can't read chunk %s.\n", chunkNameStr)
			return
		}
		file.Write(data)
		if err != nil {
			log.Printf("ERROR: Can't write chunk %s into file %s.\n", chunkNameStr, fileName)
			fmt.Printf("Can't write chunk %s into file %s.\n", chunkNameStr, fileName)
			return
		}
	}
	// checksum
	_, cksm, err := utility.FileInfo(fileName)
	if err != nil {
		log.Println("ERROR: Can't open local file. ", fileName)
		fmt.Println("Can't open local file. ")
		return
	}
	if cksm != fileChecksum {
		log.Println("ERROR: Checksum doesn't match. File retrieve failed. ")
		log.Printf("local(%s) vs. expected(%s) \n", cksm, fileChecksum)
		fmt.Println("Checksum doesn't match. File retrieve failed. ")
		os.Remove(fileName)
		return
	} else {
		log.Printf("Log: %s saved successfully\n", fileName)
		fmt.Printf("File %s saved successfully\n", fileName)
	}

}

// remove all the chunk file from temp folder
func removeChunkFromTemp(chunkNameList []string) {
	for _, chunkName := range chunkNameList {
		//skip if file exists
		_, _, err := utility.FileInfo("temp/" + chunkName)
		if err != nil {
			continue
		}

		err = os.Remove("temp/" + chunkName)
		if err != nil {
			log.Println("ERROR: Can't delete temp chunkfile. ", err)
			fmt.Println("ERROR: Can't delete temp chunkfile. ", err)
		}
	}
}

// each thread get one chunk from node
func getOneChunk(chunkName string, checksum string, size int64, pipingList []string, ch chan<- bool) {
	defer wg.Done()
	success := false

	// create temp folder to save the chunk
	chunkPath := "temp/" + chunkName
	err := os.MkdirAll("temp/", os.ModePerm)
	if err != nil {
		log.Println("ERROR: Can't create or open temp folder. ", err)
		fmt.Println("ERROR: Can't create or open temp folder. ", err)
		ch <- false
		return
	}

	// loop the piping list, until successfully get the chunk
	for _, host := range pipingList {

		// connect to ndoe
		log.Println("trying to connect to: ", host)
		conn, err := net.Dial("tcp", host)
		if err != nil {
			log.Printf("WARNING: Can't establish connection with node(%s). Try next candidte. \n", host)
			// fmt.Printf("WARNING: Can't establish connection with node(%s). Try next candidte. \n", host)
			continue
		}
		nodeMsgHandler := utility.NewMessageHandler(conn)
		defer nodeMsgHandler.Close()

		// wrap up request message
		chunk := utility.Chunk{
			//request contains chunk name
			FileName: chunkName,
			Checksum: checksum,
		}
		wrapper := &utility.Wrapper{
			Msg: &utility.Wrapper_RequestMsg{
				RequestMsg: &utility.Request{
					Req: &utility.Request_ChunkReq{
						ChunkReq: &utility.ChunkReq{
							GetReq:    true,
							ChunkData: &chunk,
						},
					},
				},
			},
		}

		// send request
		err = nodeMsgHandler.Send(wrapper)
		log.Printf("LOG: Sent get chunk request to node(%s). \n", host)
		if err != nil {
			log.Printf("WARNING: Chunk transmission failed with node(%s). Try next candidate. \n", host)
			// fmt.Printf("WARNING: Chunk transmission failed with node(%s). Try next candidate. \n", host)
			continue
		}
		// receive chunk from node
		resWrapper, err := nodeMsgHandler.Receive()
		log.Printf("LOG: Receive response. \n")
		if err != nil {
			log.Printf("WARNING: Can't receive response from node %s on chunk %s", host, chunkName)
			// fmt.Printf("WARNING: Can't receive response from node %s on chunk %s", host, chunkName)
			continue
		}

		// extract response type: true / false
		resType := resWrapper.GetResponseMsg().GetChunkRes().GetStatus()
		if !resType {
			log.Println("WARNING: Node refused to send chunk. Try next candidate.")
			// fmt.Println("WARNING: Unknown error when receiving response from node.")
			continue
		}

		// get chunk info
		chunkInfo := resWrapper.GetResponseMsg().GetChunkRes().GetChunkData()
		chunkData := chunkInfo.DataStream

		// store chunk
		err = ioutil.WriteFile(chunkPath, chunkData, 0666)
		if err != nil {
			log.Println("WARNING: Can't write chunk file. ", err.Error())
			// fmt.Println("WARNING: Can't write chunk file. ", err.Error())
			continue
		}

		// check checksum, if right, change success
		_, newCheckSum, err := utility.FileInfo(chunkPath)
		if newCheckSum != checksum {
			log.Println("WARNING: Checksum does not match. Try next candidate.")
			// fmt.Println("WARNING: Checksum does not match. Try next candidate.")
			continue
		}

		// successfully save the chunk, no need to try the rest of list
		success = true
		if success {
			break
		}
	}

	//put result to channel
	ch <- success
}
