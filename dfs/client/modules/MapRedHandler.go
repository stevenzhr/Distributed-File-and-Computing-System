package ClientModules

import (
	"bufio"
	"dfs/utility"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
)

func MapReduce(reqStr []string, managerHost string) bool {
	soFilePath := reqStr[1]
	inputName := reqStr[2]
	outputName := reqStr[3]
	var parameters []string
	if len(reqStr) > 4 {
		parameters = reqStr[4:]
	}

	// check if so_file exist
	soFileSize, soFileChksm, err := utility.FileInfo(soFilePath)
	if err != nil {
		log.Printf("WARNING: %s file doesn't exist.\n", soFilePath)
		fmt.Printf("WARNING: %s file doesn't exist.\n", soFilePath)
		return true
	}

	// check if input file in dfs
	file, err := isFileExist(inputName)
	if err != nil {
		log.Printf("WARNING: Error in checking the existance of %s\n", inputName)
		fmt.Printf("WARNING: Error in checking the existance of %s\n", inputName)
		return false
	}
	if file == nil {
		return true
	}

	// make so file into chunk
	soFile, err := os.Open(soFilePath)
	if err != nil {
		log.Fatalln(err.Error())
		return true
	}
	defer soFile.Close()
	reader := bufio.NewReader(soFile)
	soFileData := make([]byte, soFileSize)
	_, err = reader.Read(soFileData)
	if err != nil && err.Error() != "EOF" {
		log.Println("WARNING: File read error when read in .so file. ", err.Error())
		fmt.Println("File read error when read in .so file. ")
		return true
	}

	soChunk := &utility.Chunk{
		FileName:   filepath.Base(soFilePath),
		Checksum:   soFileChksm,
		Size:       soFileSize,
		DataStream: soFileData,
	}
	log.Printf("LOG: Send so file(%s), checksum(%s), size(%d). \n", filepath.Base(soFilePath), soFileChksm, soFileSize)
	// connect to MapReduce manager
	mrMsgHandler, err := createConnection(managerHost)
	if err != nil {
		log.Fatalln(err.Error())
		fmt.Printf("ERROR: Can't establish connection with MapReduce Manager %s.\n", managerHost)
		return false
	}
	defer mrMsgHandler.Close()
	//send MapReduce request to MRmanager
	err = sendMRRequest(mrMsgHandler, soChunk, file, outputName, parameters)
	if err != nil {
		return false
	}
	// receive response
	completed := false // a flag reflecting whether the job is completed
	for !completed {
		completed, err = getMRResponse(mrMsgHandler)
		if err != nil {
			return false
		}
	}

	return true
}

func getMRResponse(mrMsgHandler *utility.MessageHandler) (bool, error) {
	resWrapper, err := mrMsgHandler.Receive()
	if err != nil {
		fmt.Println("Connection error when unpackage from controller. Please exit. ")
		log.Printf("WARNING: Connection error receiving response from controller. (%s)\n", err.Error())
		return true, err
	}
	generalRes := resWrapper.GetResponseMsg().GetGeneralRes()
	switch generalRes.GetResType() {
	case "accept":
		log.Println("LOG: MapReduce request accepted.")
		return false, nil
	case "report":
		log.Printf("LOG: report - %s", generalRes.GetResponseArg()[0])
		fmt.Println(generalRes.GetResponseArg()[0])
		return false, nil
	case "complete":
		log.Println("LOG: MapReduce job complete.")
		fmt.Println("MapReduce job complete.")
		return true, nil
	case "deny":
		log.Println("ERROR: MapReduce request denied by manager. ", generalRes.GetResponseArg()[0])
		fmt.Println("MapReduce request denied by manager. Error msg: ", generalRes.GetResponseArg()[0])
		return true, nil
	default:
		log.Println("WARNING: Unknown error when package from manager.(Can't parse response type) ")
		fmt.Println("Unknown error when package from manager.(Can't parse response type) ")
		return true, nil
	}

}

func sendMRRequest(mrMsgHandler *utility.MessageHandler, soChunk *utility.Chunk,
	inputFile *utility.File, outputName string, parameters []string) error {
	// prepare message sent to MRManager
	log.Println(inputFile.ChunkNodeList) //-------------
	reqMsg := utility.Request{
		Req: &utility.Request_MapredReq{
			MapredReq: &utility.MapRedReq{
				SoChunk:        soChunk,
				InputFile:      inputFile,
				OutputName:     outputName,
				Parameters:     parameters,
				ControllerHost: os.Args[1],
			},
		},
	}
	wrapper := &utility.Wrapper{
		Msg: &utility.Wrapper_RequestMsg{
			RequestMsg: &reqMsg,
		},
	}
	err := mrMsgHandler.Send(wrapper)
	if err != nil {
		fmt.Println("Connection error when send request to controller. Please exit.")
		log.Printf("WARNING: Connection error when send request to controller. (%s)\n", err.Error())
		return err
	}
	log.Printf("LOG: Successfully sent MapReduce task %s on %s.\n", soChunk.FileName, inputFile.Filename)
	return nil
}

// Establish connection with MRManager
func createConnection(host string) (*utility.MessageHandler, error) {
	conn, err := net.Dial("tcp", host)
	if err != nil {
		fmt.Println("ERROR: Can't establish connection with MRManager. Please check server name. ")
		log.Println("ERROR: Can't establish connection with MRManager. Please check server name. ")
		return nil, err
	}
	msgHandler := utility.NewMessageHandler(conn)
	return msgHandler, nil
}
