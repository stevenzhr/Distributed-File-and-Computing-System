package utility

import (
	"crypto/md5"
	"fmt"
	"io"
	"log"
	"os"
	"time"
)

func SetLogFile(host, port string) (*os.File, error) {
	wd, _ := os.Getwd()
	logFilename := wd + "/log/" + host + "_" + port + "_" + time.Now().Format("20060102-150405") + ".log"
	err := os.MkdirAll("log/", os.ModePerm)
	if err != nil {
		fmt.Println("Can't create or open temp folder. ", err)
		return nil, err
	}
	file, err := os.OpenFile(logFilename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatalln("Failed to open log file:", err)
		return nil, err
	}
	return file, nil
}

// Input file, return file size, checksum.
func FileInfo(filename string) (uint64, string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return 0, "", err
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return 0, "", err
	}

	// get file size
	size := uint64(fileInfo.Size())

	// get checksum
	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		panic(err)
	}
	checksum := hash.Sum(nil)
	checksumStr := fmt.Sprintf("%x", checksum)

	return size, checksumStr, nil
}

func CheckFileExist(filename string) bool {
	_, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	if err != nil {
		panic(err)
	}
	return true
}

// Check file with given checksum
func FileCheck(filename string, tSize uint64, tChecksum string) bool {
	size, checksum, err := FileInfo(filename)
	if err != nil {
		log.Fatalln(err.Error())
		return false
	}
	if size != tSize || checksum != tChecksum {
		return false
	} else {
		return true
	}
}

func SendChunk(msgHandler *MessageHandler, chunk *Chunk) bool {
	// chunk data stream in rawChunkReq is unchecked.
	// Check chunk data with checksum
	checksumStr := fmt.Sprintf("%x", md5.Sum(chunk.GetDataStream()))
	if chunk.Checksum != checksumStr {
		return false
	}
	reqMsg := Request{
		Req: &Request_ChunkReq{
			ChunkReq: &ChunkReq{
				GetReq:    false,
				ChunkData: chunk,
			},
		},
	}
	wrapper := &Wrapper{
		Msg: &Wrapper_RequestMsg{
			RequestMsg: &reqMsg,
		},
	}
	msgHandler.Send(wrapper)
	return true
}

// Input msgHandler and chunk's checksum,
// the method will verify chunk and return chunk structure.
func ReceiveChunk(msgHandler *MessageHandler, checksum string) (*Chunk, error) {
	wrapper, err := msgHandler.Receive()
	if err != nil {
		log.Fatalln(err.Error())
		return &Chunk{}, err
	}

	switch msg := wrapper.Msg.(type) {
	case *Wrapper_RequestMsg:
		// Handle chunk request
		req, ok := msg.RequestMsg.Req.(*Request_ChunkReq)
		if ok && !req.ChunkReq.GetGetReq() {
			return req.ChunkReq.ChunkData, nil
		}
	case *Wrapper_ResponseMsg:
		// Handle chunk response
		res, ok := msg.ResponseMsg.Res.(*Response_ChunkRes)
		if ok && res.ChunkRes.GetStatus() {
			return res.ChunkRes.ChunkData, nil
		}
	}
	return &Chunk{}, err
}
