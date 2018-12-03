package main

import (
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"simpledfs/utils"
	"strconv"
	"time"
)

const (
	BufferSize = 4096
)

// Usage of correct client command
func usage() {
	fmt.Println("Usage of ./client")
	fmt.Println("   -master=[master IP:Port] put [localfilename] [sdfsfilename]")
	fmt.Println("   -master=[master IP:Port] get [sdfsfilename] [localfilename]")
	fmt.Println("   -master=[master IP:Port] delete [sdfsfilename]")
	fmt.Println("   -master=[master IP:Port] ls [sdfsfilename]")
	fmt.Println("   -master=[master IP:Port] store")
	fmt.Println("   -master=[master IP:Port] get-versions [sdfsfilename] [num-versions] [localfilename]")
}

func contactNode(addr string) (net.Conn, error) {
	// Time out needed in order to deal with server failure.
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	printError(err)
	return conn, err
}

func filePut(pr utils.PutResponse, localfile string) {
	port := utils.StringPort(pr.NexthopPort)
	ip := utils.StringIP(pr.NexthopIP)

	conn, err := net.Dial("tcp", ip+":"+port)
	if err != nil {
		utils.PrintError(err)
		return
	}
	defer conn.Close()

	file, err := os.Open(localfile)
	utils.PrintError(err)

	wr := utils.WriteRequest{MsgType: utils.WriteRequestMsg}
	wr.FilenameHash = pr.FilenameHash
	wr.Filesize = pr.Filesize
	wr.Timestamp = pr.Timestamp
	wr.DataNodeList = pr.DataNodeList
	//wr.DataNodeList[0] = 0

	bin := utils.Serialize(wr)
	_, err = conn.Write(bin)
	utils.PrintError(err)

	buf := make([]byte, BufferSize)

	n, err := conn.Read(buf)
	for string(buf[:n]) != "OK" {
	}
	fmt.Println(string(buf[:n]))

	buf = make([]byte, BufferSize)
	for {
		n, err := file.Read(buf)
		conn.Write(buf[:n])
		if err == io.EOF {
			fmt.Println("Send file finish")
			break
		}
	}
}

func fileGet(gr utils.GetResponse, localfile string) {
	var conn net.Conn
	hasConn := false
	for index, ip := range gr.DataNodeIPList {
		var err error
		conn, err = net.Dial("tcp", utils.StringIP(ip)+":"+fmt.Sprintf("%d", gr.DataNodePortList[index]))
		utils.PrintError(err)
		if err == nil {
			hasConn = true
			fmt.Println("Dial ", utils.StringIP(ip), " successful")
			break
		} else {
			continue
		}
		defer conn.Close()
	}
	if hasConn == false {
		fmt.Println("Cannot dial all datanodes")
		return
	}

	file, err := os.Create(localfile)
	utils.PrintError(err)

	rr := utils.ReadRequest{MsgType: utils.ReadRequestMsg}
	rr.FilenameHash = gr.FilenameHash

	bin := utils.Serialize(rr)
	_, err = conn.Write(bin)
	fmt.Println("Sent ReadRequest")
	utils.PrintError(err)

	buf := make([]byte, BufferSize)
	var receivedBytes uint64

	for {
		n, err := conn.Read(buf)
		file.Write(buf[:n])
		receivedBytes += uint64(n)
		if err == io.EOF {
			fmt.Printf("Receive file with %d bytes\n", receivedBytes)
			break
		}

	}

	if gr.Filesize != receivedBytes {
		fmt.Println("Unmatched two files")
	}
}

// Put command execution for simpleDFS client
func putCommand(masterConn net.Conn, sdfsfile string, filesize uint64, localfile string) {
	prPacket := utils.PutRequest{MsgType: utils.PutRequestMsg, Filesize: filesize}
	copy(prPacket.Filename[:], sdfsfile)

	bin := utils.Serialize(prPacket)
	_, err := masterConn.Write(bin)
	printErrorExit(err)

	buf := make([]byte, BufferSize)
	n, err := masterConn.Read(buf)
	printErrorExit(err)

	response := utils.PutResponse{}
	utils.Deserialize(buf[:n], &response)
	if response.MsgType != utils.PutResponseMsg {
		fmt.Println("Unexpected message from MasterNode")
		return
	}
	fmt.Printf("%s %d %v\n", utils.Hash2Text(response.FilenameHash[:]), response.Timestamp, response.DataNodeList)

	go filePut(response, localfile)

	// Read Put Confirm
	buf = make([]byte, BufferSize)
	n, err = masterConn.Read(buf)
	printErrorExit(err)

	pc := utils.PutConfirm{}
	utils.Deserialize(buf[:n], &pc)
	if pc.MsgType != utils.PutConfirmMsg {
		fmt.Println("Unexpected message from MasterNode")
		return
	}
	fmt.Printf("[put confirm from master] %s put finished\n", utils.ParseFilename(pc.Filename[:]))
}

// Get command execution for simpleDFS client
func getCommand(masterConn net.Conn, sdfsfile string, localfile string) {
	grPacket := utils.GetRequest{MsgType: utils.GetRequestMsg}
	copy(grPacket.Filename[:], sdfsfile)

	bin := utils.Serialize(grPacket)
	_, err := masterConn.Write(bin)
	printErrorExit(err)

	buf := make([]byte, BufferSize)
	n, err := masterConn.Read(buf)
	printErrorExit(err)

	response := utils.GetResponse{}
	utils.Deserialize(buf[:n], &response)
	if response.MsgType != utils.GetResponseMsg {
		fmt.Println("Unexpected message from MasterNode")
		return
	}

	if response.Filesize == 0 {
		fmt.Printf("SDFS File %s does not exist\n", sdfsfile)
		return
	}

	fileGet(response, localfile)
}

func fileVersionGet(gvr utils.GetVersionsResponse, localfile string) {
	var conn net.Conn
	hasConn := false
	for index, ip := range gvr.DataNodeIPList {
		var err error
		conn, err = net.Dial("tcp", utils.StringIP(ip)+":"+fmt.Sprintf("%d", gvr.DataNodePortList[index]))
		utils.PrintError(err)
		if err == nil {
			hasConn = true
			fmt.Println("Dial ", utils.StringIP(ip), " successful")
			break
		} else {
			continue
		}
		defer conn.Close()
	}
	if hasConn == false {
		fmt.Println("Cannot dial all datanodes")
		return
	}

	file, err := os.Create(localfile + fmt.Sprintf("-v%d", gvr.Timestamp))
	utils.PrintError(err)

	rvr := utils.ReadVersionRequest{MsgType: utils.ReadVersionRequestMsg}
	rvr.FilenameHash = gvr.FilenameHash
	rvr.Timestamp = gvr.Timestamp

	bin := utils.Serialize(rvr)
	_, err = conn.Write(bin)
	fmt.Println("Sent ReadVersionRequest")
	utils.PrintError(err)

	buf := make([]byte, BufferSize)
	var receivedBytes uint64

	for {
		n, err := conn.Read(buf)
		file.Write(buf[:n])
		receivedBytes += uint64(n)
		if err == io.EOF {
			fmt.Printf("Receive versioned file with %d bytes\n", receivedBytes)
			break
		}

	}

	if gvr.Filesize != receivedBytes {
		fmt.Println("Unmatched two files")
	}
}

// Delete a SDFSFile command execution for simpleDFS client
func deleteCommand(masterConn net.Conn, sdfsfile string) {
	drPacket := utils.DeleteRequest{MsgType: utils.DeleteRequestMsg}
	copy(drPacket.Filename[:], sdfsfile)

	bin := utils.Serialize(drPacket)
	_, err := masterConn.Write(bin)
	printErrorExit(err)

	buf := make([]byte, BufferSize)
	n, err := masterConn.Read(buf)
	printErrorExit(err)

	response := utils.DeleteResponse{}
	utils.Deserialize(buf[:n], &response)
	if response.MsgType != utils.DeleteResponseMsg {
		fmt.Println("Unexpected message from MasterNode")
		return
	}

	if response.IsSuccess {
		fmt.Printf("SDFS File %s successfully deleted\n", sdfsfile)
	} else {
		fmt.Printf("SDFS File %s does not exist\n", sdfsfile)
	}

}

// List a SDFSFile command execution for simpleDFS client
func lsCommand(masterConn net.Conn, sdfsfile string) {
	lrPacket := utils.ListRequest{MsgType: utils.ListRequestMsg}
	copy(lrPacket.Filename[:], sdfsfile)

	bin := utils.Serialize(lrPacket)
	_, err := masterConn.Write(bin)
	printErrorExit(err)

	buf := make([]byte, BufferSize)
	n, err := masterConn.Read(buf)
	printErrorExit(err)

	response := utils.ListResponse{}
	utils.Deserialize(buf[:n], &response)
	if response.MsgType != utils.ListResponseMsg {
		fmt.Println("Unexpected message from MasterNode")
		return
	}

	if response.DataNodeIPList[0] == 0 {
		fmt.Printf("SDFS File %s does not exist\n", sdfsfile)
	} else {
		fmt.Println("SDFS File", sdfsfile, "stores in below addresses")
		for _, value := range response.DataNodeIPList {
			if value == 0 {
				break
			}
			fmt.Println(utils.StringIP(value))
		}
	}

}

// List all SDFSFile stored for simpleDFS client
func storeCommand(masterConn net.Conn) {
	srPacket := utils.StoreRequest{MsgType: utils.StoreRequestMsg}
	bin := utils.Serialize(srPacket)
	_, err := masterConn.Write(bin)
	printErrorExit(err)

	buf := make([]byte, 5)
	n, err := masterConn.Read(buf)
	printErrorExit(err)

	response := utils.StoreResponse{}
	utils.Deserialize(buf[:n], &response)
	if response.MsgType != utils.StoreResponseMsg {
		fmt.Println("Unexpected message from MasterNode")
		return
	}

	if response.FilesNum == 0 {
		fmt.Println("There is no any files")
		return
	}

	for i := uint32(0); i < response.FilesNum; i++ {
		buf = make([]byte, 128)
		_, err := masterConn.Read(buf)
		printErrorExit(err)
		fmt.Println(utils.ParseFilename(buf))
	}

	return
}

// Get number of versions of SDFSFile stored in simpleDFS
func getVersionsCommand(masterConn net.Conn, numVersion int64, sdfsfile string, localfile string) {
	gvrPacket := utils.GetVersionsRequest{MsgType: utils.GetVersionsRequestMsg, VersionNum: uint8(numVersion)}
	copy(gvrPacket.Filename[:], sdfsfile)
	bin := utils.Serialize(gvrPacket)
	_, err := masterConn.Write(bin)
	printErrorExit(err)

	versionNum := int64(-1)
	for i := int64(0); i < numVersion; i++ {
		if versionNum <= i && versionNum != -1 {
			break
		}

		buf := make([]byte, 74)
		n, err := masterConn.Read(buf)
		printErrorExit(err)

		response := utils.GetVersionsResponse{}
		utils.Deserialize(buf[:n], &response)
		if response.MsgType != utils.GetVersionsResponseMsg {
			fmt.Println("Unexpected message from MasterNode")
			return
		}

		if response.VersionNum == 0 {
			fmt.Println("There is no any files")
			return
		}

		versionNum = int64(response.VersionNum)

		fileVersionGet(response, localfile)
	}

	return
}

func main() {
	// If no command line arguments, return
	if len(os.Args) <= 1 {
		usage()
		return
	}
	ipPtr := flag.String("master", "xx.xx.xx.xx:port", "Master's IP:Port address")
	flag.Parse()
	masterAddr := *ipPtr
	fmt.Println("Master IP:Port address ", masterAddr)
	masterConn, err := contactNode(masterAddr)
	if err != nil {
		return
	}

	args := flag.Args()
	command := args[0]
	switch command {
	case "put":
		if len(args) != 3 {
			fmt.Println("Invalid put usage")
			usage()
		}
		fmt.Println(args[1:])
		localfile := args[1]
		sdfsfile := args[2]
		filenode, err := os.Stat(localfile)
		if err != nil {
			printError(err)
			return
		}
		fmt.Println(filenode.Size(), sdfsfile)
		putCommand(masterConn, sdfsfile, uint64(filenode.Size()), localfile)

	case "get":
		if len(args) != 3 {
			fmt.Println("Invalid get usage")
			usage()
		}
		fmt.Println(args[1:])
		sdfsfile := args[1]
		localfile := args[2]
		getCommand(masterConn, sdfsfile, localfile)

	case "delete":
		if len(args) != 2 {
			fmt.Println("Invalid delete usage")
			usage()
		}
		fmt.Println(args[1:])
		sdfsfile := args[1]
		deleteCommand(masterConn, sdfsfile)

	case "ls":
		if len(args) != 2 {
			fmt.Println("Invalid ls usage")
			usage()
		}
		fmt.Println(args[1:])
		sdfsfile := args[1]
		lsCommand(masterConn, sdfsfile)

	case "store":
		if len(args) != 1 {
			fmt.Println("Invalid store usage")
			usage()
		}
		storeCommand(masterConn)

	case "get-versions":
		if len(args) != 4 {
			fmt.Println("Invalid get-versions usage")
			usage()
		}
		fmt.Println(args[1:])
		sdfsfile := args[1]
		num := args[2]
		numInt, err := strconv.ParseInt(num, 10, 32)
		if err != nil {
			utils.PrintError(err)
			return
		}
		localfile := args[3]
		getVersionsCommand(masterConn, numInt, sdfsfile, localfile)

	default:
		usage()
	}
}

// Helper function to print the err in process
func printError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "\n[ERROR]", err.Error())
		fmt.Println(" ")
	}
}

func printErrorExit(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "\n[ERROR]\n", err.Error())
		fmt.Println(" ")
		// os.Exit(1)
	}
}
