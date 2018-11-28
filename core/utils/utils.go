package utils

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"os/exec"
	"hash/fnv"
	"encoding/json"
	"plugin"
)

func Serialize(data interface{}) []byte {
	buf := bytes.Buffer{}
	binary.Write(&buf, binary.BigEndian, data)
	return buf.Bytes()
}

func Deserialize(data []byte, sample interface{}) {
	buf := bytes.NewReader(data)
	binary.Read(buf, binary.BigEndian, sample)
}

func ParseFilename(data []byte) string {
	n := bytes.IndexByte(data, 0)
	filename := fmt.Sprintf("%s", data[:n])
	return filename
}

func HashFilename(filename string) [32]byte {
	hash := sha256.Sum256([]byte(filename))
	return hash
}

func StringHashFilename(hash []byte) string {
	return fmt.Sprintf("%x", hash)
}

func Hash2Text(hashcode []byte) string {
	return base64.URLEncoding.EncodeToString(hashcode)
}

func BinaryIP(IP string) uint32 {
	return binary.BigEndian.Uint32(net.ParseIP(IP).To4())
}

func StringIP(binIP uint32) string {
	IP := make(net.IP, 4)
	binary.BigEndian.PutUint32(IP, binIP)
	return IP.String()
}

func StringPort(binPort uint16) string {
	return fmt.Sprint(binPort)
}

// Helper function to print the err in process
func PrintError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "\n[ERROR]", err.Error())
		fmt.Println(" ")
	}
}

// A trick to simply get local IP address
func GetLocalIP() net.IP {
	dial, err := net.Dial("udp", "8.8.8.8:80")
	PrintError(err)
	localAddr := dial.LocalAddr().(*net.UDPAddr)
	dial.Close()

	return localAddr.IP
}

// Return local FQDN
func GetLocalHostname() string {
	cmd := exec.Command("/bin/hostname", "-f")
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		fmt.Println(err.Error())
	}
	hostname := out.String()
	hostname = hostname[:len(hostname)-1] // removing EOL

	return hostname
}

func LookupIP(hostname string) string {
	addrs, err := net.LookupHost(hostname)
	if err != nil {
		fmt.Println(err.Error())
	}

	return addrs[0]
}

func Hash(value interface{}) int {
	bytes, _ := json.Marshal(value)
	h := fnv.New32a()
	h.Write(bytes)
	return int(h.Sum32())
}

func LookupProcFunc(pluginFile string, procFuncName string) func([]interface{}, *[]interface{}, *[]interface{}) error {
	// Load module
	plug, err := plugin.Open(pluginFile)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// Look up a symbol, in this case, the ProcFunc
	symProcFunc, err := plug.Lookup(procFuncName)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// Assert the symbol is the desired one
	var procFunc func([]interface{}, *[]interface{}, *[]interface{}) error
	procFunc, ok := symProcFunc.(func([]interface{}, *[]interface{}, *[]interface{}) error)
	if !ok {
		fmt.Println("unexpected type from module symbol")
		os.Exit(1)
	}

	return procFunc
}
