package clientrpc

import (
	"net/rpc"
	"strconv"
	"sync"
	"time"

	"github.com/okuralabs/okura-node/logger"
	"github.com/okuralabs/okura-node/tcpip"
)

const (
	retryInterval = 5 * time.Second
	bufferSize    = 1024 * 1024
)

var InRPC = make(chan []byte)
var OutRPC = make(chan []byte)
var muRPC sync.Mutex

func ConnectRPC(ip string) {
	address := ip + ":" + strconv.Itoa(tcpip.Ports[tcpip.RPCTopic])
	var client *rpc.Client
	var err error

	// Inicjalne połączenie
	for {
		client, err = rpc.Dial("tcp", address)
		if err == nil {
			break
		}
		logger.GetLogger().Printf("Failed to connect to RPC server at %s: %v. Retrying in %v...", address, err, retryInterval)
		time.Sleep(retryInterval)

	}

	line := <-InRPC
	muRPC.Lock()
	defer muRPC.Unlock()
	reply := make([]byte, bufferSize)
	err = client.Call("Listener.Send", line, &reply)

	if err != nil {
		logger.GetLogger().Printf("RPC call failed: %v. Reconnecting...", err)

		// Reconnect loop
		// for {
		// 	client, err = rpc.Dial("tcp", address)
		// 	if err == nil {
		// 		break
		// 	}
		// 	logger.GetLogger().Printf("Failed to reconnect to RPC server at %s: %v. Retrying in %v...", address, err, retryInterval)
		// 	time.Sleep(retryInterval)
		// }
	}

	OutRPC <- reply

}
