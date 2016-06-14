package main

import (
	"net"
	"net/rpc"
	"strings"
	"log"
	
//	"os"
//	"syscall"
	"sync"
	"sync/atomic"
	"fmt"
//	"math/rand"
)

const totalServer := 7

type PaxosInstance struct {

	lock sync.Mutex
	ownValue string
	prepareN int
	proposeN int
	proposeV int
	decided bool
}

type Paxos struct{
	me int
	peers []string
	minimum int
	instance []*PaxosInstance
	l  net.Listener
	rpcCount int32
}

type prepareMessage struct{
	seq int
	prepareN int
}

type prepareACK struct{
	mes string
	proposeN int
	proposeV int
}

type proposeMessage struct{
	seq int
	proposeN int
	proposeV int
}

type proposeACK struct{
	mes string
	proposeN int
}

type proposeValue struct{
}

func (px *Paxos) isdead() bool {
	return false
}

func (px *Paxos) prepareHandler(args *prepareMessage, reply *prepareACK) error {
	if args.seq > len(px.instance) {
		reply.mes = "ignore"
		return nil
	}
	p := px.instance[args.seq]
	if args.prepareN > p.prepareN {
		p.prepareN = args.prepareN
		reply.mes = "ok"
		reply.proposeN = p.proposeN
		reply.proposeV = p.proposeV	
	} else {
		reply.mes = "reject"
		reply.proposeN = p.proposeN
		reply.proposeV = p.proposeV
	}
	return nil
}

func (px *Paxos) acceptHandler(args *proposeMessage, reply *proposeACK) error {
	if args.seq > len(px.instance) {
		reply.mes = "ignore"
		return nil
	}
	p := px.instance[args.seq]
	if args.proposeN >= p.prepareN {
		p.prepareN = args.proposeN
		p.proposeN = args.proposeN
		p.proposeV = args.proposeV
		reply.mes = "ok"
		reply.proposeN = p.prepareN
	} else {
		reply.mes = "reject"
		reply.proposeN = p.prepareN
	}
	return nil
}

prepareRes 
proposeRes

func (px *Paxos) propose(seq int, value interface{}) {
	//wait for complete
	p := px.instance[seq]
	currentMaxN := 0
	for p.decided == false {
		chooseN := (currentMaxN / totalServer + 1) * totalServer + px.me
		mes := &prepareMessage{seq, chooseN}
		// wait for modification
		for i := 1; i<= totalServer; i++ {
			reply := &prepareACK{}
			go call(px.peers[i], "Paxos.prepareHandler", mes, reply, "prepare")
		}
		for len(prepareRes)*2 <= totalServer {
		}
		
		for _,reply := range(prepareRes) {
			
			if reply.proposeN > currentMaxN {
				currentMaxN = reply.proposeN
			}
			
			
		}
		
	} 
}

func (px *Paxos) start(seq int, value interface{}){
	go px.propose(seq, value)
}


func Make(peers []string, me int) *Paxos { 
	px := new(Paxos)
	rpc.Register(px)
	l, e := net.Listen("tcp", strings.Split(peers[me], ":")[1])
	if e != nil {
			log.Fatal("listen error: ", e)
		}
	px.l = l
	go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					atomic.AddInt32(&px.rpcCount, 1)
					go rpc.ServeConn(conn)
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept the RPC connection: %v\n", me, err.Error())
				}
			}
		}()	
	return px	
}

func call(srv string, name string, args interface{}, reply interface{}, specification string) bool {
	c, err := rpc.Dial("tcp", srv)
	if err != nil {
		err1 := err.(*net.OpError)
	//	if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
	//	}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		if specification == "prepare" {
			prepareRes = append(prepareRes, reply)
		} else if specification == "propose" {
			proposeRes = append(proposeRes, reply)
		}
		return true
	}
	fmt.Println(err)
	return false
}

func main() {
	//Make({"127.0.0.1:1234"}, 0)
}
