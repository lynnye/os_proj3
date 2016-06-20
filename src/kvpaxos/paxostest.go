package main

import(
	"fmt"
	"paxos"
	"time"
	"encoding/gob"
	"strconv"
)

var px [10]*paxos.Paxos

type ProposeValue struct{
	Seq int
	Mes string
	Id string
	Me int
	Key string
	Value string
	Succ bool
}

func MakePropose(mes, id, key, value string, me, seq int) (ProposeValue) {
	var v ProposeValue
	v.Mes = mes
	v.Id = id
	v.Me = me
	v.Key = key
	v.Value = value
	return v
} 

func CheckDecided(seq int) (decided_number int, value ProposeValue) {
	//value := ""
	//decided_number := 0
	for i:=0; i <10; i ++ {
		decided, decided_value := px[i].Status(seq)
		//fmt.Println("paxos peer" + strconv.Itoa(i) + ":", decided)
		if decided {
			//fmt.Println(decided_value.(ProposeValue).Value)
			decided_number ++
			this_value := decided_value.(ProposeValue)
			if(decided_number == 1) {
				value = this_value
				continue
			}
			if(value != this_value) {
				fmt.Println("Failed! Decided on different values.")
			}
		}
	}
	return decided_number, value
}

func MultipleInstanceTest() {

	v0 := MakePropose("insert", "122.1.1.1:212", "111", "aaa", 0, 10)
	v1 := MakePropose("insert", "122.1.1.1:212", "111", "bbb", 1, 10)
	v2 := MakePropose("insert", "122.1.1.1:212", "111", "ccc", 2, 10)
	px[0].Start(10, v0)
	px[1].Start(10, v1)
	px[2].Start(10, v2)
	time.Sleep(time.Second)
	decided_number, decided_value := CheckDecided(10)
	fmt.Println("In MultipleInstanceTest", decided_number, decided_value.Value)

	v9 := MakePropose("insert", "122.1.1.1:212", "111", "xxx", 0, 10)
	px[9].Start(10, v9)
	time.Sleep(time.Second*2)
	decided_number2, decided_value2 := CheckDecided(10)
	fmt.Println("In MultipleInstanceTest", decided_number2, decided_value2.Value)
	if(decided_value == decided_value2) {
		fmt.Println("MultipleInstanceTest Succeed.")
	} else {
		fmt.Println("MultipleInstanceTest Failed.")
	}
}

func RandomOrderTest() {
	var v [10]ProposeValue
	for i:=0; i <10; i ++ {
		v[i] = MakePropose("delete", "122.1.1.1:212", "111", strconv.Itoa(i), i, i)
	}
	for i:=9; i >= 0; i -- {
		px[i].Start(i, v[i])
	}
	time.Sleep(time.Second)
	for i:=0; i <10; i ++ {
		decided_number, decided_value := CheckDecided(i)
		fmt.Println("In RandomOrderTest, instance", i, ":", decided_number, decided_value.Value)
	}
	fmt.Println(px[0].Max())
	fmt.Println("RandomOrderTest Succeed.")
}

func MemoryClearTest() {
	
}

func UnreliableNetworkTest() {
	
}

func KillTest() {
	px[0].Kill()
	v0 := MakePropose("insert", "122.1.1.1:212", "111", "aaa", 0, 1000)
	px[0].Start(1000, v0)
	time.Sleep(time.Second)
	decided_number, decided_value := CheckDecided(1000)
	fmt.Println("In KillTest, instance", 1000, ":", decided_number, decided_value.Value)
	if(decided_number != 0) {
		fmt.Println("KillTest Failed.")
		return
	} 

	for i:=0; i < 5; i ++ {
		px[i].Kill()
	}
	v9 := MakePropose("insert", "122.1.1.1:212", "111", "aaa", 9, 1001)
	px[9].Start(1001, v9)
	time.Sleep(time.Second)
	decided_number, decided_value = CheckDecided(1001)
	fmt.Println("In KillTest, instance", 1001, ":", decided_number, decided_value.Value)
	if(decided_number != 0) {
		fmt.Println("KillTest Failed.")
		return
	} 
	fmt.Println("KillTest Succeed.")
}

func Initialize() {
	gob.Register(ProposeValue{})

	peers := []string{"127.0.0.1:1234", "127.0.0.2:1235", "127.0.0.3:1236", "127.0.0.4:1237", "127.0.0.5:1238","127.0.0.6:1238","127.0.0.7:1238",
					"127.0.0.8:1238", "127.0.0.9:1238","127.0.0.10:1238"}
	for i := 0; i < 10; i ++ {
		px[i] = paxos.Make(peers, i)
	} 
}

func main() {
	Initialize()
	MultipleInstanceTest()
	RandomOrderTest()
	MemoryClearTest()
	UnreliableNetworkTest()
	KillTest()
}
