package main
import (
	"fmt"
	"net/http"
	"log"
	"encoding/json"
	//"net/rpc"
	"io/ioutil"
	"strings"
	"sync"
	"os"
//	"net/url"
//	"io"
	"paxos"
	"strconv"
	"time"
	"encoding/gob"
)

type ProposeValue struct{
	Seq int
	Mes string
	Id string
	Me int
	Key string
	Value string
	Succ bool
}

var DataBase = struct{
    sync.Mutex
    data map[string]string
}{data: make(map[string]string)}
/*
var dataLockLock sync.Mutex
var dataLock map[string]*sync.Mutex = make(map[string]*sync.Mutex)
*/
var me int
var meStr string
var px *paxos.Paxos

var seq int = -1
var seqLock sync.Mutex

var queue []chan ProposeValue
var queueLock sync.Mutex

var idData map[string]bool = make(map[string]bool)
var idDataValue map[string]string = make(map[string]string)

var isDead bool

const MODE = "debug"

func PrintLog(condition string, log_content string) {
	if(condition == "debug") {
		fmt.Println("In server " + meStr + ": " + log_content)
	}
}

func UnsuccessResponse(error_message string) string {
	json_encode, _ := json.Marshal(map[string]string{
		"success":"false","error":error_message,
	})	
	return string(json_encode)
}

func getNextOperation(seq int) ProposeValue {
	
	to := 10 * time.Millisecond
	for {
		decided, v := px.Status(seq)
		
		if decided {
			return v.(ProposeValue) 
		}
		//PrintLog("debug", "wait for decided")
		
		time.Sleep(to)
		if to < 10 * time.Second {
			to *= 2
		}	
	}
} 

func HandleDecidedOperation(){
	nextSeq := 0
	for {
		//PrintLog("debug", "handleDecidedOperation")
		v := getNextOperation(nextSeq)
			
		//PrintLog("debug", v.Mes+" "+v.Key+" "+v.Value)
		_, ok := idData[v.Id]
		
		if (v.Id == "-1" || !ok) {  
		
			switch (v.Mes) {
			case "insert":
				v.Succ = Insert(v.Key, v.Value)
			case "delete":
				v.Succ, v.Value = Delete(v.Key)
			case "get":
				v.Succ, v.Value = Get(v.Key)
			case "update":
				v.Succ = Update(v.Key, v.Value)
			}
			
			idData[v.Id] = v.Succ
			idDataValue[v.Id] = v.Value
			
		} else {
			v.Succ = idData[v.Id]
			v.Value = idDataValue[v.Id]
		}
		queueAdd(nextSeq, v)
		nextSeq ++
	}
}

func queueAdd(seq int, v ProposeValue) {

	//PrintLog("debug", "queue add started")
	queueLock.Lock()
	//PrintLog("debug", "queue add get lock")
	if len(queue) <= seq {
		queue = append(queue, make(chan ProposeValue, 1))
	}
	
	queueLock.Unlock()
	//PrintLog("debug", "queue add release lock")
	queue[seq] <- v
	
	//PrintLog("debug", "queue add finished")
}

func queueDelete(seq int) (v ProposeValue) {
	queueLock.Lock()
	if len(queue) <= seq {
		queue = append(queue, make(chan ProposeValue))
	}
	queueLock.Unlock()
	
	v = <- queue[seq]
	queue[seq] = nil
	return v
}


func WaitDecided(mes string, id string, me int, key string, value string) (bool, string){
	var v ProposeValue
	v.Mes = mes
	v.Id = id
	v.Me = me
	v.Key = key
	v.Value = value
	
	for {
		//PrintLog("debug", "start try paxos instance")
		seqLock.Lock()
		seq++
		seq1 := seq
		seqLock.Unlock()
		v.Seq = seq1
		px.Start(seq1, v)

		v1 := queueDelete(seq1)
		//PrintLog("debug", "try paxos instance, get a decided value")
		//fmt.Println(v.Me, v1.Me, v.Seq, v1.Seq)
		if (v.Me == v1.Me && v.Seq == v1.Seq) {
			return v1.Succ, v1.Value
		}
		
	}
}

/*func DataLockFind(key string) *sync.Mutex {
	dataLockLock.Lock()
	defer dataLockLock.Unlock()
	elementLock, ok := dataLock[key]
	if ok == true {
		return elementLock 
	} else {
		dataLock[key] = new(sync.Mutex)
		return dataLock[key]
	}
}
*/
func Insert(key, value string) bool {
	_, ok := DataBase.data[key]
	if(ok) {
		PrintLog(MODE, "In Insert, key " + key + " already exists")
		return false
	}
	DataBase.data[key] = value
	PrintLog(MODE, "Inserted " + key + ":" + value)	
	return true
}


func HandleInsert(w http.ResponseWriter, request *http.Request) {
	
	//PrintLog("debug", "before insert")
	
	if(request.ParseForm() != nil) {
		fmt.Fprintln(w, UnsuccessResponse("In HandleInsert, fail to parse URL"))
		PrintLog(MODE, "In HandleInsert, fail to parse URL")
		return
	}
	
	
	key_list, key_ok := request.Form["key"]
	value_list, value_ok := request.Form["value"]
	id_list, id_ok := request.Form["requestid"]
	
	if(!key_ok || !value_ok || len(key_list)!=1 || len(value_list)!=1) {
		fmt.Fprintln(w, UnsuccessResponse("In insert, key or value input not correct"))
		return
	}
	var key string = key_list[0]
	var value string = value_list[0]

	var id string
	if (id_ok && len(id_list) == 1) {
		id = request.RemoteAddr + id_list[0]
	} else {
		id = "-1"
	}
	
	succ, _ := WaitDecided("insert", id, me, key, value)

	if(!succ){
		fmt.Fprintln(w, UnsuccessResponse("In insert, key already exists"))
		return	
	}
	
	json_encode, _ := json.Marshal(map[string]string{
			"success":"true", 
	})		
	fmt.Fprintln(w, string(json_encode))
}

func Delete(key string) (bool, string) {
	value, ok := DataBase.data[key]
	if(!ok) {
		PrintLog(MODE, "In Delete, key " + key + " not exists")
		return false, ""
	}
	delete(DataBase.data, key)
	PrintLog(MODE, "Deleted " + key + ":" + value)
	return true, value
}

func HandleDelete(w http.ResponseWriter, request *http.Request) {
	if(request.ParseForm() != nil) {
		fmt.Fprintln(w, UnsuccessResponse("In HandleDelete, fail to parse URL"))
		PrintLog(MODE, "In HandleDelete, fail to parse URL")
		return
	}
	key_list, key_ok := request.Form["key"]
	id_list, id_ok := request.Form["requestid"]
	
	if(!key_ok || len(key_list)!=1) {
		fmt.Fprintln(w, UnsuccessResponse("In delete, key input not correct"))
		return
	}

	var key string = key_list[0]

	var id string
	if id_ok && len(id_list) == 1 {
		id = request.RemoteAddr + id_list[0]
	} else {
		id = "-1"
	}
	
	succ, delete_value := WaitDecided("delete", id, me, key, "")
	
	if(!succ){
		fmt.Fprintln(w, UnsuccessResponse("In delete, key not exists"))
		return
	}
	
	json_encode, _ := json.Marshal(map[string]string{
		"success":"true", "value":delete_value,
	})		
	fmt.Fprintln(w, string(json_encode))		
}

func Get(key string) (bool, string) {
	value, ok := DataBase.data[key]
	if(!ok) {
		PrintLog(MODE, "In Get, key " + key + " not exists")
		return false, ""
	}
	PrintLog(MODE, "Got " + key + ":" + value)
	return true, value
}

func HandleGet(w http.ResponseWriter, request *http.Request) {
	if(request.ParseForm() != nil) {
		fmt.Fprintln(w, UnsuccessResponse("In HandleGet, fail to parse URL"))
		PrintLog(MODE, "In HandleGet, fail to parse URL")
		return
	}
	key_list, key_ok := request.Form["key"]
	id_list, id_ok := request.Form["requestid"]
	if(!key_ok || len(key_list)!=1) {
		fmt.Fprintln(w, UnsuccessResponse("In get, key input not correct"))
		return
	}

	var key string = key_list[0]


	var id string
	if id_ok && len(id_list) == 1 {
		id = request.RemoteAddr + id_list[0]
	} else {
		id = "-1"
	}
	
	succ, get_value := WaitDecided("get", id, me, key, "")
	
	if(!succ){
		fmt.Fprintln(w, UnsuccessResponse("In get, key not exists"))
		return
	}
				
	json_encode, _ := json.Marshal(map[string]string{
		"success":"true", "value":get_value,
	})		
	fmt.Fprintln(w, string(json_encode))		

}

func Update(key string, value string) (bool) {
	_, ok := DataBase.data[key]
	if(!ok) {
		PrintLog(MODE, "In Update, key " + key + " not exists")
		return false
	}
	DataBase.data[key] = value
	PrintLog(MODE, "Updated " + key + ":" + value)	
	return true	
}

func HandleUpdate(w http.ResponseWriter, request *http.Request) {
	if(request.ParseForm() != nil) {
		fmt.Fprintln(w, UnsuccessResponse("In HandleUpdate, fail to parse URL"))
		PrintLog(MODE, "In HandleUpdate, fail to parse URL")
		return
	}
	key_list, key_ok := request.Form["key"]
	value_list, value_ok := request.Form["value"]
	id_list, id_ok := request.Form["requestid"]
	
	if(!key_ok || !value_ok || len(key_list)!=1 || len(value_list)!=1) {
		fmt.Fprintln(w, UnsuccessResponse("In update, key or value input not correct"))
		return
	}
	
	var key string = key_list[0]
	var value string = value_list[0]
	
	var id string
	if id_ok && len(id_list) == 1 {
		id = request.RemoteAddr + id_list[0]
	} else {
		id = "-1"
	}
	
	succ, _ := WaitDecided("update", id, me, key, value)
	
	if(!succ){
		fmt.Fprintln(w, UnsuccessResponse("In update, key not exists"))
		return
	}
	
	json_encode, _ := json.Marshal(map[string]string{
		"success":"true", 
	})		
	fmt.Fprintln(w, string(json_encode))		
}

func HandleDefault(w http.ResponseWriter, request *http.Request) {
	fmt.Println("In HandleDefault")
	fmt.Fprintf(w, "Hello,"+request.URL.Path[1:])
}

func HandleCountKey(w http.ResponseWriter, request *http.Request) {
	
	total_key := strconv.Itoa(len(DataBase.data))
	PrintLog(MODE, "inquiring count key : " + total_key)
	ret, _ := json.Marshal(map[string]string{
		"result":total_key, "mes":"true",
	})
	fmt.Fprintln(w, string(ret))
}

func HandleDump(w http.ResponseWriter, request *http.Request) {
	
	if(request.ParseForm() != nil) {
		fmt.Fprintln(w, UnsuccessResponse("In HandleUpdate, fail to parse URL"))
		PrintLog(MODE, "In HandleUpdate, fail to parse URL")
		return
	}
	
	total_key := strconv.Itoa(len(DataBase.data))
	PrintLog(MODE, "dumping database : " + total_key)
	ret, _ := json.Marshal(DataBase.data)
	fmt.Fprintln(w, string(ret))
}

func HandleShutdown(w http.ResponseWriter, request *http.Request) {
	//DataBase.Lock()
	PrintLog(MODE, "shutdown")
	isDead = true
}
func HandleRestart(w http.ResponseWriter, request *http.Request) {
	//DataBase.Lock()
	PrintLog(MODE, "restart")
	isDead = false
}

func DecodeConfig() (serverPeers, paxosPeers []string) {
	config_file, err := ioutil.ReadFile("../../conf/settings.conf")
	if err != nil{
		fmt.Println("Load config file error")
		log.Fatal("Load config gile: ", err.Error())
	}
	dec := json.NewDecoder(strings.NewReader(string(config_file)))
	var config map[string]interface{};
	err = dec.Decode(&config)
	if err != nil {
		fmt.Println("Parse config file error")
		log.Fatal("Parse config file(Json): ", err.Error())
	}
	/*for v := range(config) {
		PrintLog("debug", v)
	}*/
	M := len(config) - 1
	serverPeers = make([]string, M, M)
	paxosPeers = make([]string, M, M)
	
	port := config["port"].(string)
	t, err := strconv.Atoi(port)
	if err != nil {
		PrintLog("error", "port is invalid")
		os.Exit(0)
	}
	paxosPort := strconv.Itoa(t+1)
	 
	for i := 0; i < M; i++ {
		serverPeers[i] = config["n"+strconv.Itoa((i+1)/10)+strconv.Itoa((i+1)%10)].(string)+ ":" + port
		paxosPeers[i] = config["n"+strconv.Itoa((i+1)/10)+strconv.Itoa((i+1)%10)].(string)+ ":" + paxosPort
	}
	return serverPeers, paxosPeers
}

func main() {
	go http.HandleFunc("/", HandleDefault)
	go http.HandleFunc("/kv/insert", HandleInsert)
	go http.HandleFunc("/kv/delete", HandleDelete)
	go http.HandleFunc("/kv/get", HandleGet)
	go http.HandleFunc("/kv/update", HandleUpdate)
	go http.HandleFunc("/kvman/countkey", HandleCountKey)
	go http.HandleFunc("/kvman/dump", HandleDump)
	go http.HandleFunc("/kvman/shutdown", HandleShutdown)
	go http.HandleFunc("/kvman/restart", HandleRestart)
	
	gob.Register(ProposeValue{})

	var err error
	me, err = strconv.Atoi(os.Args[0])
	me = me - 1
	meStr = strconv.Itoa(me)
	
	serverPeers, paxosPeers := DecodeConfig()
	
	if err != nil || me >= len(serverPeers) || me < 0 {
		fmt.Println("Error! Node id invaild!")
		return
	}
	
	px = paxos.Make(paxosPeers, me)

	go HandleDecidedOperation()
	
	err = http.ListenAndServe(serverPeers[me], nil)
	
	if err != nil {
		log.Fatal("ListenAndServe: ", err.Error())
	}

	
	os.Exit(0) //why need os.Exit?
}
