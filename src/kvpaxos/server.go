package main
import (
	"fmt"
	"net/http"
	"log"
	"encoding/json"
	//"net/rpc"
	"strconv"
	"io/ioutil"
	"strings"
	"sync"
	"os"
	"net/url"
	"io"
	"paxos"
	"strconv"
)

var DataBase = struct{
    sync.Mutex
    data map[string]string
}{data: make(map[string]string)}

var dataLockLock sync.Mutex
var dataLock map[string]*sync.Mutex = make(map[string]*sync.Mutex)
var me int
var meStr string
var px Paxos

const MODE = "test"

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

func DataLockFind(key string) *sync.Mutex {
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

func Insert(key, value string) bool {
	//DataBase.RLock()
	_, ok := DataBase.data[key]
	//DataBase.RUnlock()
	if(ok) {
		PrintLog(MODE, "In Insert, key " + key + " already exists")
		return false
	}
	//DataBase.Lock()
	DataBase.data[key] = value
	//DataBase.Unlock()
	PrintLog(MODE, "Inserted " + key + ":" + value)	
	return true
}

func HandleInsert(w http.ResponseWriter, request *http.Request) {
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
	
	var propose paxos.ProposeValue
	propose.Mes = "insert"
	propose.Id = id
	propose.Server = me
	propose.Key = key
	
	seqLock.Lock()
	seq++
	seq1 := seq
	seqLock.Unlock()
	propose.Seq = seq1
	queue[seq1].Lock()
	px.Start(seq1, propose)

	queue[seq1].Lock()
	queue[seq1].Unlock()
	queue[seq1] = nil

	elementLock := DataLockFind(key)
	elementLock.Lock()
	{	
		DataBase.Lock()
		succ_insert := Insert(key, value)
		if(!succ_insert){
			DataBase.Unlock()
			elementLock.Unlock()
			fmt.Fprintln(w, UnsuccessResponse("In insert, key already exists"))
			return
		}
		DataBase.Unlock()
	}
	elementLock.Unlock()

	json_encode, _ := json.Marshal(map[string]string{
			"success":"true", 
	})		
	fmt.Fprintln(w, string(json_encode))
}

func Delete(key string) (bool, string) {
	//DataBase.RLock()
	value, ok := DataBase.data[key]
	//DataBase.RUnlock()
	if(!ok) {
		PrintLog(MODE, "In Delete, key " + key + " not exists")
		return false, ""
	}
	//DataBase.Lock()
	delete(DataBase.data, key)
	//DataBase.Unlock()
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
	
	var propose paxos.ProposeValue
	propose.mes = "delete"
	propose.id = id
	propose.server = me
	propose.key = key
	
	seqLock.Lock()
	seq++
	seq1 := seq
	seqLock.Unlock()
	propose.seq = seq1
	queue[seq1].Lock()
	px.Start(seq1, propose)

	queue[seq1].Lock()
	queue[seq1].Unlock()
	queue[seq1] = nil

	elementLock := DataLockFind(key)
	elementLock.Lock()
	
		response, err := http.PostForm("http://" + backup_address + "/kv/delete", 
    		url.Values{"key": {key}})

  		if err != nil {
    		PrintLog(MODE, "Post Delete to Backup: " + err.Error())
    		//fmt.Fprintln(w, UnsuccessResponse("In delete, backup fails"))
    		//DataBase.Unlock()
    		//return
  		} else {	 
  			io.Copy(ioutil.Discard, response.Body) 
			response.Body.Close()
		}
		DataBase.Lock()
		succ_delete, delete_value := Delete(key)
		if(!succ_delete){
			DataBase.Unlock()
			elementLock.Unlock()
			fmt.Fprintln(w, UnsuccessResponse("In delete, key not exists"))
			return
		}	
		DataBase.Unlock()
		
	elementLock.Unlock()
	
	json_encode, _ := json.Marshal(map[string]string{
		"success":"true", "value":delete_value,
	})		
	fmt.Fprintln(w, string(json_encode))		
}

func Get(key string) (bool, string) {
	//DataBase.RLock()
	value, ok := DataBase.data[key]
	//DataBase.RUnlock()
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
	
	var propose paxos.ProposeValue
	propose.mes = "get"
	propose.id = idt
	propose.server = me
	propose.key = key
	
	seqLock.Lock()
	seq++
	seq1 := seq
	seqLock.Unlock()
	propose.seq = seq1
	queue[seq1].Lock()
	px.Start(seq1, propose)

	queue[seq1].Lock()
	queue[seq1].Unlock()
	queue[seq1] = nil

	elementLock := DataLockFind(key)
	elementLock.Lock()
	{

		DataBase.Lock()
		succ_get, get_value := Get(key)
		if(!succ_get){
			DataBase.Unlock()
			fmt.Fprintln(w, UnsuccessResponse("In get, key not exists"))
			return
		}
		DataBase.Unlock()
	}
	elementLock.Unlock()
			
	json_encode, _ := json.Marshal(map[string]string{
		"success":"true", "value":get_value,
	})		
	fmt.Fprintln(w, string(json_encode))		

}

func Update(key string, value string) (bool) {
	//DataBase.RLock()
	_, ok := DataBase.data[key]
	//DataBase.RUnlock()
	if(!ok) {
		PrintLog(MODE, "In Update, key " + key + " not exists")
		return false
	}
	//DataBase.Lock()
	DataBase.data[key] = value
	//DataBase.Unlock()
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
	
	var propose paxos.ProposeValue
	propose.mes = "update"
	propose.id = id
	propose.server = me
	propose.key = key
	propose.value = value
	
	seqLock.Lock()
	seq++
	seq1 := seq
	seqLock.Unlock()
	propose.seq = seq1
	queue[seq1].Lock()
	px.Start(seq1, propose)

	queue[seq1].Lock()
	queue[seq1].Unlock()
	queue[seq1] = nil

	elementLock := DataLockFind(key)
	elementLock.Lock()
	{
		DataBase.Lock()
		succ_update := Update(key, value)
		if(!succ_update){
			DataBase.Unlock()
			elementLock.Unlock()
			fmt.Fprintln(w, UnsuccessResponse("In update, key not exists"))
			return
		}
		DataBase.Unlock()
	}	
	elementLock.Unlock()
	
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
	
	DataBase.Lock()
	total_key := strconv.Itoa(len(DataBase.data))
	PrintLog(MODE, "inquiring count key : " + total_key)
	ret, _ := json.Marshal(map[string]string{
		"result":total_key, "mes":"true",
	})
	fmt.Fprintln(w, string(ret))
	DataBase.Unlock()
}

func HandleDump(w http.ResponseWriter, request *http.Request) {
	if(request.ParseForm() != nil) {
		fmt.Fprintln(w, UnsuccessResponse("In HandleUpdate, fail to parse URL"))
		PrintLog(MODE, "In HandleUpdate, fail to parse URL")
		return
	}
	
	DataBase.Lock()
	total_key := strconv.Itoa(len(DataBase.data))
	PrintLog(MODE, "dumping database : " + total_key)
	ret, _ := json.Marshal(DataBase.data)
	fmt.Fprintln(w, string(ret))
	DataBase.Unlock()
}

func getNextOperation(seq int) paxos.ProposeValue {
	
	to := 10 * time.Millisecond
	for {
		decided, v := px.Status(seq)
		if decided {
			return v 
		}
		time.Sleep(to)
		if to < 10 * time.Second {
			to *= 2
		}	
	}
} 

func HandleDecidedOperation(){
	nextSeq := 0
	for {
		v := getStatus(nextSeq)
	
		if v.server == me && (v.id == "-1" || !idData[v.id]) {
			idData[v.id] = true
			decide[nextSeq] = v
			queue[nextSeq].Unlock()
		}
		else queue[nextSeq] = nil
		nextSeq ++
	}
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
	M := len(config) - 1
	serverPeers = make([]string, M, M)
	paxosPeers = make([]string, M, M)
	for i := 0; i < M; i++ {
		serverPeers[i] = config["N"+strconv.Itoa((i+1)/10)+strconv.Itoa((i+1)%10)]+ ":" + config["Port"]
		paxosPeers[i] = config["N"+strconv.Itoa((i+1)/10)+strconv.Itoa((i+1)%10)]+ ":" + 
				strcov.Itoa((strcov.Atoi(config["Port"]) + 1))
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
	
	me := strcov.Atoi(os.Args[1]) - 1
	meStr = strcov.Itoa(me)
	
	serverPeers, paxosPeers := DecodeConfig()
	
	if me >= len(serverPeers) || me < 0 {
		fmt.Println("Error! Node id invaild!")
		return
	}
	
	px := paxos.Make(paxosPeers, me)
	
	err := http.ListenAndServe(serverPeers[args], nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err.Error())
	}
	
	os.Exit(0) //why need os.Exit?
}
