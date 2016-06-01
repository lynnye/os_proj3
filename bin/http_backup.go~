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
)

var DataBase = struct{
    sync.RWMutex
    data map[string]string
}{data: make(map[string]string)}

const MODE = "test"

var server_address, backup_address string

func PrintLog(condition string, log_content string) {
	if(condition == "debug") {
		fmt.Println("In backup server: " + log_content)
	}
}

func UnsuccessResponse(error_message string) string {
	json_encode, _ := json.Marshal(map[string]string{
		"success":"false","error":error_message,
	})	
	return string(json_encode)
}

func Insert(key, value string) bool {
	DataBase.RLock()
	_, ok := DataBase.data[key]
	DataBase.RUnlock()
	if(ok) {
		PrintLog(MODE, "In Insert, key " + key + " already exists")
		return false
	}
	DataBase.Lock()
	DataBase.data[key] = value
	DataBase.Unlock()
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
	if(!key_ok || !value_ok || len(key_list)!=1 || len(value_list)!=1) {
		fmt.Fprintln(w, UnsuccessResponse("In insert, key or value input not correct"))
		return
	}

	var key string = key_list[0]
	var value string = value_list[0]

	succ_insert := Insert(key, value)
	if(!succ_insert){
		fmt.Fprintln(w, UnsuccessResponse("In insert, key already exists"))
		return
	} 
	
	json_encode, _ := json.Marshal(map[string]string{
		"success":"true", "error":"", 
	})		
	fmt.Fprintln(w, string(json_encode))
	//request.Body.Close()	
}

func Delete(key string) (bool, string) {
	DataBase.RLock()
	value, ok := DataBase.data[key]
	DataBase.RUnlock()
	if(!ok) {
		PrintLog(MODE, "In Delete, key " + key + " not exists")
		return false, ""
	}
	DataBase.Lock()
	delete(DataBase.data, key)
	DataBase.Unlock()
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
	if(!key_ok || len(key_list)!=1) {
		fmt.Fprintln(w, UnsuccessResponse("In delete, key input not correct"))
		return
	}

	var key string = key_list[0]

	succ_delete, delete_value := Delete(key)
	if(!succ_delete){
		fmt.Fprintln(w, UnsuccessResponse("In delete, key not exists"))
		return
	} 
	
	json_encode, _ := json.Marshal(map[string]string{
		"success":"true", "value":delete_value, "error":"", 
	})		
	fmt.Fprintln(w, string(json_encode))
}

func Get(key string) (bool, string) {
	DataBase.RLock()
	value, ok := DataBase.data[key]
	DataBase.RUnlock()
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
	if(!key_ok || len(key_list)!=1) {
		fmt.Fprintln(w, UnsuccessResponse("In get, key input not correct"))
		return
	}

	var key string = key_list[0]

	succ_get, get_value := Get(key)
	if(!succ_get){
		fmt.Fprintln(w, UnsuccessResponse("In get, key not exists"))
		return
	} 
	
	json_encode, _ := json.Marshal(map[string]string{
		"success":"true", "value":get_value, "error":"", 
	})		
	fmt.Fprintln(w, string(json_encode))
}

func Update(key string, value string) (bool) {
	DataBase.RLock()
	_, ok := DataBase.data[key]
	DataBase.RUnlock()
	if(!ok) {
		PrintLog(MODE, "In Update, key " + key + " not exists")
		return false
	}
	DataBase.Lock()
	DataBase.data[key] = value
	DataBase.Unlock()
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
	if(!key_ok || !value_ok || len(key_list)!=1 || len(value_list)!=1) {
		fmt.Fprintln(w, UnsuccessResponse("In update, key or value input not correct"))
		return
	}

	var key string = key_list[0]
	var value string = value_list[0]

	succ_update := Update(key, value)
	if(!succ_update){
		fmt.Fprintln(w, UnsuccessResponse("In update, key not exists"))
		return
	} 
	
	json_encode, _ := json.Marshal(map[string]string{
		"success":"true", "error":"", 
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
		"result":total_key,
	})
	fmt.Fprintln(w, string(ret))
}

func HandleDump(w http.ResponseWriter, request *http.Request) {
	total_key := strconv.Itoa(len(DataBase.data))
	PrintLog(MODE, "dumping database : " + total_key)
	ret, _ := json.Marshal(DataBase.data)
	fmt.Fprintln(w, string(ret))
}

func HandleShutdown(w http.ResponseWriter, request *http.Request) {
	DataBase.Lock()
	PrintLog(MODE, "In shutdown, backup server shutdown")
	os.Exit(0)
}

func DecodeConfig() (string, string) {
	config_file, err := ioutil.ReadFile("../conf/settings.conf")
	if err != nil{
		fmt.Println("Load config file error")
		log.Fatal("Load config gile: ", err.Error())
	}
	type server_config struct{
		Primary, Backup, Port string
	}
	dec := json.NewDecoder(strings.NewReader(string(config_file)))
	var config server_config;
	err = dec.Decode(&config)
	if err != nil {
		fmt.Println("Parse config file error")
		log.Fatal("Parse config file(Json): ", err.Error())
	}
	return config.Primary + ":" + config.Port, config.Backup + ":" + config.Port
}

func InitialDump() {
	response, err := http.Get("http://" + server_address + "/kvman/dump")
	if err != nil {
    	//fmt.Println("Initial dump: ", err.Error())
    	return
  	} 
	defer response.Body.Close()
  	dec := json.NewDecoder(response.Body)
	dumped_data := make(map[string]string)
	dec.Decode(&dumped_data)
	PrintLog(MODE, "Initial Dumped data:")
	for key, value := range dumped_data {
    	PrintLog(MODE, "Key:" + key + "    Value:" + value)
	} 
	DataBase.data = dumped_data
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
	server_address, backup_address = DecodeConfig()
	PrintLog(MODE, "server address: " + server_address)
	PrintLog(MODE, "backup address: " + backup_address)
	InitialDump()
	err := http.ListenAndServe(backup_address, nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err.Error())
	}
}
