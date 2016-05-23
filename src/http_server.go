package main
import (
	"fmt"
	"net/http"
	"log"
	//"encoding/json"
	//"net"
	//"net/rpc"
	//"time"
)

const SERVERADDRESS = "127.0.0.1:1234"
var DataBase map[string]string

const MODE = "debug"

func PrintLog(condition string, log_content string) {
	if(condition == "debug") {
		fmt.Println(log_content)
	}
}

func UnsuccessResponse(error_message string) string {
	PrintLog(MODE, error_message)

	return ""
	//json_encode, json_ok = json.Marshal(map[string]string{
	//	"success":strconv.FormatBool(success),
	//})	
}

func HandleInsert(w http.ResponseWriter, request *http.Request) {
	key_list, key_ok := request.Form["key"]
	value_list, value_ok := request.Form["value"]
	if(!key_ok || !value_ok) {
		UnsuccessResponse("in insert, key or value input not correct")
		fmt.Println(len(key_list))
		fmt.Println(len(value_list))
		return
	}

	var key string = key_list[0]
	var value string = value_list[0]
	PrintLog(MODE, "Inserting " + key + ":" + value)

	_, ok := DataBase[key]
	if(ok){
		PrintLog(MODE, "key " + key + " already exists")
		//fmt.Fprintln(w, string(response))
	} else {
		DataBase[key] = value
		PrintLog(MODE, "Inserted " + key + ":" + value)
	}
}

func HandleDelete(w http.ResponseWriter, request *http.Request) {
	
}

func HandleGet(w http.ResponseWriter, request *http.Request) {
	
}

func HandleUpdate(w http.ResponseWriter, request *http.Request) {
	
}

func HandleDefault(w http.ResponseWriter, request *http.Request) {
	fmt.Println("In HandleDefault")
    fmt.Fprintf(w, "Hello,"+request.URL.Path[1:])
}


func main() {
	http.HandleFunc("/", HandleDefault)
	http.HandleFunc("/kv/insert", HandleInsert)
	http.HandleFunc("/kv/delete", HandleDelete)
	http.HandleFunc("/kv/get", HandleGet)
	http.HandleFunc("/kv/update", HandleUpdate)
	
    err := http.ListenAndServe(SERVERADDRESS, nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err.Error())
	}
}
