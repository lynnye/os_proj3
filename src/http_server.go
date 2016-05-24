package main
import (
	"fmt"
	"net/http"
	"log"
	//"encoding/json"
	//"net"
	//"net/rpc"
	//"time"
	"strconv"
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

func Insert(key, value string) {
	DataBase[key] = value
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
		// make response
		return
	} else {
		Insert(key, value)
		PrintLog(MODE, "Inserted " + key + ":" + value)
		// make response
	}
}

func Delete(key string) {

}

func HandleDelete(w http.ResponseWriter, request *http.Request) {
	key:=""
	Delete(key)
}

func Get(key string) string {

	return ""
}

func HandleGet(w http.ResponseWriter, request *http.Request) {
	key:=""
	Get(key)
	// make response
}

func Update(key string, value string) {

}

func HandleUpdate(w http.ResponseWriter, request *http.Request) {
	key, value := "", ""
	Update(key, value)	
}

func HandleDefault(w http.ResponseWriter, request *http.Request) {
	fmt.Println("In HandleDefault")
	fmt.Fprintf(w, "Hello,"+request.URL.Path[1:])
}

func HandleCountKey(w http.ResponseWriter, request *http.Request) {
	PrintLog(MODE, "inquiring count key : " + strconv.Itoa(len(DataBase)))

}

func HandleDump(w http.ResponseWriter, request *http.Request) {
	
}

func HandleShutdown(w http.ResponseWriter, request *http.Request) {
	
}

func main() {
	http.HandleFunc("/", HandleDefault)
	http.HandleFunc("/kv/insert", HandleInsert)
	http.HandleFunc("/kv/delete", HandleDelete)
	http.HandleFunc("/kv/get", HandleGet)
	http.HandleFunc("/kv/update", HandleUpdate)
	http.HandleFunc("/kvman/countkey", HandleCountKey)
	http.HandleFunc("/kvman/dump", HandleDump)
	http.HandleFunc("/kvman/shutdown", HandleShutdown)

	err := http.ListenAndServe(SERVERADDRESS, nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err.Error())
	}
}
