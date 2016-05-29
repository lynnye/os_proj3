package main 
import (
	"net/http"
	"net/url"
	"fmt"
	//"io/ioutil"
	"encoding/json"
	"strconv"
	"time"
	"os/exec"
	"os"
)

const serverAddress = "http://127.0.0.1:1234"

type BackupResponse struct{
	Success string
}

func Insert(key,value string) {
	response, err := http.PostForm(serverAddress + "/kv/insert", 
    	url.Values{"key": {key}, "value": {value}})
  	if err != nil {
    	fmt.Println("Post Insert: ", err.Error())
    	return
  	} 
 	defer response.Body.Close()
  	
  	dec := json.NewDecoder(response.Body)
  	type Insert struct{
		Success string
		Error string
	}
	var ret Insert
	dec.Decode(&ret)
	fmt.Println(ret.Success + ":" + ret.Error)
}

func InsertFalse(key,value string) {
	response, err := http.PostForm(serverAddress + "/kv/insert", 
    	url.Values{"keyfalse": {key}, "value": {value}})
  	if err != nil {
    	fmt.Println("Post Insert: ", err.Error())
    	return
  	} 
 	defer response.Body.Close()
  	
  	dec := json.NewDecoder(response.Body)
  	type Insert struct{
		Success string
		Error string
	}
	var ret Insert
	dec.Decode(&ret)
	fmt.Println(ret.Success + ":" + ret.Error)
}

func Delete(key string) {
	response, err := http.PostForm(serverAddress + "/kv/delete", 
    	url.Values{"key": {key}})
  	if err != nil {
    	fmt.Println("Post Delete: ", err.Error())
    	return
  	} 
 	defer response.Body.Close()
  	
  	dec := json.NewDecoder(response.Body)
  	type Delete struct{
		Success string
		Value string
		Error string
	}
	var ret Delete
	dec.Decode(&ret)
	fmt.Println(ret.Success + ":" + ret.Value + ":" + ret.Error)
}

func Get(key string) {
	response, err := http.Get(serverAddress + "/kv/get?key=" + key)
  	if err != nil {
    	fmt.Println("Post Get: ", err.Error())
    	return
  	} 
 	defer response.Body.Close()
  	
  	dec := json.NewDecoder(response.Body)
  	type Get struct{
		Success string
		Value string
		Error string
	}
	var ret Get
	dec.Decode(&ret)
	fmt.Println(ret.Success + ":" + ret.Value + ":" + ret.Error)
}

func Update(key,value string) {
	response, err := http.PostForm(serverAddress + "/kv/update", 
    	url.Values{"key": {key}, "value": {value}})
  	if err != nil {
    	fmt.Println("Post Update: ", err.Error())
    	return
  	} 
 	defer response.Body.Close()
  	
  	dec := json.NewDecoder(response.Body)
  	type Update struct{
		Success string
		Error string
	}
	var ret Update
	dec.Decode(&ret)
	fmt.Println(ret.Success + ":" + ret.Error)
}


func CountKey() {
	response, err := http.Get(serverAddress + "/kvman/countkey")
	if err != nil {
    	fmt.Println("Post Update: ", err.Error())
    	return
  	} 
	defer response.Body.Close()
  	dec := json.NewDecoder(response.Body)
  	type CountKey struct{
		Result string
	}
	var count CountKey
	dec.Decode(&count)
	fmt.Println("count key: " + count.Result)
}

func Dump() {
	response, err := http.Get(serverAddress + "/kvman/dump")
	if err != nil {
    	fmt.Println("Post Update: ", err.Error())
    	return
  	} 
	defer response.Body.Close()
  	dec := json.NewDecoder(response.Body)
	dumped_data := make(map[string]string)
	dec.Decode(&dumped_data)
	fmt.Println("Dumped data:")
	for key, value := range dumped_data {
    	fmt.Println("Key:", key, "    Value:", value)
	} 
}

func Shutdown() {
	response, err := http.Get(serverAddress + "/kvman/shutdown")
	if err != nil {
    	fmt.Println("Post Shutdown: ", err.Error())
    	return
  	} 
	response.Body.Close()
}

func StartServer(argument string) {
	cmd := exec.Command("python", "start_server", argument)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	fmt.Println(cmd.Start())
}

func StopServer(argument string) {
	cmd := exec.Command("python", "stop_server", argument)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	fmt.Println(cmd.Start())
}


func main() {
	StartServer("-b")
	StartServer("-p")
	time.Sleep(time.Second*3)

	Insert("1", "100")
	Insert("2", "200")
	Insert("3", "300")
	InsertFalse("aaaaaaaaaaaaaaaaa", "kkkkkkkkkkkkkkkk")
	Insert("298187&* ////\"\"=", "298187&* ////\"\"==========")
	CountKey()
	Insert("1", "100")
	CountKey()
	Delete("2")
	CountKey()
	Delete("4")
	CountKey()
	Get("1")
	Get("3")
	Get("298187&* ////\"\"=")
	Update("1", "100000000")
	Get("1")
	CountKey()
	Dump()
	
	for i := 1; i <= 20; i ++ {
		Insert(strconv.Itoa(i), strconv.Itoa(i*1000));
		//time.Sleep(time.Second)
	}
time.Sleep(time.Second)
	StopServer("-p")
	
	Insert("adfegaegae", "3dg34g3h")
	
	StartServer("-p")

	time.Sleep(time.Second)
	Insert("adfegaegae", "*****************")
	
time.Sleep(time.Second)	
	StopServer("-b")
	//time.Sleep(time.Second)
	Insert("adsewgeageaegaewgageagha", "*/%&&/%/%/%$$&**")
	Dump()
time.Sleep(time.Second)
	StopServer("-p")
	//time.Sleep(time.Second)

	/*
	for i := 1; i <=1000000; i ++ {
		Insert(strconv.Itoa(i), strconv.Itoa(i*1000));
	}

	Shutdown()*/

}
