package main 
import (
	"net/http"
	"net/url"
	"fmt"
	"io/ioutil"
	"encoding/json"
	"strconv"
	"time"
	"os/exec"
	"os"
	"log"
	"strings"
)

var server_address = "http://127.0.0.1:1234"
var backup_address = "http://127.0.0.2:1234"
var stop_chan = make(chan int)

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

type BackupResponse struct{
	Success string
}

func Insert(key,value string) {
	response, err := http.PostForm(server_address + "/kv/insert", 
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
	response, err := http.PostForm(server_address + "/kv/insert", 
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
	response, err := http.PostForm(server_address + "/kv/delete", 
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
	response, err := http.Get(server_address + "/kv/get?key=" + key)
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
	response, err := http.PostForm(server_address + "/kv/update", 
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
	response, err := http.Get(server_address + "/kvman/countkey")
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
	response, err := http.Get(server_address + "/kvman/dump")
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

func Shutdown(address string) {
	response, err := http.Get(address + "/kvman/shutdown")
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
	fmt.Println(cmd.Run())
	stop_chan <- 0
}


func main() {
	server_address, backup_address = DecodeConfig()
	server_address, backup_address = "http://" + server_address, "http://" + backup_address
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
	//go StopServer("-p")
	//_ = <- stop_chan
	//time.Sleep(time.Second)
	Shutdown(server_address)

	Insert("adfegaegae", "3dg34g3h")
	
	StartServer("-p")

	time.Sleep(time.Second)
	Insert("adfegaegae", "*****************")
	
	time.Sleep(time.Second)	
	go StopServer("-b")
	return_chan := <- stop_chan
	fmt.Println(return_chan)
	//time.Sleep(time.Second)

	Insert("adsewgeageaegaewgageagha", "*/%&&/%/%/%$$&**")
	Dump()

	time.Sleep(time.Second)
	go StopServer("-p")
	return_chan = <- stop_chan
	fmt.Println(return_chan)
	//time.Sleep(time.Second)

	StartServer("-b")
	StartServer("-p")
	time.Sleep(time.Second*3)
	
	for i := 1; i <=1000000; i ++ {
		Insert(strconv.Itoa(i), strconv.Itoa(i*1000));
	}

	//Shutdown()

}
