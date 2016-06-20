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
	"io"
	//"sync"
	"math/rand"
	"sort"
)

const MODE = "debug"

var servers []string

func PrintLog(condition string, log_content string) {
	if(condition == "debug") {
		fmt.Println(condition + " : " + log_content)
	}
}

var numberInsert, numberInsertSuccess, numberGet int
var insertTotTime, getTotTime float64
var insertTime, getTime []float64
 
func Insert(key,value,server_address string) (string, string){

	//PrintLog("debug", "insert " + key + " " + value + " " + server_address)
	numberInsert ++
	
	startTime := time.Now()
	response, err := http.PostForm("http://" + server_address + "/kv/insert", 
    	url.Values{"key": {key}, "value": {value}})
    	endTime := time.Now()
    	duration := endTime.Sub(startTime).Seconds()
 	
 	insertTotTime += duration
	insertTime = append(insertTime, duration)

  	if err != nil {
  	if MODE == "debug" {
    		fmt.Println("Post Insert: ", err.Error())
    	}
    	return "error", ""
  	} 
  	
  	dec := json.NewDecoder(response.Body)
  	type Insert struct{
		Success string
		Error string
	}
	var ret Insert
	dec.Decode(&ret)
	if MODE == "debug" {
		fmt.Println(ret.Success + ":" + ret.Error)
    	}
	
	io.Copy(ioutil.Discard, response.Body)
	response.Body.Close()
	if ret.Success == "true" {
		numberInsertSuccess ++
	}
	return "", ret.Success	
}

func InsertFalse(key,value,server_address string) {
	response, err := http.PostForm("http://" + server_address + "/kv/insert", 
    	url.Values{"keyfalse": {key}, "value": {value}})
  	if err != nil {
  	if MODE == "debug" {
    		fmt.Println("Post Insert: ", err.Error())
    	}
    	return
  	} 
  	
  	dec := json.NewDecoder(response.Body)
  	type Insert struct{
		Success string
		Error string
	}
	var ret Insert
	dec.Decode(&ret)
	fmt.Println(ret.Success + ":" + ret.Error)

	io.Copy(ioutil.Discard, response.Body)
	response.Body.Close()
}

func Delete(key,server_address string) (string, string, string){
	response, err := http.PostForm("http://" + server_address + "/kv/delete", 
    	url.Values{"key": {key}})
  	if err != nil {
    	if MODE == "debug" {
    		fmt.Println("Post Delete: ", err.Error())
    	}
    	return "error","","" 
  	}
  	
  	dec := json.NewDecoder(response.Body)
  	type Delete struct{
		Success string
		Value string
		Error string
	}
	var ret Delete
	dec.Decode(&ret)
	if MODE == "debug" {
		fmt.Println(ret.Success + ":" + ret.Value + ":" + ret.Error)
	}
	io.Copy(ioutil.Discard, response.Body)
	response.Body.Close()
	return "",ret.Success,ret.Value
}

func Get(key,server_address string) (string,string,string){
	numberGet ++
	
	startTime := time.Now()
	response, err := http.Get("http://" + server_address + "/kv/get?key=" + key)
	endTime := time.Now()
    	duration := endTime.Sub(startTime).Seconds()
 	
 	getTotTime += duration
	getTime = append(getTime, duration)
	
  	if err != nil {
  	if MODE == "debug" {
  	  	fmt.Println("Post Get: ", err.Error())
    	}
    	return "error", "",""
  	}
 	
  	dec := json.NewDecoder(response.Body)
  	type Get struct{
		Success string
		Value string
		Error string
	}
	var ret Get
	dec.Decode(&ret)
//	fmt.Println(ret.Success + ":" + ret.Value + ":" + ret.Error)

	io.Copy(ioutil.Discard, response.Body)
	response.Body.Close()
	
	return "", ret.Success,ret.Value
}

var checkCondition = 13
func Update(key,value,server_address string) (string,string){
	response, err := http.PostForm("http://" + server_address + "/kv/update", 
    	url.Values{"key": {key}, "value": {value}})
  	if err != nil {
    	if MODE == "debug" {
    		fmt.Println("Post Update: ", err.Error())
    	}
    	return "error",""
  	} 
  	
  	dec := json.NewDecoder(response.Body)
  	type Update struct{
		Success string
		Error string
	}
	var ret Update
	dec.Decode(&ret)
	if MODE == "debug" {
		fmt.Println(ret.Success + ":" + ret.Error)
	}
	io.Copy(ioutil.Discard, response.Body)
	response.Body.Close()
	return "",ret.Success
}

func CountKey(server_address string) (string, string, string){
	response, err := http.Get("http://" + server_address + "/kvman/countkey")
	if err != nil {
    	if MODE == "debug" {
    		fmt.Println("Post Update: ", err.Error())
    	}
    	return "error", "",""
  	} 

  	dec := json.NewDecoder(response.Body)
  	type CountKey struct{
		Result string
		Mes string
	}
	var count CountKey
	dec.Decode(&count)
	if MODE == "debug" {
		fmt.Println("count key: " + count.Result)
	}
	io.Copy(ioutil.Discard, response.Body)
	response.Body.Close()
	return "",count.Result,count.Mes
}

func Dump(server_address string) (string, map[string]string){
	response, err := http.Get("http://" + server_address + "/kvman/dump")
	
	dumped_data := make(map[string]string)
	if err != nil {
    	if MODE == "debug" {
    		fmt.Println("Post Update: ", err.Error())
    	}
    	return "error", dumped_data
  	} 

  	dec := json.NewDecoder(response.Body)
	dec.Decode(&dumped_data)
	if MODE == "debug" {
    
		fmt.Println("Dumped data:")
		for key, value := range dumped_data {
    			fmt.Println("Key:", key, "    Value:", value)
    		}
	} 

	io.Copy(ioutil.Discard, response.Body)
	response.Body.Close()
	return "", dumped_data
}

func Shutdown(address string) string{
	response, err := http.Get("http://" + address + "/kvman/shutdown")
	if err != nil {
    	if MODE == "debug" {
    		fmt.Println("Post Shutdown: ", err.Error())
    	}
    	return "error"
  	} 
  	io.Copy(ioutil.Discard, response.Body) 
	response.Body.Close()
	return ""
}

func StartServer(argument string) string{
	
	//PrintLog("debug", "test")
	cmd := exec.Command("./start_server", argument)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil {
		PrintLog(MODE, err.Error())
		return "error"
	}
	return ""
}

func StopServer(address string) string{
	response, err := http.Get("http://" + address + "/kvman/shutdown")
	if err != nil {
    	if MODE == "debug" {
    		fmt.Println("Post Shutdown: ", err.Error())
    	}
    	return "error"
  	} 
  	io.Copy(ioutil.Discard, response.Body) 
	response.Body.Close()
	return ""
}

func KillAll() string {
	cmd := exec.Command("killall", "server")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil {
		PrintLog(MODE, err.Error())
		return "error"
	}
	return ""
}

func DecodeConfig() (serverPeers []string) {
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
	
	port := config["port"].(string)
	 
	for i := 0; i < M; i++ {
		serverPeers[i] = config["n"+strconv.Itoa((i+1)/10)+strconv.Itoa((i+1)%10)].(string)+ ":" + port
	}
	return serverPeers
}

func Equal(a,b map[string]string) bool {
	if(len(a) != len(b)) {
		fmt.Println("Map with different size:", len(a), len(b))
		return false
	}
	for key, value := range(a) {
		value2, ok := b[key]
		if(!ok) {
			fmt.Println("key " + key + " not exists")
			return false
		}
		if(value2 != value) {
			fmt.Println("value not equal:" + value + " " + value2)
			return false
		}
	}
	return true
}

func BasicTest() {
	Insert("1", "1", servers[0])
	Insert("11", "11", servers[0])
	Insert("2", "2", servers[1])
	Insert("3", "3", servers[2])
	Delete("2", servers[2])
	Update("11", "111", servers[1])
	Get("3", servers[0])
	for i := 1; i <= 100; i++ {
		operation_type := rand.Intn(5)
		server_id := rand.Intn(3)
		if operation_type == 0 {//insert
			key := strconv.Itoa(rand.Intn(100))
			value := strconv.Itoa(rand.Intn(100))
			Insert(key, value, servers[server_id])
		}
		if operation_type == 1 {//delete
			key := strconv.Itoa(rand.Intn(100))
			Delete(key, servers[server_id])
		}
		if operation_type == 2 {//update
			key := strconv.Itoa(rand.Intn(100))
			value := strconv.Itoa(rand.Intn(100))
			Update(key, value, servers[server_id])
		}
		if operation_type == 3 {//get
			key := strconv.Itoa(rand.Intn(100))
			Get(key, servers[server_id])
		}
		if operation_type == 4 {//dump
			_, data_0 := Dump(servers[0])	
			_, data_1 := Dump(servers[1])
			_, data_2 := Dump(servers[2])
			if(!Equal(data_0, data_1) || !Equal(data_1,data_2)) {
				TestFail("BasicTest")
			}
		}
	}


}

func TestFail(place string) {
	fmt.Println("Failed in " + place + "!")
	os.Exit(0)
}

func  TestSucceed() {
	fmt.Println("Succeed!")
	fmt.Printf("Number of Insertions Succeeded/Total Number of Insertions %d/%d\n", 
	numberInsertSuccess, numberInsert)
	if numberGet == 0 {
		numberGet ++
		getTime = append(getTime, 0)
	}
	if numberInsert == 0 {
		numberInsert ++
		insertTime = append(insertTime, 0)
	}
	fmt.Printf("Average Insert Time/Average Get Time %f/%f\n", 
	insertTotTime / float64(numberInsert), getTotTime / float64(numberGet))
	sort.Float64s(insertTime)
	sort.Float64s(getTime)
	
	fmt.Printf("20th insertTime/20th getTime: %f/%f\n", 
	insertTime[int(0.2*float64(len(insertTime)))], getTime[int(0.2*float64(len(getTime)))])
	
	fmt.Printf("50th insertTime/50th getTime: %f/%f\n", 
	insertTime[int(0.5*float64(len(insertTime)))], getTime[int(0.5*float64(len(getTime)))])
	
	fmt.Printf("70th insertTime/70th getTime: %f/%f\n", 
	insertTime[int(0.7*float64(len(insertTime)))], getTime[int(0.7*float64(len(getTime)))])
	
	fmt.Printf("90th insertTime/90th getTime: %f/%f\n", 
	insertTime[int(0.9*float64(len(insertTime)))], getTime[int(0.9*float64(len(getTime)))])
}

func main() {
	rand.Seed(time.Now().UnixNano())  
	
	StartServer("1")
	StartServer("2")
	StartServer("3")
	
	servers = DecodeConfig()
	fmt.Println(servers)

	BasicTest()	
	KillAll()
	TestSucceed()
	
	return	
}
