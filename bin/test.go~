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
	"sync"
	"math/rand"
)

var server_address = "http://127.0.0.1:1234"
var backup_address = "http://127.0.0.2:1234"
var stop_chan = make(chan int)

const MODE = ""

func PrintLog(condition string, log_content string) {
	if(condition == "debug") {
		fmt.Println("In primary server: " + log_content)
	}
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

type BackupResponse struct{
	Success string
}

func Insert(key,value string) (string, string){
	response, err := http.PostForm("http://" + server_address + "/kv/insert", 
    	url.Values{"key": {key}, "value": {value}})
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
	return "", ret.Success	
}

func InsertFalse(key,value string) {
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

func Delete(key string) (string, string, string){
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

func Get(key string) (string,string,string){
	response, err := http.Get("http://" + server_address + "/kv/get?key=" + key)
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

func Update(key,value string) (string,string){
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


func CountKey() (string, string, string){
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
		Error string
	}
	var count CountKey
	dec.Decode(&count)
	if MODE == "debug" {
		fmt.Println("count key: " + count.Result)
	}
	io.Copy(ioutil.Discard, response.Body)
	response.Body.Close()
	return "",count.Result,count.Error
}

func Dump() (string, map[string]string){
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

func StopServer(argument string) string{
	cmd := exec.Command("./stop_server", argument)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil {
		PrintLog(MODE, err.Error())
		return "error"
	}
	return ""
}

func ConcurrentTest() {
	for i := 1; i <=1000; i ++ {
		go Get(strconv.Itoa(i))
		go Update(strconv.Itoa(i), "updated")
		go Delete(strconv.Itoa(i))
	}
}

func LianlianTest() {
	
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
		Insert(strconv.Itoa(i), strconv.Itoa(i*1000))
	}

	time.Sleep(time.Second)
	Shutdown(server_address)

	Insert("adfegaegae", "3dg34g3h")
	
	StartServer("-p")

	Insert("adfegaegae", "*****************")
	
	time.Sleep(time.Second)	
	StopServer("-b")

	Insert("adsewgeageaegaewgageagha", "*/%&&/%/%/%$$&**")
	Dump()

	StartServer("-b")
	
	for i := 1; i <=1000000; i ++ {
		Insert(strconv.Itoa(i), strconv.Itoa(i*1000));
	}

	ConcurrentTest()
}

func BasicTest() {
	time.Sleep(time.Second)
	Shutdown(server_address)
	Shutdown(backup_address)
	StartServer("-b")
	StartServer("-p")
	for i := 1; i <= 10; i++ {
		Insert(strconv.Itoa(i), strconv.Itoa(i * 100))
	}
	
	for i := 1; i <= 10; i++ {
		go Get(strconv.Itoa(i))
	}
	
	time.Sleep(time.Second)
	StopServer("-b")
	time.Sleep(time.Second)
	StartServer("-b")
	time.Sleep(time.Second)
		
	time.Sleep(time.Second)
	Shutdown(server_address)
	Shutdown(backup_address)
}

var localData map[string]string
var globalLock sync.Mutex

func Fail() {
	fmt.Println("fail!")
	os.Exit(0)
}

func RandomTestFunction(wg *sync.WaitGroup) {
	rand.Seed(time.Now().UnixNano())  
	defer wg.Done()
	for i := 1; i <= 5; i++ {
		x := rand.Intn(4)
	
		if x == 0 {//insert
			k := rand.Intn(10)
			globalLock.Lock()
			err,mes := Insert(strconv.Itoa(k), strconv.Itoa(k))
			if err != "" {
				Fail()
			}
			_, ok := localData[strconv.Itoa(k)]
			if ok == true && mes == "true" || ok == false && mes == "false" {
				Fail()
			}
			if ok == false {
				localData[strconv.Itoa(k)] = strconv.Itoa(k)
			}
			globalLock.Unlock()
		} else if x == 1 {//delete
			
			globalLock.Lock()
			if len(localData) == 0 {
				globalLock.Unlock()
				continue
			}
			var k string
			for k = range(localData) {
				break
			}
			err,mes,value := Delete(k)
			if err != ""  || mes == "false" || value != localData[k] {
				Fail()
			}
			delete(localData, k)
			globalLock.Unlock()
		} else if x == 2 {//update
			
			
			val := strconv.Itoa(rand.Intn(100000))
			globalLock.Lock()
			if len(localData) == 0 {
				
				globalLock.Unlock()
				continue
			}
			var k string
			for k = range(localData) {
				break
			}
			err, mes := Update(k, val)
			localData[k] = val
			if err != "" || mes == "false" {
				Fail()
			}
			globalLock.Unlock()
		} else if x == 3 {//get
			
			globalLock.Lock()
			if len(localData) == 0 {
				
				globalLock.Unlock()
				continue
			}
			var k string
			for k = range(localData) {
				break
			}
			err, mes, val := Get(k)
			if err != "" || mes == "false" || val != localData[k] {
				Fail()
			}
			globalLock.Unlock()
		}
	}
}
func RandomTest() {
	time.Sleep(time.Second)
	Shutdown(server_address)
	Shutdown(backup_address)
	StartServer("-b")
	StartServer("-p")

 	var wg sync.WaitGroup
	localData = make(map[string]string)	
	for i := 1; i <= 5; i++ {
		for j := 1; j <= 5; j++ {
			wg.Add(1)
			go RandomTestFunction(&wg)
		}
		wg.Wait()
		
		err,size,mes := CountKey()
		if err != "" || mes != "true" || size != strconv.Itoa(len(localData) ) {
			Fail()
		}
		
		err,dumped_data := Dump()
		
		for key,val := range dumped_data {
			if err != "" || localData[key] != val {
				fmt.Println(err)
				Fail()
			}
		}
		
		k := rand.Intn(2)
		if k == 0 {
			StopServer("-b")
			time.Sleep(time.Second)
			StartServer("-b")
		} else {
			Shutdown(server_address)
			time.Sleep(time.Second)
			StartServer("-p")
		} 
		
	}
	
	time.Sleep(time.Second)
	Shutdown(server_address)
	Shutdown(backup_address)
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
func ThroughputTest(){

	time.Sleep(time.Second)
	Shutdown(server_address)
	Shutdown(backup_address)
	StartServer("-b")
	StartServer("-p")
	//startTime := time.Now()
	for i := 1; i <= 1000; i++ {
		tmp := make([]rune, 32000)
		for j := range(tmp) {
			tmp[j] = letters[rand.Intn(len(letters))]
		}
		b := string(tmp)
		go func() {
			k := rand.Intn(4)
			if k == 0 {
				Insert(b, b)
			} else if k == 1 {
				Update(b, b)
			} else if k == 2 {
				Get(b)
			} else if k == 3 {
				Delete(b)
			}	
		}()
	}
	//endTime := time.Now()
	//fmt.Println(endTime.Sub(startTime))
	time.Sleep(time.Second)
	Shutdown(server_address)
	Shutdown(backup_address)
}
 
func main() {
	server_address, backup_address = DecodeConfig()
	
	//LianlianTest()	
	//BasicTest()
	//ThroughputTest()
	RandomTest()
	return	
}
