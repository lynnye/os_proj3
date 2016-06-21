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
	"sort"
)

var server_address = "http://127.0.0.1:1234"
var backup_address = "http://127.0.0.2:1234"
var stop_chan = make(chan int)

const MODE = "test"

func PrintLog(condition string, log_content string) {
	if(condition == "debug") {
		fmt.Println(condition + " : " + log_content)
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

var numberInsert, numberInsertSuccess, numberGet int
var insertTotTime, getTotTime float64
var insertTime, getTime []float64
 
func Insert(key,value string) (string, string){

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
	err := cmd.Start()
	time.Sleep(100*time.Millisecond)
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
	err := cmd.Start()
	time.Sleep(100*time.Millisecond)
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

var N = 100
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
	
	Dump()
	
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

var dataLock map[string]*sync.Mutex = make(map[string]*sync.Mutex)
var dataLockLock sync.Mutex
func Fail() {
	fmt.Println("fail!")
	os.Exit(0)
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

func RandomTestFunction(wg *sync.WaitGroup) {
	
	defer wg.Done()
	for i := 1; i <= 100; i++ {
		x := rand.Intn(4)
	
		if x == 0 {//insert
			k := rand.Intn(100)
			elementLock := DataLockFind(strconv.Itoa(k)) 
			elementLock.Lock()
			err,mes := Insert(strconv.Itoa(k), strconv.Itoa(k))
			if err != "" {
				Fail()
			}
			
			globalLock.Lock()
			_, ok := localData[strconv.Itoa(k)]
			if ok == true && mes == "true" || ok == false && mes == "false" {
				fmt.Println(k)
				Fail()
			}
			if ok == false {
				localData[strconv.Itoa(k)] = strconv.Itoa(k)
			}
			globalLock.Unlock()
			
			elementLock.Unlock()
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
			globalLock.Unlock()
			
			elementLock := DataLockFind(k) 
			elementLock.Lock()
			
			err,mes,value := Delete(k)
			
			globalLock.Lock()
			res,ok := localData[k]
			if err != "" || ok == true && mes == "false" || ok == true && res != value || ok == false && mes == "true"{
				Fail()
			}
			delete(localData, k)
			
			globalLock.Unlock()
			elementLock.Unlock()
			
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
			globalLock.Unlock()
			
			elementLock := DataLockFind(k) 
			elementLock.Lock()
			
			err, mes := Update(k, val)
			
			globalLock.Lock()
			_, ok := localData[k]
			
			if err != "" || mes == "false" && ok == true || ok == false && mes == "true"{
				Fail()
			}
			if ok == true {
				localData[k] = val
			}
			globalLock.Unlock()
			elementLock.Unlock()
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
			globalLock.Unlock()
			
			elementLock := DataLockFind(k) 
			elementLock.Lock()
			err, mes, val := Get(k)
			globalLock.Lock()
			res, ok := localData[k]
			if err != "" || mes == "false" && ok == true || mes == "true" && ok == false || ok == true && res != val {
				Fail()
			}
			globalLock.Unlock()
			elementLock.Unlock()

		}
	}
}
func RandomTest() {
	time.Sleep(200 * time.Millisecond)
	Shutdown(server_address)
	Shutdown(backup_address)
	StartServer("-b")
	StartServer("-p")

	var M = 20
	var wg sync.WaitGroup
	localData = make(map[string]string)
	
	//startTime := time.Now()	
	for i := 1; i <= 5; i++ {
		for j := 1; j <= M; j++ {
			wg.Add(1)
			go RandomTestFunction(&wg)
		}
		wg.Wait()
		
		err,size,mes := CountKey()
		check := rand.Intn(N)
		
		if (err != "" || mes != "true" || size != strconv.Itoa(len(localData))) && check < checkCondition{
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
			time.Sleep(100*time.Millisecond) 
			StartServer("-b")
		} else {
			Shutdown(server_address)
			time.Sleep(100*time.Millisecond)
			StartServer("-p")
		} 
	}
	//endTime := time.Now()
	//fmt.Println(endTime.Sub(startTime))
	time.Sleep(100 * time.Millisecond)
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
	var wg sync.WaitGroup
	//startTime := time.Now()
	for i := 1; i <= 10; i++ {
		tmp := make([]rune, 100)
		for j := range(tmp) {
			tmp[j] = letters[rand.Intn(len(letters))]
		}
		b := string(tmp)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 1; i <= 1; i++ {
				k := 0
				if k == 0 {
					Insert(b, b)
				} else if k == 1 {
					Update(b, b)
				} else if k == 2 {
					Get(b)
				} else if k == 3 {
					Delete(b)
				}
			}	
		}()
	}
	wg.Wait()
	
	/*for i := 1; i <= 50; i++ {
		tmp := make([]rune, 2000000)
		for j := range(tmp) {
			tmp[j] = letters[rand.Intn(len(letters))]
		}
		b := string(tmp)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 1; i <= 1; i++ {
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
			}	
		}()
	}
	wg.Wait()*/
	//endTime := time.Now()
	//fmt.Println(endTime.Sub(startTime))
	time.Sleep(time.Second)
	Shutdown(server_address)
	Shutdown(backup_address)
}
 
 
func EncodingTest() {

	time.Sleep(time.Second)
	Shutdown(server_address)
	Shutdown(backup_address)
	StartServer("-b")
	StartServer("-p")
	
	key, value := "的的", "11" 
	
	response,_ := http.PostForm("http://" + server_address + "/kv/insert",
  	url.Values{"key": {key}, "value": {value}})
    	_, _, val := Get("的的")
    	io.Copy(ioutil.Discard, response.Body)
	response.Body.Close()
    	if val != "11" {
    		Fail()
    	}
    	time.Sleep(time.Second)
	Shutdown(server_address)
	Shutdown(backup_address)
}
 
 
func Success() {
	fmt.Println("Success!")
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

func TooManyFilesTest() {

	time.Sleep(time.Second)
	Shutdown(server_address)
	Shutdown(backup_address)
	StartServer("-b")
	StartServer("-p")
	var wg sync.WaitGroup
	for i := 1; i <= 1016; i++ {
		//wg.Add(1)
		//go func() {
		//	defer wg.Done()
			key, value := "111", "11" 
			http.PostForm("http://" + server_address + "/kv/insert",
		  	url.Values{"key": {key}, "value": {value}})
		//}()
		  
	}  	
	
	for i := 1; i <= 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			
			Insert("1", "1")
		}()
		  
	}  	
	wg.Wait()
	

	time.Sleep(time.Second)
	Shutdown(server_address)
	Shutdown(backup_address)
}

func main() {
	server_address, backup_address = DecodeConfig()
	rand.Seed(time.Now().UnixNano())  
	//LianlianTest()	
	BasicTest()
	ThroughputTest()
	//TooManyFilesTest()
	RandomTest()
	EncodingTest()
	Success()
	return	
}
