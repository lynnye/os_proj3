package main 
import (
	"net/http"
	"net/url"
	"fmt"
	//"io/ioutil"
	"encoding/json"
)

const serverAddress = "http://127.0.0.1:1234"

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
	response, err := http.PostForm(serverAddress + "/kv/get", 
    	url.Values{"key": {key}})
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
	response, _ := http.Get(serverAddress + "/kvman/countkey")
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
	response, _ := http.Get(serverAddress + "/kvman/dump")
	defer response.Body.Close()
  	dec := json.NewDecoder(response.Body)
	dumped_data := make(map[string]string)
	dec.Decode(&dumped_data)
	fmt.Println("Dumped data:")
	for key, value := range dumped_data {
    	fmt.Println("Key:", key, "    Value:", value)
	} 
}

func main() {
	Insert("1", "100")
	Insert("2", "200")
	Insert("3", "300")
	CountKey()
	Insert("1", "100")
	CountKey()
	Delete("2")
	CountKey()
	Delete("4")
	CountKey()
	Get("1")
	Get("3")
	Update("1", "100000000")
	Get("1")
	CountKey()
	Dump()
}