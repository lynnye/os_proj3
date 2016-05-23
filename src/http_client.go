package main 
import (
	"net/http"
	"net/url"
	"fmt"
	"io/ioutil"
)

const serverAddress = "http://127.0.0.1:1234"

func Insert(key,value string) {
	//fmt.Println(key, value)
	resp, err := http.PostForm(serverAddress + "/kv/insert", 
    	url.Values{"key": {key}, "value": {value}})
  	if err != nil {
    	fmt.Println("Post Insert: ", err.Error())
    	return
  	} 
 	defer resp.Body.Close()
  	ioutil.ReadAll(resp.Body)
}
func main() {
	Insert("1", "100")
	Insert("2", "200")
	Insert("3", "300")
}