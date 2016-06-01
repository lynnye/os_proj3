# os_proj3
proj3 for operating systems

This project implements an in-memory key value server named primary server, and its backup called backup server. More details can be found in **document**.

This project uses language golang 1.6.2 and python 2.7.6, please install compliers and explainers before use, The test enviroment is Ubuntu-64bit.

Test under following instructions:
1. ./compile.sh
2. go to the **bin** directory, run command: ./test 

Source files and executable files are put in the **bin** directory, the ip configure is put in the **conf** directory.

under directory **bin**
http_back.go:      the source code to implement a backup server
http_back:         the executable file of backup server
http_server.go:    the source code to implement a primary server
http_server:       the executable file of primary server
start_server:      the script to active server, with argument '-p' to start primary server, argument '-b' to start backup server
stop_server:       the script to shutdown server, with argument '-p' to stop primary server, argument '-b' to stop backup server
test.go:	   the source code of test
test:		   the executable file of test
http_client.go     just for test

under directory **conf**
settings.conf	   the configure file of ip and port of server

TestCases explain:
1. RandomTest: repeat 5 turns. Each turn, there are 20 clients where each requests 100 times . After each turn, countkey, dump and restart server will be tested. Each request result will be checked to ensure the correctness.
2. ThroughputTest: firstly, 1000 clients request in concurrecy with packages 30KB. Secondly, 50 clients request in concurrency with 2MB packages.
3. EncodingTest: to test whether Chinese character can be used to request.

Our program run average 20s in the Test.



