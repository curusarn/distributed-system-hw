
build:
	GOPATH=~/go go build main.go

dsv1:
	./main 10001 --init-cluster --ip 192.168.0.11 --join 192.168.0.12:10002

dsv1-noinit:
	./main 10001 --ip 192.168.0.11 --join 192.168.0.12:10002

dsv2:
	./main 10002 --ip 192.168.0.12 --join 192.168.0.11:10001

dsv3:
	./main 10003 --ip 192.168.0.13 --join 192.168.0.12:10002

dsv4:
	./main 10004 --ip 192.168.0.14 --join 192.168.0.12:10002

dsv5:
	./main 10005 --ip 192.168.0.15 --join 192.168.0.12:10002
