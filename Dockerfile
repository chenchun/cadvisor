FROM golang:1.5.3

RUN go get github.com/tools/godep
WORKDIR /go/src/github.com/google/cadvisor
