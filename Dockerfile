FROM golang:latest

WORKDIR /app

COPY ./ /app

RUN mkdir -p /res
RUN go mod download
RUN go get github.com/githubnemo/CompileDaemon

WORKDIR /app/cmd/parserSms/
ENTRYPOINT CompileDaemon --build="go build main.go" --command=./main