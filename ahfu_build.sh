export GOARCH=amd64
export GOOS=linux
export CGO_ENABLED=0
make build
docker build -t prometheus-ahfuzhang:2.29.1 -f Dockerfile .
