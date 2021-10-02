export GOARCH=amd64
export GOOS=linux
export CGO_ENABLED=0
make build
docker build -t prometheus-ahfuzhang:2.29.1 -f Dockerfile .
docker tag d0cc8c9e4816 csighub.tencentyun.com/ahfuzhang/prometheus:2.29.1
docker push csighub.tencentyun.com/ahfuzhang/prometheus:2.29.1
