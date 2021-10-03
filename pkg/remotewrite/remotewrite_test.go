package remotewrite

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
)

func Test_utilConvertLabels(t *testing.T) {

}

func Test_RemoteWriteClient(t *testing.T){
	st := &prompb.WriteRequest{
		Timeseries:           []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{
						Name: "__name__",
						Value: "ahfu_metric_1",
					},
					{
						Name: "job",
						Value: "job1",
					},
					{
						Name: "instance",
						Value: "container111",
					},
				},
				Samples: []prompb.Sample{
					{
						Value: 123.456,
						Timestamp: time.Now().UnixNano()/1000000,
					},
				},
			},
		},
		Metadata:             nil,
	}
	buf,_ := proto.Marshal(st)
	dst := make([]byte, 0, len(buf))
	dst = snappy.Encode(dst, buf)
	//http://11.135.205.116/
	//
	url := "http://127.0.0.1:9089/api/v1/receive"
	url = "http://11.135.205.116/api/v1/receive"
	req,_ := http.NewRequest("POST",url, bytes.NewReader(dst))
	rsp, err := http.DefaultClient.Do(req)
	if err!=nil{
		t.Errorf(err.Error())
		return
	}
	buf, err = ioutil.ReadAll(rsp.Body)
	if err!=nil{
		t.Errorf(err.Error())
		return
	}
	t.Logf("%s", string(buf))
}
