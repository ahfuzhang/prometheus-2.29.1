package remotewrite

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
)

var tsdb storage.Storage

//RunRemoteWriteSvr start a http server,
//  to support remote write protocol,
//  so than prometheus can use push mode
func RunRemoteWriteSvr(s storage.Storage) {
	tsdb = s
	http.HandleFunc("/api/v1/receive", receiveHandler)
	log.Fatal(http.ListenAndServe(":9089", nil))
}

func receiveHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var err error
	bufRaw,_ := ioutil.ReadAll(r.Body)
	dst := make([]byte,0, len(bufRaw)*3)
	dst,err = snappy.Decode(dst, bufRaw)
	if err!=nil{
		w.WriteHeader(500)
		fmt.Fprintf(w, "snappy decode error:%s", err.Error())
		return
	}
	//reader := snappy.NewReader(r.Body)
	//buf, err := ioutil.ReadAll(reader)
	//if err != nil {
	//	w.WriteHeader(500)
	//	fmt.Fprintf(w, "read error:%s", err.Error())
	//	return
	//}
	st := &prompb.WriteRequest{}
	err = proto.Unmarshal(dst, st)
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintf(w, "pb decode error:%s", err.Error())
		return
	}
	errCnt := 0
	//
	appender := tsdb.Appender(context.Background())
	for _, item := range st.Timeseries {
		labelsData := utilConvertLabels(item.Labels)
		for _, s := range item.Samples {
			_, err = appender.Append(0, labelsData,
				s.Timestamp, s.Value,
			)
			if err != nil {
				errCnt++
				log.Println(err)
			}
		}
	}
	err=appender.Commit()
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintf(w, "commit error:%s", err.Error())
		return
	}
	w.WriteHeader(200)
	fmt.Fprintf(w, "err count:%d", errCnt)
}

func utilConvertLabels(l []prompb.Label) labels.Labels {
	out := make([]labels.Label, 0, len(l))
	for _, item := range l {
		out = append(out, labels.Label{
			Name:  item.Name,
			Value: item.Value,
		})
	}
	return labels.Labels(out)
}
