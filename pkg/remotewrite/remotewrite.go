package remotewrite

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
)

var tsdb storage.Storage

var (
	//httpRequestCount = prometheus.NewCounterVec(
	//	prometheus.CounterOpts{
	//		Namespace:   "",
	//		Subsystem:   "",
	//		Name:        "http_request_total",
	//		Help:        "",
	//		ConstLabels: nil,
	//	},
	//	[]string{"podname", "podip"},
	//)
	httpRequestError = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   "",
		Subsystem:   "",
		Name:        "http_request_error",
		Help:        "",
		ConstLabels: nil,
	}, []string{"reason"})
	//dataPointTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	//	Namespace:   "",
	//	Subsystem:   "",
	//	Name:        "data_point_total",
	//	Help:        "",
	//	ConstLabels: nil,
	//}, []string{"podname", "podip"})
	//httpLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	//	Namespace:   "",
	//	Subsystem:   "",
	//	Name:        "http_latency",
	//	Help:        "",
	//	ConstLabels: nil,
	//	Buckets:     []float64{10, 20, 30, 40, 50, 100, 200, 300, 400, 500, 1000, 2000, 3000},
	//}, []string{"podname", "podip"})
	//podname = os.Getenv("POD_NAME")
	//podip   = os.Getenv("POD_IP")
	httpRequestCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "http_request_total",
		Help: "remote write http request total",
	})
	dataPointTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "data_point_total",
		Help: "remote write: total data point",
	})
	httpLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "http_latency",
		Buckets: []float64{10, 20, 30, 40, 50, 100, 200, 300, 400, 500, 1000, 2000, 3000},
	})
)

func init() {
	prometheus.MustRegister(httpRequestCount, httpRequestError, dataPointTotal, httpLatency)
}

//RunRemoteWriteSvr start a http server,
//  to support remote write protocol,
//  so than prometheus can use push mode
func RunRemoteWriteSvr(addr string, s storage.Storage) {
	log.SetFlags(log.LstdFlags|log.Lshortfile)
	tsdb = s
	http.HandleFunc("/api/v1/receive", receiveHandler)
	log.Fatal(http.ListenAndServe(addr, nil))
}

var pool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 1024*32)
	},
}

func receiveHandler2(w http.ResponseWriter, r *http.Request) {
	log.Println("")
	defer func(t time.Time) {
		httpLatency.Observe(float64(time.Since(t).Milliseconds()))
	}(time.Now())
	httpRequestCount.Inc()
	//defer r.Body.Close()
	var err error
	//
	reqData := pool.Get().([]byte)
	bodyLen := 0
	for {
		n, err := r.Body.Read(reqData[bodyLen:])
		if err == io.EOF {
			bodyLen += n
			break
		}
		if err != nil {
			w.WriteHeader(500)
			fmt.Fprintf(w, "http read error:%s", err.Error())
			httpRequestError.WithLabelValues("http_read").Inc()
			return
		}
		bodyLen += n
		if bodyLen == cap(reqData) {
			//数据超过32KB，要特殊处理
			temp := make([]byte, cap(reqData)*2)
			copy(temp, reqData)
			pool.Put(reqData)
			reqData = temp
		}
	}
	log.Println("")
	reqBody := reqData[:bodyLen]
	defer pool.Put(reqData)
	//
	dst := pool.Get().([]byte)
	dst = dst[0:0]
	defer pool.Put(dst) //bug: 导致很小空间的buf放回池，进一步导致上面的代码收包极慢
	dst, err = snappy.Decode(dst, reqBody)
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintf(w, "snappy decode error:%s", err.Error())
		httpRequestError.WithLabelValues("snappy_decode").Inc()
		return
	}
	log.Println("")
	//bufRaw, err := ioutil.ReadAll(r.Body)
	//if err != nil {
	//	w.WriteHeader(500)
	//	fmt.Fprintf(w, "http read error:%s", err.Error())
	//	httpRequestError.WithLabelValues("http_read").Inc()
	//	return
	//}
	//dst := make([]byte, 0, len(bufRaw)*5)
	//dst, err = snappy.Decode(dst, bufRaw)
	//if err != nil {
	//	w.WriteHeader(500)
	//	fmt.Fprintf(w, "snappy decode error:%s", err.Error())
	//	httpRequestError.WithLabelValues("snappy_decode").Inc()
	//	return
	//}
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
		httpRequestError.WithLabelValues("pb_decode").Inc()
		return
	}
	log.Println("")
	errCnt := 0
	//
	//sb := strings.Builder{}
	appender := tsdb.Appender(context.Background())
	for _, item := range st.Timeseries {
		labelsData := utilConvertLabels(item.Labels)
		for _, s := range item.Samples {
			_, err = appender.Append(0, labelsData,
				s.Timestamp, s.Value,
			)
			if err != nil {
				errCnt++
				//log.Println(err)
				httpRequestError.WithLabelValues("ts_append").Inc()
				//sb.WriteString(fmt.Sprintf("err=%s,ts=%s\n", err.Error(), item.String()))
				continue
			}
			dataPointTotal.Inc()
		}
	}
	log.Println("")
	err = appender.Commit()
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintf(w, "commit error:%s", err.Error())
		httpRequestError.WithLabelValues("commit").Inc()
		return
	}
	w.WriteHeader(200)
	if errCnt > 0 {
		fmt.Fprintf(w, "err count:%d\n", errCnt)
		//fmt.Fprint(w, sb.String())
	}
	log.Println("success")
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

func receiveHandler(w http.ResponseWriter, r *http.Request) {
	defer func(t time.Time) {
		httpLatency.Observe(float64(time.Since(t).Milliseconds()))
	}(time.Now())
	httpRequestCount.Inc()
	defer r.Body.Close()
	var err error
	//
	bufRaw, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintf(w, "http read error:%s", err.Error())
		httpRequestError.WithLabelValues("http_read").Inc()
		return
	}
	dst := make([]byte, 0, len(bufRaw)*5)
	//dstTemp := pool.Get().([]byte)
	//dst := dstTemp[0:0]
	//defer pool.Put(dstTemp)

	dst, err = snappy.Decode(dst, bufRaw)
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintf(w, "snappy decode error:%s", err.Error())
		httpRequestError.WithLabelValues("snappy_decode").Inc()
		return
	}
	st := &prompb.WriteRequest{}
	err = proto.Unmarshal(dst, st)
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintf(w, "pb decode error:%s", err.Error())
		httpRequestError.WithLabelValues("pb_decode").Inc()
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
				//log.Println(err)
				httpRequestError.WithLabelValues("ts_append").Inc()
				//sb.WriteString(fmt.Sprintf("err=%s,ts=%s\n", err.Error(), item.String()))
				continue
			}
			dataPointTotal.Inc()
		}
	}
	err = appender.Commit()
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintf(w, "commit error:%s", err.Error())
		httpRequestError.WithLabelValues("commit").Inc()
		return
	}
	w.WriteHeader(200)
	if errCnt > 0 {
		fmt.Fprintf(w, "err count:%d\n", errCnt)
		//fmt.Fprint(w, sb.String())
	}
}
