// Copyright 2021 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package remote

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"errors"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
)

type writeHandler struct {
	logger     log.Logger
	appendable storage.Appendable
}

// NewWriteHandler creates a http.Handler that accepts remote write requests and
// writes them to the provided appendable.
func NewWriteHandler(logger log.Logger, appendable storage.Appendable) http.Handler {
	return &writeHandler{
		logger:     logger,
		appendable: appendable,
	}
}

func (h *writeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if isMemoryFull {
		http.Error(w, "prometheus memory full", http.StatusFound)
		return
	}
	req, err := DecodeWriteRequest(r.Body)
	if err != nil {
		level.Error(h.logger).Log("msg", "Error decoding remote write request", "err", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = h.write(r.Context(), req)
	switch err {
	case nil:
	case storage.ErrOutOfOrderSample, storage.ErrOutOfBounds, storage.ErrDuplicateSampleForTimestamp:
		// Indicated an out of order sample is a bad request to prevent retries.
		level.Error(h.logger).Log("msg", "Out of order sample from remote write", "err", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	default:
		level.Error(h.logger).Log("msg", "Error appending remote write", "err", err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *writeHandler) write(ctx context.Context, req *prompb.WriteRequest) (err error) {
	app := h.appendable.Appender(ctx)
	defer func() {
		if err != nil {
			app.Rollback()
			return
		}
		err = app.Commit()
	}()

	for _, ts := range req.Timeseries {
		labels := labelProtosToLabels(ts.Labels)
		for _, s := range ts.Samples {
			_, err = app.Append(0, labels, s.Timestamp, s.Value)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

//check left memory, no OOM
func init() {
	const (
		memMaxStr = "--mem.max="
		memMinStr = "--mem.min="
	)
	for _, item := range os.Args {
		if strings.HasPrefix(item, memMaxStr) {
			n := ParseByteNumber(item[len(memMaxStr):])
			if n <= 0 {
				panic("cmd flags error:" + item)
			}
			maxMem = n
		} else if strings.HasPrefix(item, memMinStr) {
			n := ParseByteNumber(item[len(memMinStr):])
			if n <= 0 {
				panic("cmd flags error:" + item)
			}
			minMem = n
		}
	}
	if maxMem == 0 {
		maxMem = 32 * 1024 * 1024 * 1024
	}
	if minMem == 0 {
		minMem = 512 * 1024 * 1024
	}
	go checkMemory()
}

var (
	isMemoryFull       = false
	maxMem       int64 = 0
	minMem       int64 = 0
)

func checkMemory() {
	for {
		memUsed, err := GetRss()
		if err != nil {
			fmt.Println("GetRss() fail, err=", err.Error())
			time.Sleep(time.Duration(10) * time.Second)
			continue
		}
		isMemoryFull = maxMem-memUsed < minMem
		time.Sleep(time.Duration(10) * time.Second)
	}
}

//GetRss get rss memory
func GetRss() (int64, error) {
	buf, err := ioutil.ReadFile("/proc/self/statm")
	if err != nil {
		return 0, err
	}

	fields := strings.Split(string(buf), " ")
	if len(fields) < 2 {
		return 0, errors.New("cannot parse statm")
	}

	rss, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return 0, err
	}

	return rss * int64(os.Getpagesize()), err
}

var reOfByte = regexp.MustCompile(`(\d+)([GgMmKkBb]{0,1})`)

//ParseByteNumber
func ParseByteNumber(s string) int64 {
	arr := reOfByte.FindAllStringSubmatch(s, -1)
	if len(arr) < 1 || len(arr[0]) < 3 {
		return -1
	}
	n, err := strconv.Atoi(arr[0][1])
	if err != nil {
		return -2
	}
	if n <= 0 {
		return -3
	}
	switch arr[0][2] {
	case "G", "g":
		return int64(n) * (1024 * 1024 * 1024)
	case "M", "m":
		return int64(n) * (1024 * 1024)
	case "K", "k":
		return int64(n) * (1024)
	case "B", "b", "":
		return int64(n)
	default:
		return -4
	}
}
