/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"time"

	"net/http"
	_ "net/http/pprof"

	"qpid.apache.org/amqp"
	"qpid.apache.org/electron"
)

func usage() {
	fmt.Fprintf(os.Stderr, "usage: %s (options) amqp://... \n", os.Args[0])
	fmt.Fprintf(os.Stderr, "options:\n")
	flag.PrintDefaults()
}

var hostnameTemplate = "hostname%03d"
var metricsTemplate = "metrics%03d"

type plugin struct {
	hostname *string
	name     string
	interval int
}

type host struct {
	name    string
	plugins []plugin
}

func (m *plugin) GetMetricMessage(nthSend int, msgInJSON int) (msg string) {
	msgBuffer := make([]byte, 0, 1024)

	msgBuffer = append(msgBuffer, "["...)
	for i := 0; i < msgInJSON; i++ {
		msgBuffer = append(msgBuffer, "{\"values\": ["...)
		msgBuffer = append(msgBuffer, strconv.FormatFloat(rand.Float64(), 'f', 4, 64)...)
		msgBuffer = append(msgBuffer, "], \"dstypes\": [\"derive\"], \"dsnames\": [\"samples\"],"...)
		msgBuffer = append(msgBuffer, "\"time\": "...)
		msgBuffer = append(msgBuffer, strconv.FormatFloat(float64((time.Now().UnixNano()))/1000000000, 'f', 4, 64)...)
		msgBuffer = append(msgBuffer, ", \"interval\": 10, \"host\": \""...)
		msgBuffer = append(msgBuffer, *m.hostname...)
		msgBuffer = append(msgBuffer, "\", \"plugin\": \""...)
		msgBuffer = append(msgBuffer, m.name...)
		msgBuffer = append(msgBuffer, "\",\"plugin_instance\": \"testInstance"...)
		msgBuffer = append(msgBuffer, strconv.Itoa(i)...)
		msgBuffer = append(msgBuffer, "\",\"type\": \"testType"...)
		msgBuffer = append(msgBuffer, "\",\"type_instance\": \"\"}"...)
		if i != msgInJSON-1 {
			msgBuffer = append(msgBuffer, ","...)
		}
	}
	msgBuffer = append(msgBuffer, "]"...)
	return string(msgBuffer)

}

func generateHosts(hostPrefix *string, hostsNum int, pluginNum int, intervalSec int) []host {

	hosts := make([]host, hostsNum)
	for i := 0; i < hostsNum; i++ {
		hosts[i].name = *hostPrefix + fmt.Sprintf(hostnameTemplate, i)
		hosts[i].plugins = make([]plugin, pluginNum)
		for j := 0; j < pluginNum; j++ {
			hosts[i].plugins[j].name =
				fmt.Sprintf(metricsTemplate, j)
			hosts[i].plugins[j].interval = intervalSec
			hosts[i].plugins[j].hostname = &hosts[i].name
		}
	}
	return hosts
}

func getMessagesLimit(urls string, metricsInAmqp int, enableCPUProfile bool) {
	dummyHost := "testHost"
	dummyPlugin := &plugin{
		hostname: &dummyHost,
		name:     "testPlugin",
		interval: 10,
	}

	container := electron.NewContainer(fmt.Sprintf("telemetry-bench%d", os.Getpid()))
	url, err := amqp.ParseURL(urls)
	if err != nil {
		log.Fatal(err)
		return
	}

	con, err := container.Dial("tcp", url.Host)
	if err != nil {
		log.Fatal(err)
		return
	}

	ackChan := make(chan electron.Outcome, 100)

	var waitb sync.WaitGroup
	startTime := time.Now()

	cancel := make(chan struct{})
	cancelMesg := make(chan struct{})
	// routine for sending mesg
	waitb.Add(1)
	countSent := 0
	go func() {
		addr := strings.TrimPrefix(url.Path, "/")
		s, err := con.Sender(electron.Target(addr), electron.AtMostOnce())
		if err != nil {
			log.Fatal(err)
		}
		for {
			text := dummyPlugin.GetMetricMessage(countSent, metricsInAmqp)
			msg := amqp.NewMessage()
			body := amqp.Binary(text)
			msg.Marshal(body)
			s.SendAsync(msg, nil, body)
			countSent = countSent + 1

			select {
			case <-cancelMesg:
				waitb.Done()
				return
			default:
			}
		}
	}()

	// routine for waiting ack....
	waitb.Add(1)
	go func() {
		for {
			select {
			case out := <-ackChan:
				if out.Error != nil {
					log.Fatalf("acknowledgement %v error: %v",
						out.Value, out.Error)
				} else if out.Status != electron.Accepted {
					log.Printf("acknowledgement unexpected status: %v", out.Status)
				}
			case <-cancel:
				waitb.Done()
				return
			}
		}
	}()
	fmt.Printf("sending AMQP in 10 seconds...")
	time.Sleep(10 * time.Second)

	fmt.Printf("Done!\n")
	finishedTime := time.Now()
	duration := finishedTime.Sub(startTime)
	fmt.Printf("Total: %d sent (duration:%v, mesg/sec: %v)\n", countSent, duration, float64(countSent)/duration.Seconds())
	if enableCPUProfile {
		pprof.StopCPUProfile()
	}
	os.Exit(0)
	/*
		close(cancelMesg)
		close(cancel)
		waitb.Wait()
		con.Close(nil)
	*/
}

func main() {
	// parse command line option
	hostsNum := flag.Int("hosts", 1, "Number of hosts to simulate")
	metricsNum := flag.Int("metrics", 1, "Metrics per AMQP messages")
	prefixString := flag.String("hostprefix", "", "Host prefix")
	pluginNum := flag.Int("plugins", 1, "Plugins per interval, each plugin generates \"metrics\" (1 default) per interval")
	intervalSec := flag.Int("interval", 1, "Interval (sec)")
	metricMaxSend := flag.Int("send", 1, "How many metrics sent")
	showTimePerMessages := flag.Int("timepermesgs", -1, "Show time for each given messages")
	pprofileFileName := flag.String("pprofile", "", "go pprofile output")
	modeString := flag.String("mode", "simulate", "Mode (simulate/limit)")

	flag.Usage = usage
	flag.Parse()

	urls := flag.Args()
	if len(urls) == 0 {
		fmt.Fprintln(os.Stderr, "amqp URL is missing")
		usage()
		os.Exit(1)
	} else if len(urls) > 1 {
		fmt.Fprintln(os.Stderr, "Only one amqp URL is supported")
		usage()
		os.Exit(1)
	}

	if *pprofileFileName != "" {
		f, err := os.Create(*pprofileFileName)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	} else {
		go func() {
			log.Println(http.ListenAndServe("localhost:6060", nil))
		}()
	}

	rand.Seed(time.Now().UnixNano())
	hosts := generateHosts(prefixString, *hostsNum, *pluginNum, *intervalSec)

	if *modeString == "limit" {
		getMessagesLimit(urls[0], *metricsNum, *pprofileFileName != "")
		return
	} else if *modeString != "simulate" {
		fmt.Fprintf(os.Stderr, "Invalid mode string (simulate/limit): %s", *modeString)
		return
	}

	container := electron.NewContainer(fmt.Sprintf("telemetry-bench%d", os.Getpid()))
	url, err := amqp.ParseURL(urls[0])
	if err != nil {
		log.Fatal(err)
		return
	}

	con, err := container.Dial("tcp", url.Host)
	if err != nil {
		log.Fatal(err)
		return
	}

	ackChan := make(chan electron.Outcome, 100)
	mesgChan := make(chan string, 100)

	var wait sync.WaitGroup
	var waitb sync.WaitGroup
	startTime := time.Now()
	for _, v := range hosts {
		wait.Add(1)
		go func(m host) {
			defer wait.Done()
			for i := 0; ; i++ {
				if i >= *metricMaxSend &&
					*metricMaxSend != -1 {
					break
				}
				for _, w := range m.plugins {
					// uncomment if need to rondom wait
					/*
						time.Sleep(time.Millisecond *
							time.Duration(rand.Int()%1000))
					*/
					mesgChan <- w.GetMetricMessage(i, *metricsNum)
				}
				time.Sleep(time.Duration(*intervalSec) * time.Second)
			}
		}(v)
	}
	cancel := make(chan struct{})
	cancelMesg := make(chan struct{})
	// routine for sending mesg
	waitb.Add(1)
	countSent := 0
	countAck := 0
	go func() {
		lastCounted := time.Now()
		addr := strings.TrimPrefix(url.Path, "/")
		s, err := con.Sender(electron.Target(addr), electron.AtMostOnce())
		if err != nil {
			log.Fatal(err)
		}
		for {
			select {
			case text := <-mesgChan:
				//fmt.Printf("%s\n", text)
				msg := amqp.NewMessage()
				body := amqp.Binary(text)
				msg.Marshal(body)
				s.SendAsync(msg, ackChan, countSent)
				countSent = countSent + 1
				if countSent%(*showTimePerMessages+1) == 0 {
					lastCounted = time.Now()
				}
				if *showTimePerMessages != -1 && countSent%*showTimePerMessages == 0 {
					fmt.Printf("Sent: %d sent, %d ack'd, (%v)\n", countSent, countAck, time.Now().Sub(lastCounted))
				}

			case <-cancelMesg:
				waitb.Done()
				return
			}
		}
	}()

	// routine for waiting ack....
	waitb.Add(1)
	go func() {
		for {
			select {
			case out := <-ackChan:
				if out.Error != nil {
					log.Fatalf("acknowledgement %v error: %v",
						out.Value, out.Error)
				} else if out.Status != electron.Accepted {
					log.Printf("acknowledgement unexpected status: %v", out.Status)
				} else {
					countAck = countAck + 1
				}
			case <-cancel:
				waitb.Done()
				return
			}
		}
	}()

	wait.Wait()
	close(cancelMesg)
	close(cancel)
	waitb.Wait()
	con.Close(nil)
	finishedTime := time.Now()
	duration := finishedTime.Sub(startTime)
	fmt.Printf("Total: %d sent (duration:%v, mesg/sec: %v, metric/sec: %v)\n",
		countSent, duration, float64(countSent)/duration.Seconds(), float64(countSent**metricsNum)/duration.Seconds())
}
