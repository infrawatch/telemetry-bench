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
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"os"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"time"

	"net/http"
	_ "net/http/pprof"

	"pack.ag/amqp"
)

func usage() {
	fmt.Fprintf(os.Stderr, "usage: %s (options) amqp://... \n", os.Args[0])
	fmt.Fprintf(os.Stderr, "options:\n")
	flag.PrintDefaults()
}

var (
	sleepFunc        = func() {} // Default no debugging output
	startTime        = time.Now()
	hostnameTemplate = "hostname%03d"
	metricsTemplate  = "metrics%03d"
)

type pluginFunc = func() string

//[{"values":[11035,219350],"dstypes":["derive","derive"],"dsnames":["read","write"],"time":1536615315.346,"interval":5.000,"host":"nfvha-compute1-lab-node","plugin":"virt","plugin_instance":"instance-0000002c","type":"disk_ops","type_instance":"vda"}]

type plugin struct {
	name           string
	hostname       *string
	interval       int
	values         []pluginFunc
	dstypes        []string
	dsnames        []string
	mtype          []string
	typeInstance   []string
	pluginInstance []string
}

type host struct {
	name    string
	plugins []plugin
}

func (m *plugin) GetMetricMessage() (msgs []string) {
	bufferSize := len(m.mtype) * len(m.typeInstance) * len(m.pluginInstance)
	buffers := make([]string, bufferSize)

	msgCount := 0
	for typeOffset := 0; typeOffset < cap(m.mtype); typeOffset++ {
		for pluginInstOffset := 0; pluginInstOffset < cap(m.pluginInstance); pluginInstOffset++ {
			for typeInstOffset := 0; typeInstOffset < cap(m.typeInstance); typeInstOffset++ {
				var sb strings.Builder

				sb.Grow(1024)

				sb.WriteString("[{\"values\": [")
				for i := 0; i < len(m.values); i++ {
					if i > 0 {
						sb.WriteString(",")
					}
					sb.WriteString(m.values[i]())
				}

				sb.WriteString("], \"dstypes\": [")
				for i := 0; i < len(m.dstypes); i++ {
					if i > 0 {
						sb.WriteString(",")
					}
					sb.WriteString("\"")
					sb.WriteString(m.dstypes[i])
					sb.WriteString("\"")
				}

				sb.WriteString("], \"dsnames\": [")
				for i := 0; i < len(m.dsnames); i++ {
					if i > 0 {
						sb.WriteString(",")
					}
					sb.WriteString("\"")
					sb.WriteString(m.dsnames[i])
					sb.WriteString("\"")
				}

				sb.WriteString("], \"time\": ")
				sb.WriteString(strconv.FormatFloat(float64((time.Now().UnixNano()))/1000000000, 'f', 4, 64))

				sb.WriteString(", \"interval\": ")
				sb.WriteString(strconv.Itoa(m.interval))

				sb.WriteString(", \"host\": \"")
				sb.WriteString(*m.hostname)

				sb.WriteString("\", \"plugin\": \"")
				sb.WriteString(m.name)

				sb.WriteString("\",\"plugin_instance\": \"")
				sb.WriteString(m.pluginInstance[pluginInstOffset])

				sb.WriteString("\",\"type\": \"")
				sb.WriteString(m.mtype[typeOffset])

				sb.WriteString("\",\"type_instance\": \"")
				sb.WriteString(m.typeInstance[typeInstOffset])

				sb.WriteString("\"}]")

				buffers[msgCount] = sb.String()
				msgCount++
			}
		}
	}
	return buffers
}

func uptimeFunc() string {
	uptime := time.Now().Sub(startTime)

	return strconv.Itoa(int(uptime.Seconds()))
}

func randomFloatFunc() string {
	return strconv.FormatFloat(rand.Float64(), 'f', 4, 64)
}

func generateHosts(hostPrefix *string, numHosts int, numPlugins int, intervalSec int, numTypes int, numTypeInstances int, numPluginInstances int, uptimeEnable bool) []host {

	hosts := make([]host, numHosts)
	for i := 0; i < numHosts; i++ {
		hName := *hostPrefix + fmt.Sprintf(hostnameTemplate, i)
		hosts[i].name = hName
		hosts[i].plugins = make([]plugin, numPlugins)

		for j := 0; j < numPlugins; j++ {
			hosts[i].plugins[j].name = fmt.Sprintf(metricsTemplate, j)
			hosts[i].plugins[j].interval = intervalSec
			hosts[i].plugins[j].hostname = &hosts[i].name
			hosts[i].plugins[j].mtype = make([]string, numTypes)
			for k := 0; k < numTypes; k++ {
				hosts[i].plugins[j].mtype[k] = fmt.Sprintf("type%d", k)
			}
			hosts[i].plugins[j].typeInstance = make([]string, numTypeInstances)
			for k := 0; k < numTypeInstances; k++ {
				hosts[i].plugins[j].typeInstance[k] = fmt.Sprintf("typInst%d", k)
			}
			hosts[i].plugins[j].pluginInstance = make([]string, numPluginInstances)
			for k := 0; k < numPluginInstances; k++ {
				hosts[i].plugins[j].pluginInstance[k] = fmt.Sprintf("pluginInst%d", k)
			}
			hosts[i].plugins[j].values = []pluginFunc{randomFloatFunc}
			hosts[i].plugins[j].dstypes = []string{"derive"}
			hosts[i].plugins[j].dsnames = []string{"samples"}
		}

		if uptimeEnable {
			//
			// Prepend uptime plugin simulation for each host if requested
			//
			uptimePlugin := plugin{
				values:         []pluginFunc{uptimeFunc},
				name:           "uptime",
				hostname:       &hosts[i].name,
				dstypes:        []string{"gauge"},
				dsnames:        []string{"value"},
				interval:       5,
				pluginInstance: []string{""},
				mtype:          []string{"uptime"},
				typeInstance:   []string{""},
			}
			hosts[i].plugins = append([]plugin{uptimePlugin}, hosts[i].plugins...)
		}
	}
	return hosts
}

/*
func getMessagesLimit(urls string, metricsInAmqp int, enableCPUProfile bool) {
	dummyHost := "testHost"
	dummyPlugin := &plugin{
		hostname: &dummyHost,
		name:     "testPlugin",
		interval: 10,
	}

	container := electron.NewContainer(fmt.Sprintf("telemetry-bench%d", os.Getpid()))
	url, err := amqp.ParseURL(urls) // HERE
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
			metrics := dummyPlugin.GetMetricMessage()
			for _, metric := range metrics {
				msg := amqp.NewMessage()  //HERE
				body := amqp.Binary(metric)  //HERE
				msg.Marshal(body)
				s.SendAsync(msg, ackChan, body)
				countSent = countSent + 1

				select {
				case <-cancelMesg:
					waitb.Done()
					return
				default:
				}
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
}
*/

func main() {
	// parse command line option
	hostsNum := flag.Int("hosts", 1, "Number of hosts to simulate")
	spread := flag.Bool("spread", false, "Spread messages over the interval")
	metricsNum := flag.Int("metrics", 1, "Metrics per AMQP messages")
	prefixString := flag.String("hostprefix", "", "Host prefix added to the generated hostname000")
	pluginNum := flag.Int("plugins", 1, "Plugins per per host")
	typeNum := flag.Int("types", 1, "Number of types per plugins")
	pluginInstanceNum := flag.Int("instances", 1, "Plugins instances per plugin")
	typeInstanceNum := flag.Int("typeinstances", 1, "Plugins type instances per plugin")
	intervalSec := flag.Int("interval", 1, "Generation interval (sec)")
	metricMaxSend := flag.Int("send", 1, "How many metrics to send (-1 for continuous)")
	showTimePerMessages := flag.Int("timepermesgs", -1, "Show time for each TIMEPERMESGS message")
	pprofEnable := flag.Bool("profenable", false, "Enable profiling and create and API endpoint")
	pprofileFileName := flag.String("pprofile", "", "go pprofile output")
	modeString := flag.String("mode", "simulate", "Mode (simulate/limit)")
	verbose := flag.Bool("verbose", false, "Print extra info during test...")
	sendThreads := flag.Int("threads", 1, "How many send threads, defaults to 1")
	requireAck := flag.Bool("ack", false, "Require messages to be ack'd ")
	startMetricEnable := flag.Bool("startmetricenable", false, "Generate telemetry_bench_expected_metrics metric at start of test")
	startupWait := flag.Int("startupwait", 5, "Seconds to wait between startup metric and start of test (also helps settle queue timing when no startupmetric is sent)")
	uptimeEnable := flag.Bool("uptimeenable", false, "Generate simulated uptime plugin data for each host")

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
		if *pprofEnable == true {
			go func() {
				log.Println(http.ListenAndServe("localhost:6060", nil))
			}()
		}
	}

	rand.Seed(time.Now().UnixNano())
	hosts := generateHosts(prefixString, *hostsNum, *pluginNum, *intervalSec, *typeNum, *typeInstanceNum, *pluginInstanceNum, *uptimeEnable)

	if *modeString == "limit" {
		//getMessagesLimit(urls[0], *metricsNum, *pprofileFileName != "")
		fmt.Println("limit testing is currently disabled, sorry. It was useless with such a slow sender, maybe we'll re-enable it if this is fast now!")
		return
	} else if *modeString != "simulate" {
		fmt.Fprintf(os.Stderr, "Invalid mode string (simulate/limit): %s", *modeString)
		return
	}

	u, err := url.Parse(urls[0])
	endPointURL := u.Scheme + "://" + u.Host
	amqpAddr := u.Path

	client, err := amqp.Dial(endPointURL)
	if err != nil {
		log.Fatal("Dialing AMQP server:", err)
		return
	}
	defer client.Close()

	session, err := client.NewSession()
	if err != nil {
		log.Fatal("Creating AMQP session:", err)
		return
	}

	sender, err := session.NewSender(
		amqp.LinkTargetAddress(amqpAddr),
	)
	if err != nil {
		log.Fatal("Creating sender link:", err)
		return
	}

	mesgChan := make(chan *amqp.Message, 200)
	countAck := 0

	var wait sync.WaitGroup
	var waitb sync.WaitGroup

	sendCount := make([]int, *sendThreads)
	totalSendCount := make([]int64, *sendThreads)

	fmt.Printf("Send %v metrics every %v second(s)\n", *hostsNum**pluginNum**pluginInstanceNum**typeNum**typeInstanceNum, *intervalSec)
	if *spread == true {
		sleepDur := time.Duration((int64(*intervalSec) * int64(time.Second)) / int64(len(hosts)))
		sleepFunc = func() { time.Sleep(sleepDur) }
	}

	wait.Add(1)
	start := make(chan bool, 1) // For synchronizing the start of generating and sending

	// The following function generates AMQP messages and places them on a queue
	// after we tell it to start
	go func() {
		defer wait.Done()

		<-start // Wait here for the sending thread to be ready

		for i := 0; ; i++ {
			if i >= *metricMaxSend && *metricMaxSend != -1 {
				fmt.Printf("done...\n")
				break
			}
			start := time.Now()
			genCount := 0
			var totalSent int64
			fmt.Printf("Total sent ")
			for index := 0; index < *sendThreads; index++ {
				fmt.Printf("(%d)%d, ", index, totalSendCount[index])
				sendCount[index] = 0
				totalSent += totalSendCount[index]
			}
			fmt.Printf("total %d, %d ack'd\n", totalSent, countAck)

			for _, v := range hosts {
				if *spread == true {
					sleepFunc()
				}
				for _, w := range v.plugins {
					metrics := w.GetMetricMessage()
					for _, metric := range metrics {
						msg := amqp.NewMessage([]byte(metric))
						if *requireAck == false {
							msg.SendSettled = true
						}
						mesgChan <- msg

						genCount = genCount + 1
					}
				}
			}
			duration := time.Now().Sub(start)

			if *verbose {
				fmt.Printf("Generated %d metrics in %v\n", genCount*(*metricsNum), duration)
			}
			if *spread == false {
				time.Sleep(time.Duration(*intervalSec) * time.Second)
			}
		}
	}()

	cancel := make(chan struct{})
	cancelMesg := make(chan struct{})
	ctx := context.Background()

	// Send startup message to prime the pipe and help with evaluating test
	// See https://github.com/redhat-service-assurance/telemetry-bench/issues/6 for details
	if *startMetricEnable {
		startMetricContent := fmt.Sprintf(`
		  [{"values": [%d, %d, %d],
		  "dstypes": ["gauge", "gauge", "gauge"], "dsnames":["expected_metrics_per_interval", "intervals", "interval_length_seconds"], "time": %d, "interval": %d,
		  "host": "%s", "plugin": "telemetry_bench", "plugin_instance": "%d",
		  "type": "%s", "type_instance": "%d"}]`,
			*hostsNum**pluginNum**pluginInstanceNum**typeNum**typeInstanceNum,
			*metricMaxSend, *intervalSec,
			time.Now().Unix(), *intervalSec,
			os.Getenv("HOSTNAME"), time.Now().Unix()+int64(*startupWait),
			*modeString, *sendThreads,
		)
		msg := amqp.NewMessage([]byte(startMetricContent))
		if *requireAck == false {
			msg.SendSettled = true
		}
		err := sender.Send(ctx, msg)
		if err != nil {
			log.Fatal("Sending startup AMQP message:", err)
			return
		}
	}

	time.Sleep(time.Duration(*startupWait) * time.Second)

	start <- true // Signal to the generator that we're ready to start
	for index := 0; index < *sendThreads; index++ {
		// routine for sending mesg
		waitb.Add(1)
		go func(threadIndex int) {
			if err != nil {
				log.Fatal(err)
			}
			lastCounted := time.Now()

			for {
				select {
				case msg := <-mesgChan:
					if sendCount[threadIndex] == 0 {
						lastCounted = time.Now()
					}
					sender.Send(ctx, msg)
					totalSendCount[threadIndex]++
					sendCount[threadIndex]++
					if *showTimePerMessages != -1 && sendCount[threadIndex] == *showTimePerMessages {
						d := time.Now().Sub(lastCounted)
						tpm := (d.Seconds() / float64(sendCount[threadIndex]**metricsNum)) * 1000000
						fmt.Printf("(%d): Sent %d metrics in %v, ( %.3f uS per metric )\n", threadIndex, sendCount[threadIndex]**metricsNum, d, tpm)
						sendCount[threadIndex] = 0
					}

				case <-cancelMesg:
					waitb.Done()
					return
				}
			}
		}(index)
	}

	wait.Wait()
	close(cancelMesg)
	close(cancel)
	waitb.Wait()
}
