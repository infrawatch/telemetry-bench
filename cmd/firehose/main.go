package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"

	_ "net/http/pprof"

	"pack.ag/amqp"
)

func main() {
	flag.Parse()

	urls := flag.Args()
	if len(urls) == 0 {
		fmt.Fprintln(os.Stderr, "amqp URL is missing")
		os.Exit(1)
	} else if len(urls) > 1 {
		fmt.Fprintln(os.Stderr, "Only one amqp URL is supported")
		os.Exit(1)
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
	msg := amqp.NewMessage([]byte(`[{"values":[11035,219350],"dstypes":["derive","derive"],"dsnames":["read","write"],"time":1536615315.346,"interval":5.000,"host":"nfvha-compute1-lab-node","plugin":"virt","plugin_instance":"instance-0000002c","type":"disk_ops","type_instance":"vda"}]`))
	msg.SendSettled = true
	for {
		err := sender.Send(context.Background(), msg)
		if err != nil {
			log.Fatal("Sending AMQP message:", err)
			return
		}
	}
}
