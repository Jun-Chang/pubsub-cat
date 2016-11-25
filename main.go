package main

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"cloud.google.com/go/pubsub"
	flags "github.com/jessevdk/go-flags"
)

type Options struct {
	Project string `short:"p" long:"project" description:"GCP Project ID" required:"true"`
	Topic   string `short:"t" long:"topic" description:"PUB/SUB Topic" required:"true"`
}

var opts Options

func main() {
	body, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		log.Fatalf("invalid input error %v", err)
	}

	parser := flags.NewParser(&opts, flags.Default)
	parser.Name = "pubsub-cat"
	parser.Usage = "[OPTIONS]"

	if _, err := parser.Parse(); err != nil {
		log.Println(err)
		parser.WriteHelp(os.Stdout)
		os.Exit(1)
	}

	ctx := context.Background()

	client, err := pubsub.NewClient(ctx, opts.Project)
	if err != nil {
		log.Fatalf("pubsub Client initialize failed %v", err)
	}

	topic := client.Topic(opts.Topic)

	for _, line := range strings.Split(string(body), "\n") {
		if line == "" {
			break
		}
		v := map[string]interface{}{}
		b := []byte(line)

		if err := json.Unmarshal(b, &v); err != nil {
			log.Fatalf("json format error %v %s", err, line)
		}
		ids, err := publish(ctx, topic, b)
		if err != nil {
			log.Fatalf("publish failed %v", err)
		}
		log.Printf("published %v", ids)
	}
	os.Exit(0)
}

func publish(ctx context.Context, topic *pubsub.Topic, b []byte) ([]string, error){
	msg := &pubsub.Message{
		Data: b,
	}
	return topic.Publish(ctx, msg)
}
