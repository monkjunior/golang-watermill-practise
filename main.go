package main

import (
	"context"
	stdHttp "net/http"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-http/pkg/http"
	"github.com/ThreeDotsLabs/watermill/message"
)

func main() {
	logger := watermill.NewStdLogger(true, true).With(
		watermill.LogFields{},
		)
	httpSubscriber, err := http.NewSubscriber(
		"localhost:9000",
		http.SubscriberConfig{
			Router: nil,
			UnmarshalMessageFunc: func(topic string, request *stdHttp.Request) (*message.Message, error) {
				var body []byte
				request.Body.Read(body)
				logger.Info("UnmarshalMessageFunc Invoked", nil)
				return message.NewMessage(
						watermill.NewUUID(),
						body,
					), nil
			},
		},
		logger,
		)
	if err != nil {
		logger.Error("Could not create HTTP subscriber", err, nil)
	}

	topic1, _ := httpSubscriber.Subscribe(context.Background(), "/topic1")
	go func() {
		for msg := range topic1 {
			logger.Info("Received message", watermill.LogFields{
				"UUID": msg.UUID,
				"Payload": string(msg.Payload),
			})
			msg.Ack()
		}
	}()

	topic2, _ := httpSubscriber.Subscribe(context.Background(), "/topic2")
	go func() {
		for msg := range topic2 {
			logger.Info("Received message", watermill.LogFields{
				"UUID": msg.UUID,
				"Payload": string(msg.Payload),
			})
			msg.Nack()
		}
	}()

	r, err := message.NewRouter(
		message.RouterConfig{},
		logger,
	)
	if err != nil {
		panic(err)
	}

	r.AddHandler(
		"http_to_kafka",
		"/webhooks", // this is the URL of our API
		httpSubscriber,
		"webhooks", // this is the topic the message will be published to
		nil,
		func(msg *message.Message) ([]*message.Message, error) {
			logger.Info("HandleFunc invoked", nil)
			return []*message.Message{msg}, nil
		},
	)
	// Run router asynchronously
	go r.Run(context.Background())
	// Check if router is running then start HTTP server
	<-r.Running()

	logger.Info("Starting HTTP server", nil)
	err = httpSubscriber.StartHTTPServer()
	if err != nil {
		logger.Error("Could not start HTTP server", err, nil)
	}
}