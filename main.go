package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/docker/docker/client"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dockerClient, err := client.NewClientWithOpts(
		client.FromEnv,
		client.WithAPIVersionNegotiation(),
	)
	if err != nil {
		log.Fatalf("docker client: %v", err)
	}
	defer dockerClient.Close()

	avahiClient, err := newAvahiClient()
	if err != nil {
		log.Fatalf("avahi client: %v", err)
	}
	defer avahiClient.Close()

	w := newWatcher(dockerClient, avahiClient)
	if err := w.Start(ctx); err != nil {
		log.Fatalf("watcher: %v", err)
	}

	log.Println("container-avahi-publisher started")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Printf("received %v, shutting down", sig)
}
