package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	dockerevents "github.com/docker/docker/api/types/events"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/client"
)

const mdnsHostLabel = "mdns.host"

type watcher struct {
	docker *client.Client
	avahi  *avahiClient
}

func newWatcher(docker *client.Client, avahi *avahiClient) *watcher {
	return &watcher{docker: docker, avahi: avahi}
}

func (w *watcher) Start(ctx context.Context) error {
	if err := w.syncRunning(ctx); err != nil {
		return fmt.Errorf("initial sync: %w", err)
	}
	go w.watchEvents(ctx)
	return nil
}

// syncRunning registers all already-running containers that carry the label.
func (w *watcher) syncRunning(ctx context.Context) error {
	containers, err := w.docker.ContainerList(ctx, container.ListOptions{})
	if err != nil {
		return err
	}
	for _, c := range containers {
		hostname, ok := c.Labels[mdnsHostLabel]
		if !ok {
			continue
		}
		ip, err := w.containerIP(ctx, c.ID)
		if err != nil {
			log.Printf("sync: can't get IP for %.12s: %v", c.ID, err)
			continue
		}
		if err := w.avahi.AddHost(c.ID, normalizeHostname(hostname), ip); err != nil {
			log.Printf("sync: avahi error for %.12s: %v", c.ID, err)
		}
	}
	return nil
}

func (w *watcher) watchEvents(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		w.consumeEvents(ctx)
		if ctx.Err() != nil {
			return
		}
		log.Println("docker event stream ended, reconnecting in 5s")
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}
}

func (w *watcher) consumeEvents(ctx context.Context) {
	f := filters.NewArgs()
	f.Add("type", "container")
	f.Add("event", "start")
	f.Add("event", "die")

	eventCh, errCh := w.docker.Events(ctx, dockerevents.ListOptions{Filters: f})
	for {
		select {
		case <-ctx.Done():
			return
		case err := <-errCh:
			if err != nil {
				log.Printf("docker events error: %v", err)
			}
			return
		case ev := <-eventCh:
			w.handle(ctx, ev)
		}
	}
}

func (w *watcher) handle(ctx context.Context, ev dockerevents.Message) {
	switch ev.Action {
	case "start":
		w.handleStart(ctx, ev.Actor.ID)
	case "die":
		w.avahi.RemoveHost(ev.Actor.ID)
	}
}

func (w *watcher) handleStart(ctx context.Context, containerID string) {
	info, err := w.docker.ContainerInspect(ctx, containerID)
	if err != nil {
		log.Printf("inspect %.12s: %v", containerID, err)
		return
	}
	hostname, ok := info.Config.Labels[mdnsHostLabel]
	if !ok {
		return
	}
	ip, err := w.containerIPFromInfo(info)
	if err != nil {
		log.Printf("IP for %.12s: %v", containerID, err)
		return
	}
	if err := w.avahi.AddHost(containerID, normalizeHostname(hostname), ip); err != nil {
		log.Printf("avahi add for %.12s: %v", containerID, err)
	}
}

func (w *watcher) containerIP(ctx context.Context, containerID string) (string, error) {
	info, err := w.docker.ContainerInspect(ctx, containerID)
	if err != nil {
		return "", err
	}
	return w.containerIPFromInfo(info)
}

func (w *watcher) containerIPFromInfo(info types.ContainerJSON) (string, error) {
	for _, net := range info.NetworkSettings.Networks {
		if net.IPAddress != "" {
			return net.IPAddress, nil
		}
	}
	if info.NetworkSettings.IPAddress != "" {
		return info.NetworkSettings.IPAddress, nil
	}
	return "", fmt.Errorf("no IP address found")
}

func normalizeHostname(h string) string {
	if !strings.HasSuffix(h, ".local") {
		return h + ".local"
	}
	return h
}
