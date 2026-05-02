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

const (
	// mdnsHostLabel publishes a CNAME pointing to the host's mDNS FQDN.
	mdnsHostLabel = "mdns.host"
	// mdnsHostContainerLabel publishes an A record with the container's own IP.
	mdnsHostContainerLabel = "mdns.host.container"
)

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

func (w *watcher) syncRunning(ctx context.Context) error {
	containers, err := w.docker.ContainerList(ctx, container.ListOptions{})
	if err != nil {
		return err
	}
	for _, c := range containers {
		_, hasHost := c.Labels[mdnsHostLabel]
		_, hasContainer := c.Labels[mdnsHostContainerLabel]
		if !hasHost && !hasContainer {
			continue
		}
		info, err := w.docker.ContainerInspect(ctx, c.ID)
		if err != nil {
			log.Printf("sync: inspect %.12s: %v", c.ID, err)
			continue
		}
		w.register(info)
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
		info, err := w.docker.ContainerInspect(ctx, ev.Actor.ID)
		if err != nil {
			log.Printf("inspect %.12s: %v", ev.Actor.ID, err)
			return
		}
		w.register(info)
	case "die":
		w.avahi.RemoveHost(ev.Actor.ID)
	}
}

// register publishes all mdns labels for a container.
func (w *watcher) register(info types.ContainerJSON) {
	labels := info.Config.Labels

	// mdns.host → CNAME pointing to the host's mDNS FQDN
	if val, ok := labels[mdnsHostLabel]; ok {
		for _, alias := range parseHostnames(val) {
			if err := w.avahi.AddCNAME(info.ID, alias); err != nil {
				log.Printf("avahi: host label for %.12s: %v", info.ID, err)
			}
		}
	}

	// mdns.host.container → A record with the container's own IP
	if val, ok := labels[mdnsHostContainerLabel]; ok {
		ip, err := containerIP(info)
		if err != nil {
			log.Printf("avahi: container IP for %.12s: %v", info.ID, err)
		} else {
			for _, hostname := range parseHostnames(val) {
				if err := w.avahi.AddHost(info.ID, hostname, ip); err != nil {
					log.Printf("avahi: container label for %.12s: %v", info.ID, err)
				}
			}
		}
	}
}

func containerIP(info types.ContainerJSON) (string, error) {
	for _, n := range info.NetworkSettings.Networks {
		if n.IPAddress != "" {
			return n.IPAddress, nil
		}
	}
	if info.NetworkSettings.IPAddress != "" {
		return info.NetworkSettings.IPAddress, nil
	}
	return "", fmt.Errorf("no IP address found")
}

// parseHostnames splits a comma-separated label value into normalized hostnames.
func parseHostnames(val string) []string {
	parts := strings.Split(val, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if h := strings.TrimSpace(p); h != "" {
			out = append(out, normalizeHostname(h))
		}
	}
	return out
}

func normalizeHostname(h string) string {
	if !strings.HasSuffix(h, ".local") {
		return h + ".local"
	}
	return h
}
