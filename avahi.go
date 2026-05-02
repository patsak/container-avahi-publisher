package main

import (
	"fmt"
	"log"
	"sync"

	"github.com/godbus/dbus/v5"
)

const (
	avahiBusName        = "org.freedesktop.Avahi"
	avahiServerPath     = dbus.ObjectPath("/")
	avahiServerIface    = "org.freedesktop.Avahi.Server"
	avahiEntryGroupIface = "org.freedesktop.Avahi.EntryGroup"

	avahiIfUnspec    = int32(-1)
	avahiProtoUnspec = int32(-1)
)

type avahiClient struct {
	conn   *dbus.Conn
	server dbus.BusObject
	mu     sync.Mutex
	groups map[string]dbus.BusObject // containerID -> EntryGroup
}

func newAvahiClient() (*avahiClient, error) {
	conn, err := dbus.ConnectSystemBus()
	if err != nil {
		return nil, fmt.Errorf("connect system bus: %w", err)
	}

	server := conn.Object(avahiBusName, avahiServerPath)

	// Verify Avahi is reachable.
	var ver string
	if err := server.Call(avahiServerIface+".GetVersionString", 0).Store(&ver); err != nil {
		conn.Close()
		return nil, fmt.Errorf("ping avahi: %w", err)
	}
	log.Printf("connected to Avahi %s", ver)

	return &avahiClient{
		conn:   conn,
		server: server,
		groups: make(map[string]dbus.BusObject),
	}, nil
}

func (a *avahiClient) AddHost(containerID, hostname, ip string) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Remove stale group for this container if present.
	if g, ok := a.groups[containerID]; ok {
		g.Call(avahiEntryGroupIface+".Free", 0)
		delete(a.groups, containerID)
	}

	var groupPath dbus.ObjectPath
	if err := a.server.Call(avahiServerIface+".EntryGroupNew", 0).Store(&groupPath); err != nil {
		return fmt.Errorf("entry group new: %w", err)
	}

	group := a.conn.Object(avahiBusName, groupPath)

	call := group.Call(avahiEntryGroupIface+".AddAddress", 0,
		avahiIfUnspec,    // interface: AVAHI_IF_UNSPEC
		avahiProtoUnspec, // protocol:  AVAHI_PROTO_UNSPEC
		uint32(0),        // flags
		hostname,
		ip,
	)
	if call.Err != nil {
		group.Call(avahiEntryGroupIface+".Free", 0)
		return fmt.Errorf("add address: %w", call.Err)
	}

	if err := group.Call(avahiEntryGroupIface+".Commit", 0).Err; err != nil {
		group.Call(avahiEntryGroupIface+".Free", 0)
		return fmt.Errorf("commit: %w", err)
	}

	a.groups[containerID] = group
	log.Printf("avahi: registered %s -> %s (container %.12s)", hostname, ip, containerID)
	return nil
}

func (a *avahiClient) RemoveHost(containerID string) {
	a.mu.Lock()
	defer a.mu.Unlock()

	group, ok := a.groups[containerID]
	if !ok {
		return
	}
	group.Call(avahiEntryGroupIface+".Free", 0)
	delete(a.groups, containerID)
	log.Printf("avahi: removed host for container %.12s", containerID)
}

func (a *avahiClient) Close() {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, g := range a.groups {
		g.Call(avahiEntryGroupIface+".Free", 0)
	}
	a.conn.Close()
}
