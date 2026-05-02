package main

import (
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/godbus/dbus/v5"
)

const (
	avahiBusName         = "org.freedesktop.Avahi"
	avahiServerPath      = dbus.ObjectPath("/")
	avahiServerIface     = "org.freedesktop.Avahi.Server"
	avahiEntryGroupIface = "org.freedesktop.Avahi.EntryGroup"

	avahiIfUnspec    = int32(-1)
	avahiProtoUnspec = int32(-1)

	dnsClassIN   = uint16(1)
	dnsTypeCNAME = uint16(5)
	dnsTTL       = uint32(4500) // 75 min, standard for mDNS host records
)

type hostRecord struct {
	group dbus.BusObject
	refs  int
}

type avahiClient struct {
	conn       *dbus.Conn
	server     dbus.BusObject
	hostFQDN   string // e.g. "myserver.local"
	mu         sync.Mutex
	records    map[string]*hostRecord // hostname → shared EntryGroup + ref count
	containers map[string][]string   // containerID → registered aliases
}

func newAvahiClient() (*avahiClient, error) {
	conn, err := dbus.ConnectSystemBus()
	if err != nil {
		return nil, fmt.Errorf("connect system bus: %w", err)
	}

	server := conn.Object(avahiBusName, avahiServerPath)

	var ver string
	if err := server.Call(avahiServerIface+".GetVersionString", 0).Store(&ver); err != nil {
		conn.Close()
		return nil, fmt.Errorf("ping avahi: %w", err)
	}

	var hostFQDN string
	if err := server.Call(avahiServerIface+".GetHostNameFqdn", 0).Store(&hostFQDN); err != nil {
		conn.Close()
		return nil, fmt.Errorf("get host FQDN: %w", err)
	}
	// Avahi may return a trailing dot; normalise to plain name.
	hostFQDN = strings.TrimSuffix(hostFQDN, ".")

	log.Printf("connected to Avahi %s, host FQDN: %s", ver, hostFQDN)

	return &avahiClient{
		conn:       conn,
		server:     server,
		hostFQDN:   hostFQDN,
		records:    make(map[string]*hostRecord),
		containers: make(map[string][]string),
	}, nil
}

// AddCNAME registers alias as a DNS CNAME pointing to the host's mDNS FQDN.
// Multiple containers may register the same alias — the EntryGroup is shared
// via reference counting and freed only when the last container is removed.
func (a *avahiClient) AddCNAME(containerID, alias string) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if rec, exists := a.records[alias]; exists {
		rec.refs++
		a.containers[containerID] = append(a.containers[containerID], alias)
		log.Printf("avahi: shared CNAME %s (container %.12s, refs=%d)", alias, containerID, rec.refs)
		return nil
	}

	var groupPath dbus.ObjectPath
	if err := a.server.Call(avahiServerIface+".EntryGroupNew", 0).Store(&groupPath); err != nil {
		return fmt.Errorf("entry group new: %w", err)
	}
	group := a.conn.Object(avahiBusName, groupPath)

	call := group.Call(avahiEntryGroupIface+".AddRecord", 0,
		avahiIfUnspec,
		avahiProtoUnspec,
		uint32(0),
		alias,
		dnsClassIN,
		dnsTypeCNAME,
		dnsTTL,
		encodeDNSName(a.hostFQDN),
	)
	if call.Err != nil {
		group.Call(avahiEntryGroupIface+".Free", 0)
		return fmt.Errorf("add CNAME record: %w", call.Err)
	}

	if err := group.Call(avahiEntryGroupIface+".Commit", 0).Err; err != nil {
		group.Call(avahiEntryGroupIface+".Free", 0)
		return fmt.Errorf("commit: %w", err)
	}

	a.records[alias] = &hostRecord{group: group, refs: 1}
	a.containers[containerID] = append(a.containers[containerID], alias)
	log.Printf("avahi: registered CNAME %s → %s (container %.12s)", alias, a.hostFQDN, containerID)
	return nil
}

// AddHost registers hostname→ip as an A/AAAA record in Avahi.
// Used for mdns.host.container where each container has a unique IP.
func (a *avahiClient) AddHost(containerID, hostname, ip string) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if rec, exists := a.records[hostname]; exists {
		rec.refs++
		a.containers[containerID] = append(a.containers[containerID], hostname)
		log.Printf("avahi: shared host %s (container %.12s, refs=%d)", hostname, containerID, rec.refs)
		return nil
	}

	var groupPath dbus.ObjectPath
	if err := a.server.Call(avahiServerIface+".EntryGroupNew", 0).Store(&groupPath); err != nil {
		return fmt.Errorf("entry group new: %w", err)
	}
	group := a.conn.Object(avahiBusName, groupPath)

	call := group.Call(avahiEntryGroupIface+".AddAddress", 0,
		avahiIfUnspec,
		avahiProtoUnspec,
		uint32(0),
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

	a.records[hostname] = &hostRecord{group: group, refs: 1}
	a.containers[containerID] = append(a.containers[containerID], hostname)
	log.Printf("avahi: registered %s -> %s (container %.12s)", hostname, ip, containerID)
	return nil
}

// RemoveHost decrements ref counts for all records held by containerID.
// An EntryGroup is freed in Avahi only when its ref count reaches zero.
func (a *avahiClient) RemoveHost(containerID string) {
	a.mu.Lock()
	defer a.mu.Unlock()

	hostnames, ok := a.containers[containerID]
	if !ok {
		return
	}
	delete(a.containers, containerID)

	for _, hostname := range hostnames {
		rec, exists := a.records[hostname]
		if !exists {
			continue
		}
		rec.refs--
		if rec.refs == 0 {
			rec.group.Call(avahiEntryGroupIface+".Free", 0)
			delete(a.records, hostname)
			log.Printf("avahi: removed %s (container %.12s)", hostname, containerID)
		} else {
			log.Printf("avahi: released %s (container %.12s, refs=%d)", hostname, containerID, rec.refs)
		}
	}
}

func (a *avahiClient) Close() {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, rec := range a.records {
		rec.group.Call(avahiEntryGroupIface+".Free", 0)
	}
	a.conn.Close()
}

// encodeDNSName encodes a hostname into DNS wire format (RFC 1035 §3.1).
func encodeDNSName(name string) []byte {
	var buf []byte
	for _, label := range strings.Split(name, ".") {
		buf = append(buf, byte(len(label)))
		buf = append(buf, label...)
	}
	return append(buf, 0) // root label
}
