package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/goburrow/modbus"
	"github.com/gorilla/websocket"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Server struct {
		Listen        string `yaml:"listen"`
		WebSocketPath string `yaml:"websocket_path"`
	} `yaml:"server"`
	InfluxDB struct {
		URL    string `yaml:"url"`
		Org    string `yaml:"org"`
		Bucket string `yaml:"bucket"`
		Token  string `yaml:"token"`
	} `yaml:"influxdb"`
	Devices []Device `yaml:"devices"`
}

type Device struct {
	ID           string        `yaml:"id"`
	Protocol     string        `yaml:"protocol"`
	Enabled      bool          `yaml:"enabled"`
	PollInterval time.Duration `yaml:"poll_interval"`
	Address      string        `yaml:"address"` // HTTP URL or raw TCP address
	Method       string        `yaml:"method"`
	Request      string        `yaml:"request"` // raw TCP request bytes as text
	Timeout      time.Duration `yaml:"timeout"`
	Modbus       ModbusConfig  `yaml:"modbus"`
	Points       []Register    `yaml:"points"`
}

type ModbusConfig struct {
	Address  string `yaml:"address"` // Modbus TCP host:port
	Port     string `yaml:"port"`    // Modbus RTU serial device
	BaudRate int    `yaml:"baud_rate"`
	DataBits int    `yaml:"data_bits"`
	Parity   string `yaml:"parity"`
	StopBits int    `yaml:"stop_bits"`
	SlaveID  byte   `yaml:"slave_id"`
	Timeout  time.Duration `yaml:"timeout"`
}

type Register struct {
	Name      string  `yaml:"name"`
	Function  string  `yaml:"function"`
	Address   uint16  `yaml:"address"`
	Quantity  uint16  `yaml:"quantity"`
	DataType  string  `yaml:"data_type"`
	ByteOrder string  `yaml:"byte_order"`
	Scale     float64 `yaml:"scale"`
	Offset    float64 `yaml:"offset"`
}

type Event struct {
	DeviceID  string                 `json:"device_id"`
	Protocol  string                 `json:"protocol"`
	Timestamp time.Time              `json:"timestamp"`
	Values    map[string]interface{} `json:"values"`
}

type Hub struct {
	mu      sync.RWMutex
	clients map[chan Event]struct{}
}

func newHub() *Hub { return &Hub{clients: make(map[chan Event]struct{})} }
func (h *Hub) publish(e Event) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for ch := range h.clients {
		select { case ch <- e: default: }
	}
}
func (h *Hub) serveWS(w http.ResponseWriter, r *http.Request) {
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool {
		origin := r.Header.Get("Origin")
		if origin == "" { return true }
		u, err := netURLHost(origin)
		return err == nil && u == r.Host
	}}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil { return }
	defer conn.Close()
	ch := make(chan Event, 64)
	h.mu.Lock(); h.clients[ch] = struct{}{}; h.mu.Unlock()
	defer func() { h.mu.Lock(); delete(h.clients, ch); h.mu.Unlock() }()
	_ = conn.SetWriteDeadline(time.Now().Add(10*time.Second))
	for {
		select {
		case e := <-ch:
			_ = conn.SetWriteDeadline(time.Now().Add(10*time.Second))
			if err := conn.WriteJSON(e); err != nil { return }
		case <-r.Context().Done(): return
		}
	}
}

func netURLHost(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil { return "", err }
	return u.Host, nil
}

type Sink struct { write api.WriteAPIBlocking; org, bucket string }
func (s *Sink) writeEvent(ctx context.Context, e Event) error {
	fields := make(map[string]interface{}, len(e.Values))
	for k, v := range e.Values {
		switch v.(type) {
		case bool, string, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
			fields[k] = v
		default:
			fields[k] = fmt.Sprint(v)
		}
	}
	if len(fields) == 0 { return nil }
	p := write.NewPoint("telemetry", map[string]string{"device_id": e.DeviceID, "protocol": e.Protocol}, fields, e.Timestamp)
	return s.write.WritePoint(ctx, p)
}

func main() {
	configPath := "config.yaml"
	if len(os.Args) > 1 { configPath = os.Args[1] }
	data, err := os.ReadFile(configPath); fatalIf(err)
	data = []byte(os.ExpandEnv(string(data)))
	var cfg Config
	fatalIf(yaml.Unmarshal(data, &cfg))
	if cfg.Server.Listen == "" { cfg.Server.Listen = ":8080" }
	if cfg.Server.WebSocketPath == "" { cfg.Server.WebSocketPath = "/ws" }
	if cfg.InfluxDB.URL == "" || cfg.InfluxDB.Org == "" || cfg.InfluxDB.Bucket == "" || cfg.InfluxDB.Token == "" {
		log.Fatal("influxdb.url, org, bucket and token are required")
	}
	client := influxdb2.NewClient(cfg.InfluxDB.URL, cfg.InfluxDB.Token)
	defer client.Close()
	sink := &Sink{write: client.WriteAPIBlocking(cfg.InfluxDB.Org, cfg.InfluxDB.Bucket), org: cfg.InfluxDB.Org, bucket: cfg.InfluxDB.Bucket}
	hub := newHub()
	mux := http.NewServeMux()
	mux.HandleFunc(cfg.Server.WebSocketPath, hub.serveWS)
	server := &http.Server{Addr: cfg.Server.Listen, Handler: mux, ReadHeaderTimeout: 5*time.Second}
	go func() { log.Printf("WebSocket listening on %s%s", cfg.Server.Listen, cfg.Server.WebSocketPath); if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) { log.Printf("web server: %v", err) } }()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var wg sync.WaitGroup
	for _, d := range cfg.Devices {
		if !d.Enabled { continue }
		if d.ID == "" || d.PollInterval <= 0 { log.Printf("skipping invalid device config: id and positive poll_interval are required"); continue }
		wg.Add(1)
		go func(dev Device) { defer wg.Done(); runDevice(ctx, dev, hub, sink) }(d)
	}
	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdown)
	wg.Wait()
}

func runDevice(ctx context.Context, d Device, hub *Hub, sink *Sink) {
	for {
		started := time.Now()
		e, err := collect(ctx, d)
		if err != nil { log.Printf("device=%s protocol=%s collect: %v", d.ID, d.Protocol, err) } else if len(e.Values) > 0 {
			e.DeviceID, e.Protocol = d.ID, strings.ToLower(d.Protocol)
			if e.Timestamp.IsZero() { e.Timestamp = time.Now().UTC() }
			hub.publish(e)
			writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			if err := sink.writeEvent(writeCtx, e); err != nil { log.Printf("device=%s influx write: %v", d.ID, err) }
			cancel()
		}
		wait := time.Until(started.Add(d.PollInterval))
		if wait < 0 { wait = 0 }
		t := time.NewTimer(wait)
		select { case <-ctx.Done(): t.Stop(); return; case <-t.C: }
	}
}

func collect(ctx context.Context, d Device) (Event, error) {
	timeout := d.Timeout; if timeout <= 0 { timeout = 5*time.Second }
	ctx, cancel := context.WithTimeout(ctx, timeout); defer cancel()
	switch strings.ToLower(d.Protocol) {
	case "http": return collectHTTP(ctx, d)
	case "tcp": return collectTCP(ctx, d)
	case "modbus-tcp", "modbus-rtu": return collectModbus(ctx, d)
	default: return Event{}, fmt.Errorf("protocol %q is not implemented in this initial version (supported: http, tcp, modbus-tcp, modbus-rtu)", d.Protocol)
	}
}

func collectHTTP(ctx context.Context, d Device) (Event, error) {
	method := d.Method; if method == "" { method = http.MethodGet }
	req, err := http.NewRequestWithContext(ctx, method, d.Address, nil); if err != nil { return Event{}, err }
	resp, err := http.DefaultClient.Do(req); if err != nil { return Event{}, err }; defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 { return Event{}, fmt.Errorf("HTTP status %s", resp.Status) }
	var values map[string]interface{}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&values); err != nil { return Event{}, err }
	return Event{Timestamp: time.Now().UTC(), Values: values}, nil
}

func collectTCP(ctx context.Context, d Device) (Event, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", d.Address); if err != nil { return Event{}, err }; defer conn.Close()
	deadline, _ := ctx.Deadline(); _ = conn.SetDeadline(deadline)
	if d.Request != "" { if _, err := io.WriteString(conn, d.Request); err != nil { return Event{}, err } }
	buf, err := io.ReadAll(io.LimitReader(conn, 4<<20)); if err != nil { return Event{}, err }
	var values map[string]interface{}
	if err := json.Unmarshal(buf, &values); err != nil { return Event{}, fmt.Errorf("TCP response must be a JSON object: %w", err) }
	return Event{Timestamp: time.Now().UTC(), Values: values}, nil
}

func collectModbus(ctx context.Context, d Device) (Event, error) {
	if len(d.Points) == 0 { return Event{}, errors.New("no Modbus points configured") }
	var handler modbus.ClientHandler
	if strings.EqualFold(d.Protocol, "modbus-rtu") {
		h := modbus.NewRTUClientHandler(d.Modbus.Port)
		h.BaudRate, h.DataBits, h.Parity, h.StopBits, h.SlaveId = d.Modbus.BaudRate, d.Modbus.DataBits, d.Modbus.Parity, d.Modbus.StopBits, d.Modbus.SlaveID
		h.Timeout = d.Modbus.Timeout; if h.Timeout <= 0 { h.Timeout = 3*time.Second }; handler = h
	} else {
		h := modbus.NewTCPClientHandler(d.Modbus.Address)
		h.SlaveId = d.Modbus.SlaveID; h.Timeout = d.Modbus.Timeout; if h.Timeout <= 0 { h.Timeout = 3*time.Second }; handler = h
	}
	if err := handler.Connect(); err != nil { return Event{}, err }; defer handler.Close()
	client := modbus.NewClient(handler)
	values := make(map[string]interface{}, len(d.Points))
	for _, p := range d.Points {
		if p.Name == "" { return Event{}, errors.New("Modbus point name cannot be empty") }
		quantity := p.Quantity; if quantity == 0 { quantity = wordsNeeded(p.DataType) }
		var raw []byte; var err error
		switch strings.ToLower(p.Function) {
		case "holding_register": raw, err = client.ReadHoldingRegisters(p.Address, quantity)
		case "input_register": raw, err = client.ReadInputRegisters(p.Address, quantity)
		case "coil": raw, err = client.ReadCoils(p.Address, quantity)
		case "discrete_input": raw, err = client.ReadDiscreteInputs(p.Address, quantity)
		default: return Event{}, fmt.Errorf("point %s: unsupported function %q", p.Name, p.Function)
		}
		if err != nil { return Event{}, fmt.Errorf("point %s: %w", p.Name, err) }
		v, err := decodeValue(raw, p)
		if err != nil { return Event{}, fmt.Errorf("point %s: %w", p.Name, err) }
		if n, ok := v.(float64); ok { scale := p.Scale; if scale == 0 { scale = 1 }; values[p.Name] = n*scale + p.Offset } else { values[p.Name] = v }
	}
	_ = ctx
	return Event{Timestamp: time.Now().UTC(), Values: values}, nil
}

func wordsNeeded(t string) uint16 { switch strings.ToLower(t) { case "int32", "uint32", "float32": return 2; case "float64", "int64", "uint64": return 4; default: return 1 } }
func decodeValue(raw []byte, p Register) (interface{}, error) {
	if p.Function == "coil" || p.Function == "discrete_input" { if len(raw)==0 { return nil, errors.New("empty response") }; return raw[0]&1 != 0, nil }
	order := strings.ToUpper(p.ByteOrder); b := append([]byte(nil), raw...)
	if order == "CDAB" && len(b)>=4 { b[0],b[1],b[2],b[3]=b[2],b[3],b[0],b[1] } else if order == "BADC" { for i:=0;i+1<len(b);i+=2 { b[i],b[i+1]=b[i+1],b[i] } } else if order == "DCBA" { for i,j:=0,len(b)-1;i<j;i,j=i+1,j-1 { b[i],b[j]=b[j],b[i] } }
	need := int(wordsNeeded(p.DataType))*2; if len(b)<need { return nil, fmt.Errorf("need %d bytes for %s, got %d",need,p.DataType,len(b)) }
	switch strings.ToLower(p.DataType) {
	case "int16": return float64(int16(binary.BigEndian.Uint16(b))), nil
	case "uint16": return float64(binary.BigEndian.Uint16(b)), nil
	case "int32": return float64(int32(binary.BigEndian.Uint32(b))), nil
	case "uint32": return float64(binary.BigEndian.Uint32(b)), nil
	case "float32": return float64(math.Float32frombits(binary.BigEndian.Uint32(b))), nil
	case "float64": return math.Float64frombits(binary.BigEndian.Uint64(b)), nil
	default: return nil, fmt.Errorf("unsupported data_type %q", p.DataType)
	}
}

var _ = strconv.IntSize
