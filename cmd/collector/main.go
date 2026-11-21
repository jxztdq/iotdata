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
	"net/url"
	"os"
	"os/signal"
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
	Address      string        `yaml:"address"`
	Method       string        `yaml:"method"`
	Request      string        `yaml:"request"`
	Timeout      time.Duration `yaml:"timeout"`
	Modbus       ModbusConfig  `yaml:"modbus"`
	Points       []Register    `yaml:"points"`
}

type ModbusConfig struct {
	Address  string        `yaml:"address"`
	Port     string        `yaml:"port"`
	BaudRate int           `yaml:"baud_rate"`
	DataBits int           `yaml:"data_bits"`
	Parity   string        `yaml:"parity"`
	StopBits int           `yaml:"stop_bits"`
	SlaveID  byte          `yaml:"slave_id"`
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
		select {
		case ch <- e:
		default:
		}
	}
}
func (h *Hub) serveWS(w http.ResponseWriter, r *http.Request) {
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool {
		origin := r.Header.Get("Origin")
		if origin == "" {
			return true
		}
		u, err := netURLHost(origin)
		return err == nil && u == r.Host
	}}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	ch := make(chan Event, 64)
	h.mu.Lock()
	h.clients[ch] = struct{}{}
	h.mu.Unlock()
	defer func() {
		h.mu.Lock()
		delete(h.clients, ch)
		h.mu.Unlock()
	}()

	// 读循环：驱动 ping/pong/close 控制帧；客户端一断开就 close(done)，
	// 让写循环立刻退出并注销 channel，避免死连接堆积。
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	ping := time.NewTicker(30 * time.Second)
	defer ping.Stop()

	for {
		select {
		case e := <-ch:
			_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := conn.WriteJSON(e); err != nil {
				return
			}
		case <-ping.C:
			_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		case <-done:
			return
		case <-r.Context().Done():
			return
		}
	}
}

func netURLHost(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	return u.Host, nil
}

type Sink struct {
	write       api.WriteAPIBlocking
	org, bucket string
}

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
	if len(fields) == 0 {
		return nil
	}
	p := write.NewPoint("telemetry", map[string]string{"device_id": e.DeviceID, "protocol": e.Protocol}, fields, e.Timestamp)
	return s.write.WritePoint(ctx, p)
}

func fatalIf(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	configPath := "config.yaml"
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}
	data, err := os.ReadFile(configPath)
	fatalIf(err)
	data = []byte(os.ExpandEnv(string(data)))
	var cfg Config
	fatalIf(yaml.Unmarshal(data, &cfg))
	if cfg.Server.Listen == "" {
		cfg.Server.Listen = ":8080"
	}
	if cfg.Server.WebSocketPath == "" {
		cfg.Server.WebSocketPath = "/ws"
	}
	if cfg.InfluxDB.URL == "" || cfg.InfluxDB.Org == "" || cfg.InfluxDB.Bucket == "" || cfg.InfluxDB.Token == "" {
		log.Fatal("influxdb.url, org, bucket and token are required")
	}
	client := influxdb2.NewClient(cfg.InfluxDB.URL, cfg.InfluxDB.Token)
	defer client.Close()
	sink := &Sink{write: client.WriteAPIBlocking(cfg.InfluxDB.Org, cfg.InfluxDB.Bucket), org: cfg.InfluxDB.Org, bucket: cfg.InfluxDB.Bucket}
	hub := newHub()
	mux := http.NewServeMux()
	mux.HandleFunc(cfg.Server.WebSocketPath, hub.serveWS)
	server := &http.Server{Addr: cfg.Server.Listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Printf("WebSocket listening on %s%s", cfg.Server.Listen, cfg.Server.WebSocketPath)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("web server: %v", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var wg sync.WaitGroup
	for _, d := range cfg.Devices {
		if !d.Enabled {
			continue
		}
		if d.ID == "" || d.PollInterval <= 0 {
			log.Printf("skipping invalid device config: id and positive poll_interval are required")
			continue
		}
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
		if err != nil {
			log.Printf("device=%s protocol=%s collect: %v", d.ID, d.Protocol, err)
		} else if len(e.Values) > 0 {
			e.DeviceID, e.Protocol = d.ID, strings.ToLower(d.Protocol)
			if e.Timestamp.IsZero() {
				e.Timestamp = time.Now().UTC()
			}
			hub.publish(e)
			writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			if err := sink.writeEvent(writeCtx, e); err != nil {
				log.Printf("device=%s influx write: %v", d.ID, err)
			}
			cancel()
		}
		wait := time.Until(started.Add(d.PollInterval))
		if wait < 0 {
			wait = 0
		}
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
	}
}

func collect(ctx context.Context, d Device) (Event, error) {
	timeout := d.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	switch strings.ToLower(d.Protocol) {
	case "http":
		return collectHTTP(ctx, d)
	case "tcp":
		return collectTCP(ctx, d)
	case "modbus-tcp", "modbus-rtu":
		return collectModbus(ctx, d)
	default:
		return Event{}, fmt.Errorf("protocol %q is not implemented in this initial version (supported: http, tcp, modbus-tcp, modbus-rtu)", d.Protocol)
	}
}

func collectHTTP(ctx context.Context, d Device) (Event, error) {
	method := d.Method
	if method == "" {
		method = http.MethodGet
	}
	req, err := http.NewRequestWithContext(ctx, method, d.Address, nil)
	if err != nil {
		return Event{}, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return Event{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return Event{}, fmt.Errorf("HTTP status %s", resp.Status)
	}
	var values map[string]interface{}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&values); err != nil {
		return Event{}, err
	}
	return Event{Timestamp: time.Now().UTC(), Values: values}, nil
}

func collectTCP(ctx context.Context, d Device) (Event, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", d.Address)
	if err != nil {
		return Event{}, err
	}
	defer conn.Close()
	deadline, _ := ctx.Deadline()
	_ = conn.SetDeadline(deadline)
	if d.Request != "" {
		if _, err := io.WriteString(conn, d.Request); err != nil {
			return Event{}, err
		}
	}
	buf, err := io.ReadAll(io.LimitReader(conn, 4<<20))
	if err != nil {
		return Event{}, err
	}
	var values map[string]interface{}
	if err := json.Unmarshal(buf, &values); err != nil {
		return Event{}, fmt.Errorf("TCP response must be a JSON object: %w", err)
	}
	return Event{Timestamp: time.Now().UTC(), Values: values}, nil
}

func collectModbus(ctx context.Context, d Device) (Event, error) {
	if len(d.Points) == 0 {
		return Event{}, errors.New("no Modbus points configured")
	}

	var handler modbus.ClientHandler
	var closer func() error

	if strings.EqualFold(d.Protocol, "modbus-rtu") {
		h := modbus.NewRTUClientHandler(d.Modbus.Port)
		h.BaudRate, h.DataBits, h.Parity, h.StopBits, h.SlaveId = d.Modbus.BaudRate, d.Modbus.DataBits, d.Modbus.Parity, d.Modbus.StopBits, d.Modbus.SlaveID
		h.Timeout = d.Modbus.Timeout
		if h.Timeout <= 0 {
			h.Timeout = 3 * time.Second
		}
		if err := h.Connect(); err != nil {
			return Event{}, err
		}
		closer = h.Close
		handler = h
	} else {
		h := modbus.NewTCPClientHandler(d.Modbus.Address)
		h.SlaveId = d.Modbus.SlaveID
		h.Timeout = d.Modbus.Timeout
		if h.Timeout <= 0 {
			h.Timeout = 3 * time.Second
		}
		if err := h.Connect(); err != nil {
			return Event{}, err
		}
		closer = h.Close
		handler = h
	}
	defer closer()

	client := modbus.NewClient(handler)
	values := make(map[string]interface{}, len(d.Points))
	for _, p := range d.Points {
		if p.Name == "" {
			return Event{}, errors.New("Modbus point name cannot be empty")
		}
		quantity := p.Quantity
		if quantity == 0 {
			quantity = wordsNeeded(p.DataType)
		}
		var raw []byte
		var err error
		switch strings.ToLower(p.Function) {
		case "holding_register":
			raw, err = client.ReadHoldingRegisters(p.Address, quantity)
		case "input_register":
			raw, err = client.ReadInputRegisters(p.Address, quantity)
		case "coil":
			raw, err = client.ReadCoils(p.Address, quantity)
		case "discrete_input":
			raw, err = client.ReadDiscreteInputs(p.Address, quantity)
		default:
			return Event{}, fmt.Errorf("point %s: unsupported function %q", p.Name, p.Function)
		}
		if err != nil {
			return Event{}, fmt.Errorf("point %s: %w", p.Name, err)
		}
		v, err := decodeValue(raw, p)
		if err != nil {
			return Event{}, fmt.Errorf("point %s: %w", p.Name, err)
		}
		scale := p.Scale
		if scale == 0 {
			scale = 1
		}
		if n, ok := toFloat(v); ok {
			values[p.Name] = roundN(n*scale+p.Offset, 6)
		} else {
			values[p.Name] = v
		}
	}
	return Event{Timestamp: time.Now().UTC(), Values: values}, nil
}

// ---- helpers ----

func wordsNeeded(dt string) uint16 {
	switch strings.ToLower(dt) {
	case "uint32", "int32", "float32":
		return 2
	case "uint64", "int64", "float64":
		return 4
	default:
		return 1
	}
}

func decodeValue(raw []byte, p Register) (interface{}, error) {
	if len(raw) == 0 {
		return nil, errors.New("empty response")
	}
	dt := strings.ToLower(p.DataType)
	bo := strings.ToLower(p.ByteOrder)
	var order binary.ByteOrder
	switch bo {
	case "big", "":
		order = binary.BigEndian
	case "little":
		order = binary.LittleEndian
	case "swap":
		order = byteSwap{binary.BigEndian}
	default:
		return nil, fmt.Errorf("unknown byte_order %q", p.ByteOrder)
	}
	switch dt {
	case "uint16":
		return order.Uint16(raw), nil
	case "int16":
		return int16(order.Uint16(raw)), nil
	case "uint32":
		return order.Uint32(raw), nil
	case "int32":
		return int32(order.Uint32(raw)), nil
	case "uint64":
		return order.Uint64(raw), nil
	case "int64":
		return int64(order.Uint64(raw)), nil
	case "float32":
		return math.Float32frombits(order.Uint32(raw)), nil
	case "float64":
		return math.Float64frombits(order.Uint64(raw)), nil
	case "bool":
		return raw[0] != 0, nil
	default:
		return nil, fmt.Errorf("unsupported data_type %q", p.DataType)
	}
}

type byteSwap struct{ binary.ByteOrder }

func (b byteSwap) Uint16(buf []byte) uint16 { return b.ByteOrder.Uint16(buf) }
func (b byteSwap) Uint32(buf []byte) uint32 { return b.ByteOrder.Uint32(buf) }
func (b byteSwap) Uint64(buf []byte) uint64 { return b.ByteOrder.Uint64(buf) }

func toFloat(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	}
	return 0, false
}

// roundN 按小数位四舍五入，消除 0.1 缩放带来的浮点误差（50.900000000000006 → 50.9）
func roundN(x float64, n int) float64 {
	p := math.Pow(10, float64(n))
	return math.Round(x*p) / p
}
