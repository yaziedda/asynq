package rmq

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type MgmtClient struct {
	baseURL string
	user    string
	pass    string
	vhost   string
	http    *http.Client
}

func NewMgmtClient(mgmtURL, user, pass, vhost string) *MgmtClient {
	if vhost == "" {
		vhost = "/"
	}
	return &MgmtClient{
		baseURL: strings.TrimRight(mgmtURL, "/"),
		user:    user,
		pass:    pass,
		vhost:   vhost,
		http:    &http.Client{Timeout: 10 * time.Second},
	}
}

func (c *MgmtClient) encodedVhost() string {
	return url.PathEscape(c.vhost)
}

func (c *MgmtClient) request(method, path string, body interface{}, out interface{}) error {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequest(method, c.baseURL+path, reader)
	if err != nil {
		return err
	}
	req.SetBasicAuth(c.user, c.pass)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("rmq mgmt: %s %s returned %d: %s", method, path, resp.StatusCode, b)
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

type QueueStat struct {
	Name          string `json:"name"`
	Messages      int    `json:"messages"`
	MessagesReady int    `json:"messages_ready"`
	MessagesUnack int    `json:"messages_unacknowledged"`
	Consumers     int    `json:"consumers"`
}

func (c *MgmtClient) ListQueues() ([]QueueStat, error) {
	var out []QueueStat
	err := c.request(http.MethodGet, "/api/queues/"+c.encodedVhost(), nil, &out)
	return out, err
}

func (c *MgmtClient) GetQueue(qname string) (*QueueStat, error) {
	var out QueueStat
	err := c.request(http.MethodGet, "/api/queues/"+c.encodedVhost()+"/"+url.PathEscape(qname), nil, &out)
	if err != nil {
		if strings.Contains(err.Error(), "404") {
			return nil, nil
		}
		return nil, err
	}
	return &out, nil
}

type PeekedMessage struct {
	PayloadBytes int    `json:"payload_bytes"`
	Payload      string `json:"payload"`
}

func (c *MgmtClient) Peek(qname string, count int) ([]PeekedMessage, error) {
	body := map[string]interface{}{
		"count":    count,
		"ackmode":  "ack_requeue_true",
		"encoding": "auto",
	}
	var out []PeekedMessage
	err := c.request(http.MethodPost, "/api/queues/"+c.encodedVhost()+"/"+url.PathEscape(qname)+"/get", body, &out)
	return out, err
}

func (c *MgmtClient) PurgeQueue(qname string) error {
	return c.request(http.MethodDelete, "/api/queues/"+c.encodedVhost()+"/"+url.PathEscape(qname)+"/contents", nil, nil)
}
