package client

import (
	"context"
	"sync"
	"time"

	"github.com/lamhai1401/gologs/logs"
	v3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

var (
	dialTimeout    = 2 * time.Second
	requestTimeout = 10 * time.Second
)

type EctdResp struct {
	Data interface{}
	Err  error
}

const (
	maxChannSize     = 1024
	WatchLockOk      = 1
	WatchLockTimeout = 0
)

func Connect(url string, cfg *EtcdCfg) (Connection, error) {
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = dialTimeout
	}
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = requestTimeout
	}

	cli, err := v3.New((v3.Config{
		DialTimeout: cfg.DialTimeout,
		Endpoints:   []string{url},
	}))

	if err != nil {
		return nil, err
	}

	// defer cli.Close()
	kv := v3.NewKV(cli)

	// create a sessions to aqcuire a lock
	s, err := concurrency.NewSession(cli, concurrency.WithTTL(cfg.SessionTTL))
	if err != nil {
		return nil, err
	}

	client := &Client{
		cli:          cli,
		kv:           kv,
		session:      s,
		sessionMutex: make(map[string]*concurrency.Mutex),
	}

	return client, nil
}

type EtcdCfg struct {
	DialTimeout    time.Duration
	RequestTimeout time.Duration
	SessionTTL     int
}

type Connection interface {
	WatchKey(key string, ctx context.Context, opts ...v3.OpOption) chan *v3.Event
	PutKey(key, val string, opts ...v3.OpOption) (*v3.PutResponse, error)
	GetKey(key string, opts ...v3.OpOption) (*v3.GetResponse, error)
	DeleteKey(key string, opts ...v3.OpOption) (*v3.DeleteResponse, error)
	GetSessionKey(key string) *concurrency.Mutex
	KeyClock(l *concurrency.Mutex, excute func()) chan *EctdResp
}

type Client struct {
	cli          *v3.Client
	kv           v3.KV
	session      *concurrency.Session          // TTL for each session lock
	sessionMutex map[string]*concurrency.Mutex // to save session mutex for each key init
	mu           sync.Mutex
}

func (c *Client) GetSessionKey(key string) *concurrency.Mutex {
	return concurrency.NewMutex(c.session, key)
}

// KeyClock waiting key free until ctx timeout, it will return err or key
func (c *Client) KeyClock(l *concurrency.Mutex, excute func()) chan *EctdResp {
	chann := make(chan *EctdResp, 1)
	go func() {
		resp := &EctdResp{
			Data: l.Key(),
		}
		ctx, cancel := GetCtxTimeout(int(requestTimeout))
		defer cancel()
		defer func() {
			// prevent excute too long
			unLockCtx, _ := GetCtxTimeout(int(requestTimeout))
			l.Unlock(unLockCtx)
		}()

		err := l.Lock(ctx)
		if err != nil {
			resp.Err = err
		}
		chann <- resp
		// call func if whan
		if excute != nil {
			excute()
			// un lock when excute done
		}
	}()
	return chann
}

func (c *Client) DeleteKey(key string, opts ...v3.OpOption) (*v3.DeleteResponse, error) {
	ctx, cancel := GetCtxTimeout(int(requestTimeout))
	defer cancel()
	return c.kv.Delete(ctx, key, opts...)
}

func (c *Client) GetKey(key string, opts ...v3.OpOption) (*v3.GetResponse, error) {
	ctx, cancel := GetCtxTimeout(int(requestTimeout))
	defer cancel()
	return c.kv.Get(ctx, key, opts...)
}

func (c *Client) PutKey(key, val string, opts ...v3.OpOption) (*v3.PutResponse, error) {
	ctx, cancel := GetCtxTimeout(int(requestTimeout))
	defer cancel()
	return c.kv.Put(ctx, key, val, opts...)
}

// WatchKey return event when key was change, input is could be context timeout or call cancel func when stop watch
func (c *Client) WatchKey(key string, ctx context.Context, opts ...v3.OpOption) chan *v3.Event {
	chann := make(chan *v3.Event, maxChannSize)
	go func() {
		watchChan := c.cli.Watch(ctx, "key", v3.WithPrefix())
		for {
			select {
			case result := <-watchChan:
				for _, ev := range result.Events {
					chann <- ev
				}
			case <-ctx.Done():
				logs.Warn(key, "watching was stop")
				return
			}
		}
	}()
	return chann
}

func (c *Client) GetRespFrom()
