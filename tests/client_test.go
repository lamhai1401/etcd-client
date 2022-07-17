package tests

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	v3 "go.etcd.io/etcd/client/v3"
)

func TestClient(t *testing.T) {
	cli, _ := v3.New((v3.Config{
		DialTimeout: 3 * time.Second,
		Endpoints:   []string{"127.0.0.1:2379"},
	}))

	// defer cli.Close()
	kv := v3.NewKV(cli)

	var total = 0

	t.Run("Test Watch data", func(t *testing.T) {
		ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
		// Delete all keys
		kv.Delete(ctx, "key", v3.WithPrefix())

		stopChan := make(chan interface{})

		go func() {
			watchChan := cli.Watch(ctx, "key", v3.WithPrefix())
			for {
				select {
				case result := <-watchChan:
					for _, ev := range result.Events {
						total++
						t.Logf("watch %s %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
					}
				case <-stopChan:
					t.Log("Done watching.")
					return
				}
			}
		}()

		// Insert some keys
		for i := 0; i < 10; i++ {
			k := fmt.Sprintf("key_%02d", i)
			kv.Put(ctx, k, strconv.Itoa(i))
		}

		// Make sure watcher go routine has time to recive PUT events
		time.Sleep(3 * time.Second)

		stopChan <- 1

		if total != 10 {
			t.Error("Watching 10 msg fail")
		}
	})

	t.Run("Test Get data", func(t *testing.T) {
		t.Log("*** GetMultipleValuesWithPaginationDemo()")
		ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
		// Delete all keys
		kv.Delete(ctx, "key", v3.WithPrefix())

		// Insert 20 keys
		for i := 0; i < 4; i++ {
			k := fmt.Sprintf("key_%02d", i)
			kv.Put(ctx, k, strconv.Itoa(i))
		}

		// limit
		opts := []v3.OpOption{
			v3.WithPrefix(),
			v3.WithSort(v3.SortByKey, v3.SortAscend),
			v3.WithLimit(3),
		}

		gr, err := kv.Get(ctx, "key", opts...)
		if err != nil {
			t.Error(err.Error())
			return
		}

		total := 0
		t.Log("--- First page ---")

		for _, item := range gr.Kvs {
			total++
			t.Log(string(item.Key), string(item.Value))
		}

		if total > 3 {
			t.Error("Wrong limit 3, total is", total)
			return
		}

		lastKey := string(gr.Kvs[len(gr.Kvs)-1].Key)

		fmt.Println("--- Second page ---")
		opts = append(opts, v3.WithFromKey())
		gr, _ = kv.Get(ctx, lastKey, opts...)

		// Skipping the first item, which the last item from from the previous Get

		for _, item := range gr.Kvs[1:] {
			fmt.Println(string(item.Key), string(item.Value))
			total++
		}

		if total > 4 {
			t.Error("Wrong total is 4, current total is", total)
			return
		}
	})

	t.Run("Test set time to live Lease data", func(t *testing.T) {
		t.Log("*** LeaseDemo()")
		// Delete all keys
		ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
		kv.Delete(ctx, "key", v3.WithPrefix())

		lease, _ := cli.Grant(ctx, 1)

		// Insert key with a lease of 1 second TTL
		kv.Put(ctx, "key", "value", v3.WithLease(lease.ID))

		gr, _ := kv.Get(ctx, "key")
		if len(gr.Kvs) == 0 {
			t.Error("Not Found 'key'")
		}

		// Let the TTL expire
		time.Sleep(3 * time.Second)

		ctx, _ = context.WithTimeout(context.Background(), 3*time.Second)
		gr, err := kv.Get(ctx, "key")
		if err != nil {
			t.Error(err.Error())
		}
		if len(gr.Kvs) == 1 {
			t.Error("Cannot lease 'key'")
		}
	})
}
