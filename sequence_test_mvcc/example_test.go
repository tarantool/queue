// Setup queue module and start Tarantool instance before execution:
// Terminal 1:
// $ make deps
// $ TEST_TNT_LISTEN=3013 tarantool queue/config.lua
//
// Terminal 2:
// $ cd queue
// $ go test -v example_test.go
package queue_test

import (
	"fmt"
	"time"

	"github.com/tarantool/go-tarantool"
	"github.com/tarantool/go-tarantool/queue"
)

func multiple_put(testData_1 string, q queue.Queue) {
	var err error
	for i := 0; i < 100; i++ {
		if _, err = q.Put(testData_1 + fmt.Sprintf("%d", i)); err != nil {
			fmt.Printf("error in put is %v", err)
			return
		}
	}
}

// Example demonstrates an operations like Put and Take with queue.
func Example_simpleQueue() {
	cfg := queue.Cfg{
		Temporary: false,
		Kind:      queue.FIFO,
	}
	opts := tarantool.Opts{
		Timeout: 2500 * time.Millisecond,
		User:    "test",
		Pass:    "test",
	}

	conn, err := tarantool.Connect("127.0.0.1:3013", opts)
	if err != nil {
		fmt.Printf("error in prepare is %v", err)
		return
	}
	defer conn.Close()

	q := queue.New(conn, "test_queue")
	if err := q.Create(cfg); err != nil {
		fmt.Printf("error in queue is %v", err)
		return
	}

	defer q.Drop()

	go multiple_put("test_put", q)

	// Output: data_1:  test_data_1
}
