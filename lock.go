package lock

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/bytedance/sonic"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"github.com/google/uuid"
)

// Lock implements a distributed lock using Elasticsearch.
// The use case of this lock is improving efficiency (not correctness)
type Lock struct {
	client          *elasticsearch.Client
	indexName       string
	typeName        string
	lastTTL         time.Duration
	ID              string    `json:"-"`
	Owner           string    `json:"owner"`
	Acquired        time.Time `json:"acquired"`
	Expires         time.Time `json:"expires"`
	isAcquired      bool
	isReleased      bool
	keepAliveActive bool
	mutex           *sync.Mutex
}

var (
	clientID = uuid.New().String()
)

// NewLock create a new lock identified by a string
func NewLock(client *elasticsearch.Client, id string) *Lock {
	return &Lock{
		client:          client,
		ID:              id,
		Owner:           clientID,
		indexName:       "distributed-locks",
		typeName:        "lock",
		isAcquired:      false,
		isReleased:      false,
		keepAliveActive: false,
		mutex:           &sync.Mutex{},
	}
}

// WithOwner is a shortcut method to set the owner manually.
// If you don't specify an owner, a random UUID is used automatically.
func (lock *Lock) WithOwner(owner string) *Lock {
	lock.Owner = owner
	return lock
}

// Acquire tries to acquire a lock with a TTL.
// Returns nil when succesful or error otherwise.
func (lock *Lock) Acquire(ctx context.Context, ttl time.Duration) error {
	lock.mutex.Lock()
	defer lock.mutex.Unlock()
	lock.lastTTL = ttl
	lock.Acquired = time.Now()
	lock.Expires = lock.Acquired.Add(ttl)
	// This script ensures that the owner is the same so that a single process can renew a named lock over again.
	// In case the lock is expired, another process can take over.
	script := `
	if (ctx._source.owner != params.owner && ZonedDateTime.parse(ctx._source.expires).isAfter(ZonedDateTime.parse(params.now))) {
		ctx.op = "none";
	} else {
		ctx._source.expires = params.expires;
		if (ctx._source.owner != params.owner) {
			ctx._source.owner = params.owner;
			ctx._source.acquired = params.acquired;
		}
	}
	`
	params := map[string]interface{}{
		"now":      time.Now(),
		"owner":    lock.Owner,
		"expires":  lock.Expires,
		"acquired": lock.Acquired,
	}

	req := esapi.UpdateRequest{
		Index:      lock.indexName,
		DocumentID: lock.ID,
		Refresh:    "true",
		Body: esutil.NewJSONReader(map[string]interface{}{
			"script": map[string]interface{}{
				"source": script,
				"lang":   "painless",
				"params": params,
			},
			"upsert": map[string]interface{}{
				"owner":    lock.Owner,
				"expires":  lock.Expires,
				"acquired": lock.Acquired,
			},
		}),
	}

	res, err := req.Do(ctx, lock.client)
	if err != nil {
		return err
	}
	if res.StatusCode == http.StatusConflict {
		return fmt.Errorf("lock held by other client")
	}
	obj, err := parseResultObject(res)
	if err != nil {
		return err
	}
	if obj["result"] == "noop" {
		return fmt.Errorf("lock held by other client")
	}

	lock.isAcquired = true
	lock.isReleased = false
	return nil
}

// KeepAlive causes the lock to automatically extend its TTL to avoid expiration.
// This keep going until the context is cancelled, Release() is called, or the process dies.
// This calls Acquire again {beforeExpiry} before expirt.
// Don't use KeepAlive with very short TTLs, rather call Acquire yourself when you need to.
func (lock *Lock) KeepAlive(ctx context.Context, beforeExpiry time.Duration) error {
	lock.mutex.Lock()
	defer lock.mutex.Unlock()
	if !lock.isAcquired {
		return fmt.Errorf("acquire lock before keep alive")
	}
	if lock.keepAliveActive {
		return nil
	}
	if beforeExpiry >= lock.lastTTL {
		return fmt.Errorf("KeepAlive's beforeExpire (%v) should be smaller than lock's TTL (%v)", beforeExpiry, lock.lastTTL)
	}

	// Call Acquire {beforeExpiry} before lock expires
	timeLeft := lock.Expires.Add(-beforeExpiry).Sub(time.Now())
	if timeLeft <= 0 {
		timeLeft = 1 * time.Millisecond
	}
	lock.keepAliveActive = true
	time.AfterFunc(timeLeft, func() {
		lock.mutex.Lock()
		lock.keepAliveActive = false
		isReleased := lock.isReleased
		lock.mutex.Unlock()
		if !isReleased {
			lock.Acquire(ctx, lock.lastTTL)
			lock.KeepAlive(ctx, beforeExpiry)
		}
	})
	return nil
}

func (lock *Lock) release(errorIfNoop bool) error {
	lock.mutex.Lock()
	defer lock.mutex.Unlock()
	if lock.isReleased {
		if errorIfNoop {
			return fmt.Errorf("lock was already released")
		}
		return nil
	}
	ctx := context.Background()
	refresh := true
	req := esapi.DeleteByQueryRequest{
		Index:     []string{lock.indexName},
		Refresh:   &refresh,
		Conflicts: "proceed",
		Body: esutil.NewJSONReader(map[string]interface{}{
			"query": map[string]interface{}{
				"bool": map[string]interface{}{
					"must": []map[string]interface{}{
						{
							"term": map[string]interface{}{
								"_id": lock.ID,
							},
						},
						{
							"term": map[string]interface{}{
								"owner.keyword": lock.Owner,
							},
						},
					},
				},
			},
		}),
	}

	res, err := req.Do(ctx, lock.client)
	if err != nil {
		return err
	}
	lock.isReleased = true
	lock.isAcquired = false
	obj, err := parseResultObject(res)
	if err != nil {
		return err
	}
	deleted := int(obj["deleted"].(float64))
	if errorIfNoop && deleted == 0 {
		return fmt.Errorf("release had no effect (lock: %v, client: %v)", lock.ID, lock.Owner)
	}
	return nil
}

// Release removes the lock (if it is still held).
// The only case this errors is if there's a connection error with ES.
func (lock *Lock) Release() error {
	return lock.release(false)
}

// MustRelease removes the lock (if it is still held) but returns an error if the result was a noop.
func (lock *Lock) MustRelease() error {
	return lock.release(true)
}

// IsAcquired returns if lock is acquired and not expired
func (lock *Lock) IsAcquired() bool {
	lock.mutex.Lock()
	defer lock.mutex.Unlock()
	return lock.isAcquired && lock.Expires.After(time.Now())
}

// IsReleased returns if lock was released manually or is expired
func (lock *Lock) IsReleased() bool {
	lock.mutex.Lock()
	defer lock.mutex.Unlock()
	return lock.isReleased || lock.Expires.Before(time.Now())
}

func parseResultObject(res *esapi.Response) (map[string]interface{}, error) {
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	var result map[string]interface{}
	err = sonic.Unmarshal(body, &result)
	if err != nil {
		return nil, err
	}
	if result["error"] != nil {
		var obj struct {
			Error struct {
				Reason    string `json:"reason"`
				RootCause []struct {
					Reason string `json:"reason"`
				} `json:"root_cause"`
			} `json:"error"`
		}
		sonic.Unmarshal(body, &obj)
		reason := obj.Error.Reason
		if len(obj.Error.RootCause) == 0 {
			return nil, errors.New(result["error"].(string))
		}
		rootCause := obj.Error.RootCause[0].Reason
		message := fmt.Sprintf("%s: %s", reason, rootCause)
		return nil, errors.New(message)
	}
	return result, nil
}
