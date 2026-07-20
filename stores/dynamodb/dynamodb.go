// Package dynamodb provides a DynamoDB-backed implementation of the core
// store.Store interface (github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/store).
//
// Like the memcached backend, DynamoDB is a heavy, optional dependency and lives
// in this separate nested Go module (stores/) so the CORE library stays
// zero-dependency.
//
// # Data model
//
// Every store key maps to ONE DynamoDB item in a single table, addressed by a
// partition key (default attribute name "pk"). The item holds:
//
//   - pk   (S)  — the (prefixed) store key
//   - val  (S)  — the string payload for Get/Set/GetSet/SetNX and script state
//   - cnt  (N)  — the numeric counter used by IncrBy
//   - exp  (N)  — an epoch-seconds TTL attribute. Enable DynamoDB TTL on this
//     attribute so expired items are removed automatically. NOTE: DynamoDB TTL
//     deletion is asynchronous (typically within 48h), so reads additionally
//     honour exp themselves (an item whose exp is in the past is reported as
//     absent), giving prompt logical expiry independent of physical deletion.
//
// # Atomicity model
//
// DynamoDB has no server-side scripting, but every single-item write is
// conditional and atomic:
//
//   - IncrBy uses UpdateItem with an ADD action — a server-side atomic counter,
//     stronger than a client CAS loop and equivalent to Redis INCRBY.
//   - SetNX uses a PutItem with a attribute_not_exists condition.
//   - GetSet / the client-side "scripts" use UpdateItem with a ConditionExpression
//     on a monotonically increasing version attribute ("ver"), i.e. optimistic
//     concurrency control. On a ConditionalCheckFailedException the caller
//     re-reads and retries (bounded by maxOCCRetries, failing CLOSED on
//     exhaustion). This is single-ITEM atomic.
//
// # HONEST comparison vs Redis Lua
//
// The per-item OCC guarantee matches Redis for any algorithm whose state lives in
// ONE key/item — token bucket, GCRA, leaky bucket, fixed window, sliding-window-
// log. It is WEAKER for algorithms that need atomic access across MULTIPLE items
// (sliding-window-counter's current+previous windows; the distributed circuit
// breaker, which the Redis backend keeps in one hash but which would map to
// separate concerns here): DynamoDB TransactWriteItems could express some of
// these, but a read-compute-write decision cannot be done in a single
// transaction without an external read, so those scripts are deliberately
// unsupported (Eval returns ErrScriptUnsupported). Use Redis for them.
//
// Additional DynamoDB caveats vs Redis: eventual consistency on GSIs/replicas
// (this store uses strongly-consistent reads for the OCC loop to stay correct),
// higher latency, and per-request cost. The version attribute grows unbounded in
// principle but is a bounded-width integer per item and is reset with the item's
// TTL.
package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/store"
)

// Compile-time assertion that *DynamoDB satisfies the core Store interface.
var _ store.Store = (*DynamoDB)(nil)

// maxOCCRetries bounds the optimistic-concurrency retry loop for GetSet and the
// client-side scripts. On exhaustion the operation fails (fail-closed / deny).
const maxOCCRetries = 32

// Attribute names used in the item schema.
const (
	attrVal = "val" // string payload
	attrCnt = "cnt" // numeric counter (IncrBy)
	attrExp = "exp" // epoch-seconds TTL attribute
	attrVer = "ver" // OCC version
)

// API is the minimal subset of the DynamoDB client this store depends on.
// Depending on this small interface (rather than the concrete *dynamodb.Client)
// lets tests inject a fake with NO live AWS — see the fake in the tests. The real
// *dynamodb.Client satisfies it directly.
type API interface {
	GetItem(ctx context.Context, in *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	PutItem(ctx context.Context, in *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	UpdateItem(ctx context.Context, in *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
	DeleteItem(ctx context.Context, in *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
}

// Options configures the DynamoDB store.
type Options struct {
	// TableName is the DynamoDB table (required).
	TableName string

	// PartitionKey is the partition-key attribute name (default: "pk").
	PartitionKey string

	// KeyPrefix is prepended to every store key (default: "rl:").
	KeyPrefix string

	// Fallback is the store used when DynamoDB is unreachable. If nil, a fresh
	// per-process in-memory store is installed (see the fail-open warning on the
	// memcached and redis backends — the same trade-off applies).
	Fallback store.Store
}

func (o *Options) defaults() {
	if o.PartitionKey == "" {
		o.PartitionKey = "pk"
	}
	if o.KeyPrefix == "" {
		o.KeyPrefix = "rl:"
	}
	if o.Fallback == nil {
		o.Fallback = store.NewMemoryWithScripts()
	}
}

// DynamoDB is a Store implementation backed by a single DynamoDB table.
// All methods are safe for concurrent use.
type DynamoDB struct {
	api  API
	opts Options
}

// New creates a DynamoDB store from a real *dynamodb.Client.
func New(client *dynamodb.Client, opts Options) (*DynamoDB, error) {
	return NewFromAPI(client, opts)
}

// NewFromAPI creates a DynamoDB store from anything satisfying the API interface.
// This is the seam tests use to inject a fake DynamoDB.
func NewFromAPI(api API, opts Options) (*DynamoDB, error) {
	if opts.TableName == "" {
		return nil, errors.New("dynamodb: TableName is required")
	}
	opts.defaults()
	return &DynamoDB{api: api, opts: opts}, nil
}

func (d *DynamoDB) prefixed(key string) string {
	return d.opts.KeyPrefix + key
}

// keyAttr builds the primary-key attribute map for a store key.
func (d *DynamoDB) keyAttr(key string) map[string]ddbtypes.AttributeValue {
	return map[string]ddbtypes.AttributeValue{
		d.opts.PartitionKey: &ddbtypes.AttributeValueMemberS{Value: d.prefixed(key)},
	}
}

// expAt returns the epoch-seconds TTL attribute value for a TTL from now, or nil
// when ttl <= 0 (no expiry).
func expAt(ttl time.Duration) (int64, bool) {
	if ttl <= 0 {
		return 0, false
	}
	return time.Now().Add(ttl).Unix(), true
}

// isConnError reports whether err indicates DynamoDB is unreachable (as opposed
// to an application-level condition failure). Conditional-check failures and
// not-found are NOT connection errors.
func isConnError(err error) bool {
	if err == nil {
		return false
	}
	var cond *ddbtypes.ConditionalCheckFailedException
	if errors.As(err, &cond) {
		return false
	}
	var timeout interface{ Timeout() bool }
	if errors.As(err, &timeout) && timeout.Timeout() {
		return true
	}
	// smithy wraps transport failures; treat any operation error whose
	// underlying cause is a network/canceled failure as connectivity loss.
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var respErr interface{ HTTPStatusCode() int }
	if errors.As(err, &respErr) {
		// A structured API error (throttling, validation) is NOT a connection
		// error — surface it.
		return false
	}
	// No HTTP response at all → transport failure.
	return true
}

// isConditionalFailed reports whether err is a DynamoDB conditional-check
// failure (used for SetNX and the OCC loops).
func isConditionalFailed(err error) bool {
	var cond *ddbtypes.ConditionalCheckFailedException
	return errors.As(err, &cond)
}

// getItem reads the item for key with a strongly-consistent read, honouring the
// logical TTL (an item whose exp is in the past is reported as absent).
func (d *DynamoDB) getItem(ctx context.Context, key string) (map[string]ddbtypes.AttributeValue, error) {
	out, err := d.api.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(d.opts.TableName),
		Key:            d.keyAttr(key),
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return nil, err
	}
	if out.Item == nil {
		return nil, nil
	}
	if expired(out.Item) {
		return nil, nil
	}
	return out.Item, nil
}

// expired reports whether an item's exp attribute is in the past.
func expired(item map[string]ddbtypes.AttributeValue) bool {
	v, ok := item[attrExp]
	if !ok {
		return false
	}
	n, ok := v.(*ddbtypes.AttributeValueMemberN)
	if !ok {
		return false
	}
	exp, err := strconv.ParseInt(n.Value, 10, 64)
	if err != nil {
		return false
	}
	return time.Now().Unix() >= exp
}

func stringAttr(item map[string]ddbtypes.AttributeValue, name string) (string, bool) {
	v, ok := item[name]
	if !ok {
		return "", false
	}
	s, ok := v.(*ddbtypes.AttributeValueMemberS)
	if !ok {
		return "", false
	}
	return s.Value, true
}

func numAttr(item map[string]ddbtypes.AttributeValue, name string) (int64, bool) {
	v, ok := item[name]
	if !ok {
		return 0, false
	}
	n, ok := v.(*ddbtypes.AttributeValueMemberN)
	if !ok {
		return 0, false
	}
	x, err := strconv.ParseInt(n.Value, 10, 64)
	if err != nil {
		return 0, false
	}
	return x, true
}

// Get returns the value for key, or store.ErrNotFound if absent or expired.
func (d *DynamoDB) Get(ctx context.Context, key string) (string, error) {
	item, err := d.getItem(ctx, key)
	if err != nil {
		if isConnError(err) {
			return d.opts.Fallback.Get(ctx, key)
		}
		return "", fmt.Errorf("dynamodb get %q: %w", key, err)
	}
	if item == nil {
		return "", store.ErrNotFound
	}
	if s, ok := stringAttr(item, attrVal); ok {
		return s, nil
	}
	// Item exists but has only a counter (IncrBy). Report the numeric value.
	if n, ok := numAttr(item, attrCnt); ok {
		return strconv.FormatInt(n, 10), nil
	}
	return "", store.ErrNotFound
}

// putValue writes val (+ optional TTL) unconditionally, bumping the version.
func (d *DynamoDB) putValue(ctx context.Context, key, value string, ttl time.Duration) error {
	item := d.keyAttr(key)
	item[attrVal] = &ddbtypes.AttributeValueMemberS{Value: value}
	item[attrVer] = &ddbtypes.AttributeValueMemberN{Value: "1"}
	if exp, ok := expAt(ttl); ok {
		item[attrExp] = &ddbtypes.AttributeValueMemberN{Value: strconv.FormatInt(exp, 10)}
	}
	_, err := d.api.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(d.opts.TableName),
		Item:      item,
	})
	return err
}

// Set stores value for key with the given TTL.
func (d *DynamoDB) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	if err := d.putValue(ctx, key, value, ttl); err != nil {
		if isConnError(err) {
			return d.opts.Fallback.Set(ctx, key, value, ttl)
		}
		return fmt.Errorf("dynamodb set %q: %w", key, err)
	}
	return nil
}

// SetNX stores value only if key does not exist (or has logically expired).
// Uses a conditional PutItem (attribute_not_exists OR exp in the past).
func (d *DynamoDB) SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	item := d.keyAttr(key)
	item[attrVal] = &ddbtypes.AttributeValueMemberS{Value: value}
	item[attrVer] = &ddbtypes.AttributeValueMemberN{Value: "1"}
	if exp, ok := expAt(ttl); ok {
		item[attrExp] = &ddbtypes.AttributeValueMemberN{Value: strconv.FormatInt(exp, 10)}
	}
	now := strconv.FormatInt(time.Now().Unix(), 10)
	_, err := d.api.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           aws.String(d.opts.TableName),
		Item:                item,
		ConditionExpression: aws.String("attribute_not_exists(#pk) OR #exp < :now"),
		ExpressionAttributeNames: map[string]string{
			"#pk":  d.opts.PartitionKey,
			"#exp": attrExp,
		},
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":now": &ddbtypes.AttributeValueMemberN{Value: now},
		},
	})
	if err == nil {
		return true, nil
	}
	if isConditionalFailed(err) {
		return false, nil
	}
	if isConnError(err) {
		return d.opts.Fallback.SetNX(ctx, key, value, ttl)
	}
	return false, fmt.Errorf("dynamodb setnx %q: %w", key, err)
}

// GetSet atomically reads the current value and writes the new one via an OCC
// loop, returning the old value ("" if absent).
func (d *DynamoDB) GetSet(ctx context.Context, key, value string, ttl time.Duration) (string, error) {
	for i := 0; i < maxOCCRetries; i++ {
		item, err := d.getItem(ctx, key)
		if err != nil {
			if isConnError(err) {
				return d.opts.Fallback.GetSet(ctx, key, value, ttl)
			}
			return "", fmt.Errorf("dynamodb getset get %q: %w", key, err)
		}
		old := ""
		if item != nil {
			if s, ok := stringAttr(item, attrVal); ok {
				old = s
			}
		}
		wrote, err := d.putIfVersion(ctx, key, map[string]ddbtypes.AttributeValue{
			attrVal: &ddbtypes.AttributeValueMemberS{Value: value},
		}, item, ttl)
		if err != nil {
			if isConnError(err) {
				return d.opts.Fallback.GetSet(ctx, key, value, ttl)
			}
			return "", fmt.Errorf("dynamodb getset %q: %w", key, err)
		}
		if wrote {
			return old, nil
		}
		// Version conflict: retry.
	}
	return "", fmt.Errorf("dynamodb getset %q: %w", key, ErrOCCExhausted)
}

// IncrBy atomically increments the numeric counter of key by delta using a
// server-side ADD action. The TTL is applied only when the item is created.
func (d *DynamoDB) IncrBy(ctx context.Context, key string, delta int64, ttl time.Duration) (int64, error) {
	// First try to create the item with the initial counter + TTL, conditional on
	// absence, so the TTL is set exactly once (on creation).
	created, val, err := d.tryCreateCounter(ctx, key, delta, ttl)
	if err != nil {
		if isConnError(err) {
			return d.opts.Fallback.IncrBy(ctx, key, delta, ttl)
		}
		return 0, fmt.Errorf("dynamodb incrby create %q: %w", key, err)
	}
	if created {
		return val, nil
	}
	// Item exists: atomic ADD without touching the TTL.
	out, err := d.api.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:        aws.String(d.opts.TableName),
		Key:              d.keyAttr(key),
		UpdateExpression: aws.String("ADD #cnt :d"),
		ExpressionAttributeNames: map[string]string{
			"#cnt": attrCnt,
		},
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":d": &ddbtypes.AttributeValueMemberN{Value: strconv.FormatInt(delta, 10)},
		},
		ReturnValues: ddbtypes.ReturnValueUpdatedNew,
	})
	if err != nil {
		if isConnError(err) {
			return d.opts.Fallback.IncrBy(ctx, key, delta, ttl)
		}
		return 0, fmt.Errorf("dynamodb incrby %q: %w", key, err)
	}
	n, ok := numAttr(out.Attributes, attrCnt)
	if !ok {
		return 0, fmt.Errorf("dynamodb incrby %q: counter not returned", key)
	}
	return n, nil
}

// tryCreateCounter attempts to create the counter item (conditional on absence or
// logical expiry). Returns created=true and the initial value when it wins the
// creation race; created=false means the item already exists and the caller
// should ADD instead.
func (d *DynamoDB) tryCreateCounter(ctx context.Context, key string, delta int64, ttl time.Duration) (bool, int64, error) {
	item := d.keyAttr(key)
	item[attrCnt] = &ddbtypes.AttributeValueMemberN{Value: strconv.FormatInt(delta, 10)}
	if exp, ok := expAt(ttl); ok {
		item[attrExp] = &ddbtypes.AttributeValueMemberN{Value: strconv.FormatInt(exp, 10)}
	}
	now := strconv.FormatInt(time.Now().Unix(), 10)
	_, err := d.api.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           aws.String(d.opts.TableName),
		Item:                item,
		ConditionExpression: aws.String("attribute_not_exists(#pk) OR #exp < :now"),
		ExpressionAttributeNames: map[string]string{
			"#pk":  d.opts.PartitionKey,
			"#exp": attrExp,
		},
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":now": &ddbtypes.AttributeValueMemberN{Value: now},
		},
	})
	if err == nil {
		return true, delta, nil
	}
	if isConditionalFailed(err) {
		return false, 0, nil
	}
	return false, 0, err
}

// Del deletes one or more keys.
func (d *DynamoDB) Del(ctx context.Context, keys ...string) error {
	for _, k := range keys {
		_, err := d.api.DeleteItem(ctx, &dynamodb.DeleteItemInput{
			TableName: aws.String(d.opts.TableName),
			Key:       d.keyAttr(k),
		})
		if err != nil {
			if isConnError(err) {
				return d.opts.Fallback.Del(ctx, keys...)
			}
			return fmt.Errorf("dynamodb del %q: %w", k, err)
		}
	}
	return nil
}

// Ping verifies connectivity by issuing a lightweight GetItem for a sentinel key.
func (d *DynamoDB) Ping(ctx context.Context) error {
	_, err := d.api.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(d.opts.TableName),
		Key:       d.keyAttr("__ping__"),
	})
	return err
}

// Close closes the fallback store. The DynamoDB client holds no closable handle.
func (d *DynamoDB) Close() error {
	return d.opts.Fallback.Close()
}
