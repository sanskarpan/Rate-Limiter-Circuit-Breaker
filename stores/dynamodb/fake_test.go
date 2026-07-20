package dynamodb

import (
	"context"
	"strconv"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// fakeDDB is an in-process implementation of the API interface. It models the
// DynamoDB single-item operations the store relies on, including:
//
//   - conditional PutItem (ConditionExpression) with attribute_not_exists / a
//     "#exp < :now" / "#ver = :ver" clause — enough to drive SetNX and the OCC
//     loops without a live AWS endpoint
//   - UpdateItem "ADD #cnt :d" atomic counter
//   - GetItem / DeleteItem
//
// It supports only the specific expression shapes the store emits; that is
// deliberate — the fake exists to exercise the store's control flow, not to be a
// general DynamoDB. It is safe for concurrent use.
type fakeDDB struct {
	mu    sync.Mutex
	items map[string]map[string]ddbtypes.AttributeValue
	pk    string

	failConn bool
}

func newFakeDDB(pk string) *fakeDDB {
	return &fakeDDB{items: map[string]map[string]ddbtypes.AttributeValue{}, pk: pk}
}

type fakeErr struct{ msg string }

func (e *fakeErr) Error() string { return e.msg }

func (f *fakeDDB) keyOf(m map[string]ddbtypes.AttributeValue) string {
	v, ok := m[f.pk]
	if !ok {
		return ""
	}
	s, _ := v.(*ddbtypes.AttributeValueMemberS)
	if s == nil {
		return ""
	}
	return s.Value
}

func (f *fakeDDB) GetItem(_ context.Context, in *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failConn {
		return nil, &fakeErr{"fake: connection refused"}
	}
	k := f.keyOf(in.Key)
	it, ok := f.items[k]
	if !ok {
		return &dynamodb.GetItemOutput{}, nil
	}
	return &dynamodb.GetItemOutput{Item: cloneItem(it)}, nil
}

func (f *fakeDDB) PutItem(_ context.Context, in *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failConn {
		return nil, &fakeErr{"fake: connection refused"}
	}
	k := f.keyOf(in.Item)
	if in.ConditionExpression != nil {
		if !f.evalCondition(*in.ConditionExpression, f.items[k], in.ExpressionAttributeNames, in.ExpressionAttributeValues) {
			return nil, &ddbtypes.ConditionalCheckFailedException{}
		}
	}
	f.items[k] = cloneItem(in.Item)
	return &dynamodb.PutItemOutput{}, nil
}

func (f *fakeDDB) UpdateItem(_ context.Context, in *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failConn {
		return nil, &fakeErr{"fake: connection refused"}
	}
	k := f.keyOf(in.Key)
	it, ok := f.items[k]
	if !ok {
		it = cloneItem(in.Key)
		f.items[k] = it
	}
	// Only "ADD #cnt :d" is supported.
	expr := ""
	if in.UpdateExpression != nil {
		expr = *in.UpdateExpression
	}
	if !strings.HasPrefix(strings.TrimSpace(expr), "ADD") {
		return nil, &fakeErr{"fake: unsupported update expression: " + expr}
	}
	deltaAttr := in.ExpressionAttributeValues[":d"].(*ddbtypes.AttributeValueMemberN)
	delta, _ := strconv.ParseInt(deltaAttr.Value, 10, 64)
	cur := int64(0)
	if cv, ok := it[attrCnt].(*ddbtypes.AttributeValueMemberN); ok {
		cur, _ = strconv.ParseInt(cv.Value, 10, 64)
	}
	newVal := cur + delta
	it[attrCnt] = &ddbtypes.AttributeValueMemberN{Value: strconv.FormatInt(newVal, 10)}
	return &dynamodb.UpdateItemOutput{
		Attributes: map[string]ddbtypes.AttributeValue{
			attrCnt: &ddbtypes.AttributeValueMemberN{Value: strconv.FormatInt(newVal, 10)},
		},
	}, nil
}

func (f *fakeDDB) DeleteItem(_ context.Context, in *dynamodb.DeleteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failConn {
		return nil, &fakeErr{"fake: connection refused"}
	}
	delete(f.items, f.keyOf(in.Key))
	return &dynamodb.DeleteItemOutput{}, nil
}

// evalCondition supports exactly the two condition shapes the store emits:
//
//	"attribute_not_exists(#pk) OR #exp < :now"
//	"#ver = :ver"
func (f *fakeDDB) evalCondition(
	cond string,
	existing map[string]ddbtypes.AttributeValue,
	names map[string]string,
	values map[string]ddbtypes.AttributeValue,
) bool {
	cond = strings.TrimSpace(cond)
	switch {
	case strings.HasPrefix(cond, "attribute_not_exists"):
		if existing == nil {
			return true
		}
		// OR #exp < :now — treat a past exp as absent.
		expAttr, hasExp := existing[attrExp].(*ddbtypes.AttributeValueMemberN)
		nowAttr, hasNow := values[":now"].(*ddbtypes.AttributeValueMemberN)
		if hasExp && hasNow {
			exp, _ := strconv.ParseInt(expAttr.Value, 10, 64)
			now, _ := strconv.ParseInt(nowAttr.Value, 10, 64)
			return exp < now
		}
		return false
	case strings.Contains(cond, "#ver = :ver"):
		if existing == nil {
			return false
		}
		cur, ok := existing[attrVer].(*ddbtypes.AttributeValueMemberN)
		want, ok2 := values[":ver"].(*ddbtypes.AttributeValueMemberN)
		if !ok || !ok2 {
			return false
		}
		return cur.Value == want.Value
	default:
		return false
	}
}

func cloneItem(in map[string]ddbtypes.AttributeValue) map[string]ddbtypes.AttributeValue {
	out := make(map[string]ddbtypes.AttributeValue, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func newTestStore(f *fakeDDB) *DynamoDB {
	s, err := NewFromAPI(f, Options{TableName: "rl", KeyPrefix: "t:", PartitionKey: f.pk})
	if err != nil {
		panic(err)
	}
	return s
}
