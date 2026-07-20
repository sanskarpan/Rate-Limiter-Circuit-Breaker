//go:build integration

package dynamodb_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awsddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	ddb "github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/stores/dynamodb"
)

// These tests run only under `-tags=integration` against DynamoDB Local:
//
//	docker run -p 8000:8000 amazon/dynamodb-local
//	DYNAMODB_ENDPOINT=http://localhost:8000 AWS_REGION=us-east-1 \
//	  AWS_ACCESS_KEY_ID=x AWS_SECRET_ACCESS_KEY=x \
//	  go test -tags=integration ./dynamodb/...
//
// The test creates the table on demand.
const tableName = "rl_itest"

func newLiveClient(t *testing.T) *awsddb.Client {
	t.Helper()
	endpoint := os.Getenv("DYNAMODB_ENDPOINT")
	if endpoint == "" {
		t.Skip("DYNAMODB_ENDPOINT not set; skipping DynamoDB Local integration test")
	}
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion("us-east-1"),
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	client := awsddb.NewFromConfig(cfg, func(o *awsddb.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
	ensureTable(t, client)
	return client
}

func ensureTable(t *testing.T, client *awsddb.Client) {
	t.Helper()
	ctx := context.Background()
	_, err := client.DescribeTable(ctx, &awsddb.DescribeTableInput{TableName: aws.String(tableName)})
	if err == nil {
		return
	}
	_, err = client.CreateTable(ctx, &awsddb.CreateTableInput{
		TableName: aws.String(tableName),
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: ddbtypes.KeyTypeHash},
		},
		BillingMode: ddbtypes.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	time.Sleep(500 * time.Millisecond)
}

func TestIntegrationGetSetIncr(t *testing.T) {
	ctx := context.Background()
	s, err := ddb.New(newLiveClient(t), ddb.Options{TableName: tableName, KeyPrefix: "itest:"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	_ = s.Del(ctx, "basic", "ctr")
	if err := s.Set(ctx, "basic", "hello", time.Minute); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(ctx, "basic")
	if err != nil || got != "hello" {
		t.Fatalf("Get: %q %v", got, err)
	}
	v, err := s.IncrBy(ctx, "ctr", 4, time.Minute)
	if err != nil || v != 4 {
		t.Fatalf("IncrBy: %d %v", v, err)
	}
	v, err = s.IncrBy(ctx, "ctr", 3, time.Minute)
	if err != nil || v != 7 {
		t.Fatalf("IncrBy add: %d %v", v, err)
	}
}
