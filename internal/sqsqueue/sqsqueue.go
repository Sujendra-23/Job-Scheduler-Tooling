// Package sqsqueue is an Amazon SQS-backed replacement for the in-process
// priority broker queue. Priority is expressed as one SQS queue per tier
// (workers poll tiers highest-first), every tier queue redrives to a shared
// dead-letter queue after MaxReceiveCount deliveries, and the queue-level
// VisibilityTimeout is what returns a batch to the queue when the worker that
// holds it dies without acknowledging it.
//
// Delivery is at-least-once: a message is only removed by an explicit Delete
// (the ack). Callers must therefore make completion idempotent; see
// internal/dispatch.SQSCoordinator.
package sqsqueue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"scheduler/internal/queue"
)

const (
	MinTier = 1
	MaxTier = 5
)

// Config describes the queue topology.
type Config struct {
	Endpoint string // e.g. http://localhost:4566 for LocalStack; empty = real AWS
	Region   string
	Prefix   string // queue names are <prefix>-p1..p5 and <prefix>-dlq
	// VisibilityTimeout is how long a received message stays hidden. If the
	// holder has not deleted it by then, SQS makes it receivable again.
	VisibilityTimeout time.Duration
	// MaxReceiveCount is how many times a message may be received before SQS
	// moves it to the DLQ instead of delivering it again.
	MaxReceiveCount int
}

func (c *Config) defaults() {
	if c.Prefix == "" {
		c.Prefix = "sched"
	}
	if c.Region == "" {
		c.Region = "us-east-1"
	}
	if c.VisibilityTimeout <= 0 {
		c.VisibilityTimeout = 30 * time.Second
	}
	if c.MaxReceiveCount <= 0 {
		c.MaxReceiveCount = 3
	}
}

// Queues is a handle on the per-tier queues plus the DLQ.
type Queues struct {
	client  *sqs.Client
	cfg     Config
	tierURL map[int]string
	dlqURL  string
}

// Delivery is one received message. It stays invisible to other consumers for
// the visibility timeout; Delete acks it, Release returns it immediately.
type Delivery struct {
	Job          *queue.Job
	Tier         int
	ReceiveCount int // 1 on first delivery, >1 means redelivered
	MessageID    string

	queueURL string
	receipt  string
}

// DeadLetter is a message observed in the DLQ.
type DeadLetter struct {
	Job          *queue.Job
	MessageID    string
	ReceiveCount int
}

func newClient(ctx context.Context, cfg Config) (*sqs.Client, error) {
	opts := []func(*config.LoadOptions) error{config.WithRegion(cfg.Region)}
	if cfg.Endpoint != "" {
		opts = append(opts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("local", "local", "")))
	}
	awsCfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return sqs.NewFromConfig(awsCfg, func(o *sqs.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
	}), nil
}

// TierQueueName / DLQName expose the naming convention.
func TierQueueName(prefix string, tier int) string { return fmt.Sprintf("%s-p%d", prefix, tier) }
func DLQName(prefix string) string                 { return prefix + "-dlq" }

// Tier clamps a job priority into the supported tier range.
func Tier(priority int) int {
	if priority < MinTier {
		return MinTier
	}
	if priority > MaxTier {
		return MaxTier
	}
	return priority
}

// Create makes the DLQ and the five tier queues (idempotent when the
// attributes match) and wires each tier's redrive policy to the DLQ.
func Create(ctx context.Context, cfg Config) (*Queues, error) {
	cfg.defaults()
	client, err := newClient(ctx, cfg)
	if err != nil {
		return nil, err
	}
	q := &Queues{client: client, cfg: cfg, tierURL: map[int]string{}}

	dlq, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(DLQName(cfg.Prefix))})
	if err != nil {
		return nil, fmt.Errorf("create DLQ: %w", err)
	}
	q.dlqURL = *dlq.QueueUrl
	attrs, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: dlq.QueueUrl, AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	if err != nil {
		return nil, fmt.Errorf("get DLQ arn: %w", err)
	}
	redrive, _ := json.Marshal(map[string]any{
		"deadLetterTargetArn": attrs.Attributes[string(types.QueueAttributeNameQueueArn)],
		"maxReceiveCount":     cfg.MaxReceiveCount,
	})
	for tier := MinTier; tier <= MaxTier; tier++ {
		out, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
			QueueName: aws.String(TierQueueName(cfg.Prefix, tier)),
			Attributes: map[string]string{
				string(types.QueueAttributeNameVisibilityTimeout): strconv.Itoa(int(cfg.VisibilityTimeout.Seconds())),
				string(types.QueueAttributeNameRedrivePolicy):     string(redrive),
			},
		})
		if err != nil {
			return nil, fmt.Errorf("create tier %d queue: %w", tier, err)
		}
		q.tierURL[tier] = *out.QueueUrl
	}
	return q, nil
}

// Open attaches to queues that Create already made (used by worker processes).
func Open(ctx context.Context, cfg Config) (*Queues, error) {
	cfg.defaults()
	client, err := newClient(ctx, cfg)
	if err != nil {
		return nil, err
	}
	q := &Queues{client: client, cfg: cfg, tierURL: map[int]string{}}
	d, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(DLQName(cfg.Prefix))})
	if err != nil {
		return nil, fmt.Errorf("open DLQ: %w", err)
	}
	q.dlqURL = *d.QueueUrl
	for tier := MinTier; tier <= MaxTier; tier++ {
		out, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(TierQueueName(cfg.Prefix, tier))})
		if err != nil {
			return nil, fmt.Errorf("open tier %d queue: %w", tier, err)
		}
		q.tierURL[tier] = *out.QueueUrl
	}
	return q, nil
}

// Delete removes all the queues (test/demo cleanup).
func (q *Queues) Delete(ctx context.Context) {
	for _, u := range q.tierURL {
		_, _ = q.client.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: aws.String(u)})
	}
	_, _ = q.client.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: aws.String(q.dlqURL)})
}

// Send enqueues a job on the queue for its priority tier.
func (q *Queues) Send(ctx context.Context, job *queue.Job) error {
	body, err := json.Marshal(job)
	if err != nil {
		return err
	}
	_, err = q.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl: aws.String(q.tierURL[Tier(job.Priority)]), MessageBody: aws.String(string(body)),
	})
	return err
}

// Receive sweeps the tiers highest-first and returns the first message found,
// or (nil, nil) when every tier is empty. Strict priority: a lower tier is
// only served when all higher tiers are empty at the moment of the sweep.
func (q *Queues) Receive(ctx context.Context) (*Delivery, error) {
	for tier := MaxTier; tier >= MinTier; tier-- {
		out, err := q.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:                    aws.String(q.tierURL[tier]),
			MaxNumberOfMessages:         1,
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameApproximateReceiveCount},
		})
		if err != nil {
			return nil, err
		}
		if len(out.Messages) == 0 {
			continue
		}
		m := out.Messages[0]
		var job queue.Job
		if err := json.Unmarshal([]byte(aws.ToString(m.Body)), &job); err != nil {
			return nil, fmt.Errorf("undecodable message %s: %w", aws.ToString(m.MessageId), err)
		}
		rc, _ := strconv.Atoi(m.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)])
		return &Delivery{
			Job: &job, Tier: tier, ReceiveCount: rc, MessageID: aws.ToString(m.MessageId),
			queueURL: q.tierURL[tier], receipt: aws.ToString(m.ReceiptHandle),
		}, nil
	}
	return nil, nil
}

// Ack permanently removes a delivered message. Without it the message is
// redelivered once the visibility timeout lapses.
func (q *Queues) Ack(ctx context.Context, d *Delivery) error {
	_, err := q.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(d.queueURL), ReceiptHandle: aws.String(d.receipt)})
	return err
}

// Release makes a delivered message receivable again immediately (used for a
// triaged retry) without waiting for the visibility timeout.
func (q *Queues) Release(ctx context.Context, d *Delivery) error {
	_, err := q.client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl: aws.String(d.queueURL), ReceiptHandle: aws.String(d.receipt), VisibilityTimeout: 0,
	})
	return err
}

// PeekDeadLetters lists DLQ messages without hiding them from other peeks
// (visibility 0), so the DLQ keeps its contents for inspection/redrive.
func (q *Queues) PeekDeadLetters(ctx context.Context) ([]DeadLetter, error) {
	out, err := q.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl: aws.String(q.dlqURL), MaxNumberOfMessages: 10, VisibilityTimeout: 0,
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameApproximateReceiveCount},
	})
	if err != nil {
		return nil, err
	}
	var res []DeadLetter
	for _, m := range out.Messages {
		var job queue.Job
		if err := json.Unmarshal([]byte(aws.ToString(m.Body)), &job); err != nil {
			return nil, errors.New("undecodable DLQ message " + aws.ToString(m.MessageId))
		}
		rc, _ := strconv.Atoi(m.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)])
		res = append(res, DeadLetter{Job: &job, MessageID: aws.ToString(m.MessageId), ReceiveCount: rc})
	}
	return res, nil
}

// Config returns the effective configuration.
func (q *Queues) Config() Config { return q.cfg }
