package txn

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type Program struct {
	node       NodeInterface
	logger     *slog.Logger
	storageMtx *sync.Mutex
	storage    map[int64]int64
}

type workload struct {
	MessageBody
	Txn []Transaction
}

type baseResponse struct {
	Type string `json:"type"`
}
type txnResponse struct {
	Type string        `json:"type"`
	Txn  []Transaction `json:"txn"`
}

var wg sync.WaitGroup

func New(n *MaelstromNodeWrapper, l *slog.Logger) *Program {
	return &Program{
		node:       n,
		logger:     l,
		storageMtx: &sync.Mutex{},
		storage:    map[int64]int64{},
	}
}

func (p *Program) GetHandle(rpc_type string) HandlerFunc {
	return func(msg Message) error {
		ctx, span := otel.Tracer("txn").Start(
			context.Background(),
			fmt.Sprintf("handle_%s", rpc_type),
			trace.WithSpanKind(trace.SpanKindServer),
		)
		defer span.End()

		var body workload
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			span.SetStatus(codes.Error, err.Error())
			span.RecordError(err)
			return err
		}

		span.SetAttributes(
			attribute.Int("msg_id", body.MsgID),
			attribute.String("type", body.Type),
		)

		var resp any
		switch rpc_type {
		case "init":
			resp = p.handleInit(ctx)
		case "txn":
			resp = p.handleTxn(ctx, body)
		}

		span.SetStatus(codes.Ok, codes.Ok.String())
		return p.node.Reply(msg, resp)
	}
}

func (p *Program) handleInit(ctx context.Context) baseResponse {
	return baseResponse{Type: "init_ok"}
}

func (p *Program) handleTxn(ctx context.Context, body workload) txnResponse {
	resp := txnResponse{
		Type: "txn_ok",
		Txn:  make([]Transaction, len(body.Txn)),
	}

	span := trace.SpanFromContext(ctx)
	defer span.End()

	p.storageMtx.Lock()
	defer p.storageMtx.Unlock()

	for i, t := range body.Txn {
		resp.Txn[i] = t

		p.logger.With(
			slog.Any("trace_id", span.SpanContext().TraceID()),
			slog.Any("span_id", span.SpanContext().SpanID()),
		).Debug("handleTxn: processing item", slog.Any("txn", t))

		switch t.Op {
		case "r":
			v := p.storage[t.Key]
			resp.Txn[i].Val = &v
		case "w":
			p.storage[t.Key] = *t.Val
		}
	}

	span.SetAttributes(
		attribute.Int("count", len(body.Txn)),
	)
	span.SetStatus(codes.Ok, codes.Ok.String())

	return resp
}

func (p *Program) Shutdown() {
	// close(p.storageChan)
	wg.Wait()
}

func (p *Program) periodicJobs(dur time.Duration, f func()) func() {
	ticker := time.NewTicker(dur)
	return func() {
		for {
			<-ticker.C
			f()
		}
	}
}
