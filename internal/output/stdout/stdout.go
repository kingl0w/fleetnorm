// package stdout writes events as JSON, one per line. it is what confirms the
// pipeline works before there is an endpoint to point at.
package stdout

import (
	"context"
	"encoding/json"
	"io"
	"sync"

	"github.com/ianfrushon/fleetnorm/internal/event"
)

type Output struct {
	name string
	mu   sync.Mutex //one event per line, whoever is writing
	enc  *json.Encoder
}

func New(name string, w io.Writer) *Output {
	return &Output{name: name, enc: json.NewEncoder(w)}
}

func (o *Output) Name() string { return o.name }

func (o *Output) Send(ctx context.Context, e event.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.enc.Encode(e) //Encode appends the newline
}
