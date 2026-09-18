// package output is the contract every fleetnorm destination satisfies. one
// Send is one attempt; whether to try again is the pipeline's decision.
package output

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ianfrushon/fleetnorm/internal/event"
)

type Output interface {
	//as routing rules refer to it
	Name() string

	//one attempt, safe for concurrent use. nil means delivered. wrap
	//ErrPermanent when another attempt cannot help.
	Send(ctx context.Context, e event.Event) error
}

// ErrPermanent marks a failure retrying cannot fix. the pipeline checks it with
// errors.Is and gives up.
var ErrPermanent = errors.New("permanent failure")

// DelayError carries a delay the destination asked for. it wraps the underlying
// error, so errors.Is still sees what is beneath.
type DelayError struct {
	Delay time.Duration
	Err   error
}

func (e *DelayError) Error() string { return fmt.Sprintf("%v (retry after %v)", e.Err, e.Delay) }
func (e *DelayError) Unwrap() error { return e.Err }

// RetryAfter reports the delay a destination asked for, if any.
func RetryAfter(err error) (time.Duration, bool) {
	var d *DelayError
	if errors.As(err, &d) {
		return d.Delay, true
	}
	return 0, false
}
