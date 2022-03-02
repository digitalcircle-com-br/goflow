package goflow

import (
	"context"
	"errors"
	"sync"
	"time"
)

//Series executes all functions provided in series. In case one of them returns error
// the flow is halted, returning that error.
func Series(ctx context.Context, f ...func(ctx context.Context) error) error {
	ch := ctx.Done()
	nctx, cancel := context.WithCancel(ctx)
	done := false
	go func() {
		<-ch
		done = true
	}()
	for _, v := range f {
		if done {
			cancel()
			return nil
		}
		err := v(nctx)
		if err != nil {
			cancel()
			return err
		}
	}
	cancel()
	return nil
}

//Series executes all functions provided in series. In case one of them returns error
// the flow is halted, returning that error.
func SeriesTO(ctx context.Context, to time.Duration, f ...func(ctx context.Context) error) error {
	nctx, cancel := context.WithCancel(ctx)
	doneCh := make(chan error)
	go func() {
		var ret error
		defer func() {
			r := recover()
			if r != nil {
				ret = r.(error)
			}
			doneCh <- ret
			close(doneCh)
		}()
		ret = Series(nctx, f...)
	}()
	select {
	case <-time.After(to):
		cancel()
		return errors.New("timeout in execution")
	case ret := <-doneCh:
		cancel()
		return ret
	}
}

//Parallel will start all functions in parallel, returns errors.
func Parallel(ctx context.Context, f ...func(ctx context.Context) error) (err error) {
	errCh := make(chan error)
	nctx, cancel := context.WithCancel(ctx)

	wg := sync.WaitGroup{}

	for _, v := range f {
		wg.Add(1)
		go func(af func(ctx context.Context) error) {
			defer wg.Done()
			err := af(nctx)
			if err != nil {
				errCh <- err
				close(errCh)
				errCh = nil
				cancel()
			}
		}(v)
	}

	go func() {
		wg.Wait()
		cancel()
	}()

	select {
	case <-nctx.Done():
		cancel()
		return nil
	case err := <-errCh:
		cancel()
		return err
	}
}

//Parallel will start all functions in parallel, returns errors.
func ParallelMax(ctx context.Context, i int, f ...func(ctx context.Context) error) (err error) {
	if i < 1 {
		i = 1
	}
	errCh := make(chan error)
	nctx, cancel := context.WithCancel(ctx)
	ctlCh := make(chan struct{}, i)
	wg := sync.WaitGroup{}
	done := false
	go func() {
		<-ctx.Done()
		done = true
	}()

	for _, v := range f {
		ctlCh <- struct{}{}
		if done {
			close(ctlCh)
			ctlCh = nil
			break
		}
		wg.Add(1)
		go func(af func(ctx context.Context) error) {
			defer func() {
				wg.Done()
				<-ctlCh
			}()
			err := af(nctx)
			if err != nil {
				errCh <- err
				close(errCh)
				errCh = nil
				cancel()
				done = true
			}
		}(v)
	}

	go func() {
		wg.Wait()
		cancel()
	}()

	select {
	case <-nctx.Done():
		cancel()
		return nil
	case err := <-errCh:
		cancel()
		return err
	}
}

func WaterFall[T any](ctx context.Context, at T, f ...func(ctx context.Context, at T) (T, error)) (ret T, err error) {
	ret = at
	done := false
	nctx, cancel := context.WithCancel(ctx)
	go func() {
		<-nctx.Done()
		done = true
	}()
	for _, af := range f {
		if done {
			cancel()
			return
		}
		ret, err = af(ctx, ret)
		if err != nil {
			cancel()
			return
		}
	}
	cancel()
	return
}
