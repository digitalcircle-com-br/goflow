package goflow_test

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/digitalcircle-com-br/goflow"
)

func TestSeries(t *testing.T) {
	nctx, cancel := context.WithCancel(context.Background())
	a := func(c context.Context) error {
		log.Printf("A")
		return nil
	}
	b := func(c context.Context) error {
		log.Printf("B")
		return nil
	}
	c := func(c context.Context) error {
		log.Printf("C")
		cancel()
		return nil
	}
	//cancel()
	err := goflow.Series(nctx, a, b, c)
	if err != nil {
		log.Printf("%s", err.Error())
	} else {
		log.Printf("DONE")
	}

}

func TestSeriesTO(t *testing.T) {
	nctx, cancel := context.WithCancel(context.Background())
	a := func(c context.Context) error {
		log.Printf("A")
		return nil
	}
	b := func(c context.Context) error {
		log.Printf("B")
		time.Sleep(time.Second * time.Duration(3))
		return nil
	}
	c := func(c context.Context) error {
		log.Printf("C")
		cancel()
		return nil
	}
	d := func(c context.Context) error {
		log.Printf("D")
		cancel()
		return nil
	}
	//cancel()
	err := goflow.SeriesTO(nctx, time.Second, a, b, c, d)
	if err != nil {
		log.Printf("%s", err.Error())
	} else {
		log.Printf("DONE")
	}

}

func TestParallel(t *testing.T) {
	nctx, cancel := context.WithCancel(context.Background())
	a := func(c context.Context) error {
		log.Printf("A")
		cancel()
		return nil
	}
	b := func(c context.Context) error {
		time.Sleep(time.Second * time.Duration(1))
		log.Printf("B")
		return nil
	}
	c := func(c context.Context) error {
		log.Printf("C")
		return nil
	}
	d := func(c context.Context) error {
		time.Sleep(time.Second * time.Duration(2))
		log.Printf("D")
		return nil
	}
	//cancel()
	err := goflow.Parallel(nctx, a, b, c, d)
	if err != nil {
		log.Printf("%s", err.Error())
	} else {
		log.Printf("DONE")
	}

}

func TestParallelMax(t *testing.T) {
	nctx, cancel := context.WithCancel(context.Background())
	fns := make([]func(c context.Context) error, 0)
	nfns := 0
	for i := 0; i < 100; i++ {
		func(i int) {
			fns = append(fns, func(c context.Context) error {
				nfns++
				defer func() { nfns-- }()
				log.Printf("starting: %v - %v", i, nfns)
				return nil
			})
		}(i)
	}
	//cancel()
	err := goflow.ParallelMax(nctx, 5, fns...)
	cancel()
	if err != nil {
		log.Printf("%s", err.Error())
	} else {
		log.Printf("DONE")
	}

}

func TestWaterfall(t *testing.T) {
	ret, err := goflow.WaterFall(context.Background(), "<->",
		func(ctx context.Context, at string) (string, error) {
			return "F1:" + at, nil
		},
		func(ctx context.Context, at string) (string, error) {
			return "F2:" + at, nil
		},
		func(ctx context.Context, at string) (string, error) {
			return "F3:" + at, nil
		})
	if err != nil {
		log.Printf(err.Error())
		t.Fail()
	}
	log.Printf(ret)
}
