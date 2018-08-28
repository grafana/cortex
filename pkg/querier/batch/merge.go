package batch

import (
	"container/heap"
	"sort"

	"github.com/weaveworks/cortex/pkg/chunk"
	promchunk "github.com/weaveworks/cortex/pkg/prom1/storage/local/chunk"
)

type mergeIterator struct {
	its []*nonOverlappingIterator
	h   iteratorHeap

	// Store the current sorted batchStream
	batches batchStream

	// For the next set of batches we'll collect, and buffers to merge them in.
	nextBatches    batchStream
	nextBatchesBuf batchStream
	batchesBuf     batchStream

	currErr error
}

func newMergeIterator(cs []chunk.Chunk) *mergeIterator {
	css := partitionChunks(cs)
	its := make([]*nonOverlappingIterator, 0, len(css))
	for _, cs := range css {
		its = append(its, newNonOverlappingIterator(cs))
	}

	c := &mergeIterator{
		its: its,
		h:   make(iteratorHeap, 0, len(its)),

		// TODO its 2x # iterators guaranteed to be enough?
		batches:        make(batchStream, 0, len(its)*2),
		nextBatches:    make(batchStream, 0, len(its)*2),
		nextBatchesBuf: make(batchStream, 0, len(its)*2),
		batchesBuf:     make(batchStream, 0, len(its)*2),
	}

	for _, iter := range c.its {
		if iter.Next(1) {
			c.h = append(c.h, iter)
			continue
		}

		if err := iter.Err(); err != nil {
			c.currErr = err
		}
	}

	heap.Init(&c.h)
	return c
}

func (c *mergeIterator) Seek(t int64, size int) bool {
	c.h = c.h[:0]
	c.batches = c.batches[:0]

	for _, iter := range c.its {
		if iter.Seek(t, size) {
			c.h = append(c.h, iter)
			continue
		}

		if err := iter.Err(); err != nil {
			c.currErr = err
			return false
		}
	}

	heap.Init(&c.h)
	return c.buildNextBatch(size)
}

func (c *mergeIterator) Next(size int) bool {
	// Pop the last built batch in a way that doesn't extend the slice.
	if len(c.batches) > 0 {
		copy(c.batches, c.batches[1:])
		c.batches = c.batches[:len(c.batches)-1]
	}

	return c.buildNextBatch(size)
}

func (c *mergeIterator) nextBatchEndTime() int64 {
	batch := &c.batches[0]
	return batch.Timestamps[batch.Length-1]
}

func (c *mergeIterator) buildNextBatch(size int) bool {
	// All we need to do is get enough batches that our first batch's last entry
	// is before all iterators next entry.
	// And all whilst dynamically increasing batch size.

	for len(c.h) > 0 && (len(c.batches) == 0 || c.nextBatchEndTime() >= c.h[0].AtTime()) {

		b := c.h[0].Batch()

		c.nextBatches = c.nextBatches[:0]
		c.nextBatches = append(c.nextBatches, b)
		c.nextBatchesBuf = mergeStreams(c.batches, c.nextBatches, c.nextBatchesBuf, size)
		copy(c.batches[:len(c.nextBatchesBuf)], c.nextBatchesBuf)
		c.batches = c.batches[:len(c.nextBatchesBuf)]

		if c.h[0].Next(size) {
			heap.Fix(&c.h, 0)
		} else {
			heap.Pop(&c.h)
		}
	}

	return len(c.batches) > 0
}

func (c *mergeIterator) AtTime() int64 {
	return c.batches[0].Timestamps[0]
}

func (c *mergeIterator) Batch() promchunk.Batch {
	return c.batches[0]
}

func (c *mergeIterator) Err() error {
	return c.currErr
}

type iteratorHeap []iterator

func (h *iteratorHeap) Len() int      { return len(*h) }
func (h *iteratorHeap) Swap(i, j int) { (*h)[i], (*h)[j] = (*h)[j], (*h)[i] }

func (h *iteratorHeap) Less(i, j int) bool {
	iT := (*h)[i].AtTime()
	jT := (*h)[j].AtTime()
	return iT < jT
}

func (h *iteratorHeap) Push(x interface{}) {
	*h = append(*h, x.(iterator))
}

func (h *iteratorHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// Build a list of lists of non-overlapping chunks.
func partitionChunks(cs []chunk.Chunk) [][]chunk.Chunk {
	sort.Sort(byFrom(cs))

	css := [][]chunk.Chunk{}
outer:
	for _, c := range cs {
		for i, cs := range css {
			if cs[len(cs)-1].Through.Before(c.From) {
				css[i] = append(css[i], c)
				continue outer
			}
		}
		cs := make([]chunk.Chunk, 0, len(cs)/(len(css)+1))
		cs = append(cs, c)
		css = append(css, cs)
	}

	return css
}

type byFrom []chunk.Chunk

func (b byFrom) Len() int           { return len(b) }
func (b byFrom) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b byFrom) Less(i, j int) bool { return b[i].From < b[j].From }
