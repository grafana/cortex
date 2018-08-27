package batch

import (
	"fmt"

	promchunk "github.com/weaveworks/cortex/pkg/prom1/storage/local/chunk"
)

// batchStream deals with iteratoring through multiple, non-overlapping batches,
// and building new slices of non-overlapping batches.  Designed to be used
// without allocations.
type batchStream []promchunk.Batch

func (bs batchStream) Print() {
	fmt.Println("[")
	for _, b := range bs {
		print(b)
	}
	fmt.Println("]")
}

// append, reset, hasNext, next, atTime etc are all inlined in go1.11.
// append isn't a pointer receiver as that was causing bs to escape to the heap.
func (bs batchStream) append(t int64, v float64) batchStream {
	l := len(bs)
	if l == 0 || bs[l-1].Index == promchunk.BatchSize {
		bs = append(bs, promchunk.Batch{})
		l++
	}
	b := &bs[l-1]
	b.Timestamps[b.Index] = t
	b.Values[b.Index] = v
	b.Index++
	b.Length++
	return bs
}

func (bs *batchStream) reset() {
	for i := range *bs {
		(*bs)[i].Index = 0
	}
}

func (bs *batchStream) hasNext() bool {
	return len(*bs) > 0
}

func (bs *batchStream) next() {
	(*bs)[0].Index++
	if (*bs)[0].Index >= (*bs)[0].Length {
		*bs = (*bs)[1:]
	}
}

func (bs *batchStream) atTime() int64 {
	return (*bs)[0].Timestamps[(*bs)[0].Index]
}

func (bs *batchStream) at() (int64, float64) {
	b := &(*bs)[0]
	return b.Timestamps[b.Index], b.Values[b.Index]
}

// mergeBatches assumes the contents of batches are overlapping and unstorted.
// Merge them together into a sorted, non-overlapping stream in result.
// Caller must guarantee result is big enough.  Return value will always be a
// slice pointing to the same underly array as result, allowing mergeBatches
// to call itself recursively.
func mergeBatches(batches batchStream, result batchStream) batchStream {
	switch len(batches) {
	case 0:
		return nil
	case 1:
		copy(result[:1], batches)
		return result[:1]
	case 2:
		return mergeStreams(batches[0:1], batches[1:2], result)
	default:
		n := len(batches) / 2
		left := mergeBatches(batches[n:], result[n:])
		right := mergeBatches(batches[:n], result[:n])

		batches = mergeStreams(left, right, batches)
		result = result[:len(batches)]
		copy(result, batches)

		return result[:len(batches)]
	}
}

func mergeStreams(left, right batchStream, result batchStream) batchStream {
	result.reset()
	result = result[:0]
	for left.hasNext() && right.hasNext() {
		t1, t2 := left.atTime(), right.atTime()
		if t1 < t2 {
			result = result.append(left.at())
			left.next()
		} else if t1 > t2 {
			result = result.append(right.at())
			right.next()
		} else {
			result = result.append(left.at())
			left.next()
			right.next()
		}
	}
	for ; left.hasNext(); left.next() {
		result = result.append(left.at())
	}
	for ; right.hasNext(); right.next() {
		result = result.append(right.at())
	}
	result.reset()
	return result
}
