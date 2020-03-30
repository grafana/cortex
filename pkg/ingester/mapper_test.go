package ingester

import (
	"sort"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
)

var (
	// cm11, cm12, cm13 are colliding with fp1.
	// cm21, cm22 are colliding with fp2.
	// cm31, cm32 are colliding with fp3, which is below maxMappedFP.
	// Note that fingerprints are set and not actually calculated.
	// The collision detection is independent from the actually used
	// fingerprinting algorithm.
	fp1  = model.Fingerprint(maxMappedFP + 1)
	fp2  = model.Fingerprint(maxMappedFP + 2)
	fp3  = model.Fingerprint(1)
	cm11 = labelPairs{
		{Name: "foo", Value: "bar"},
		{Name: "dings", Value: "bumms"},
	}
	cm12 = labelPairs{
		{Name: "bar", Value: "foo"},
	}
	cm13 = labelPairs{
		{Name: "foo", Value: "bar"},
	}
	cm21 = labelPairs{
		{Name: "foo", Value: "bumms"},
		{Name: "dings", Value: "bar"},
	}
	cm22 = labelPairs{
		{Name: "dings", Value: "foo"},
		{Name: "bar", Value: "bumms"},
	}
	cm31 = labelPairs{
		{Name: "bumms", Value: "dings"},
	}
	cm32 = labelPairs{
		{Name: "bumms", Value: "dings"},
		{Name: "bar", Value: "foo"},
	}
)

func (a labelPairs) copyValuesAndSort() labels.Labels {
	c := make(labels.Labels, len(a))
	for i, pair := range a {
		c[i].Name = pair.Name
		c[i].Value = pair.Value
	}
	sort.Sort(c)
	return c
}

func TestFPMapper(t *testing.T) {
	sm := NewSeriesMap()

	mapper := newFPMapper(sm)

	// Everything is empty, resolving a FP should do nothing.
	assertFingerprintEqual(t, mapper.mapFP(fp1, cm11), fp1)
	assertFingerprintEqual(t, mapper.mapFP(fp1, cm12), fp1)

	// cm11 is in sm. Adding cm11 should do nothing. Mapping cm12 should resolve
	// the collision.
	sm.Put(fp1, &MemorySeries{Metric: cm11.copyValuesAndSort()})
	assertFingerprintEqual(t, mapper.mapFP(fp1, cm11), fp1)
	assertFingerprintEqual(t, mapper.mapFP(fp1, cm12), model.Fingerprint(1))

	// The mapped cm12 is added to sm, too. That should not change the outcome.
	sm.Put(model.Fingerprint(1), &MemorySeries{Metric: cm12.copyValuesAndSort()})
	assertFingerprintEqual(t, mapper.mapFP(fp1, cm11), fp1)
	assertFingerprintEqual(t, mapper.mapFP(fp1, cm12), model.Fingerprint(1))

	// Now map cm13, should reproducibly result in the next mapped FP.
	assertFingerprintEqual(t, mapper.mapFP(fp1, cm13), model.Fingerprint(2))
	assertFingerprintEqual(t, mapper.mapFP(fp1, cm13), model.Fingerprint(2))

	// Add cm13 to sm. Should not change anything.
	sm.Put(model.Fingerprint(2), &MemorySeries{Metric: cm13.copyValuesAndSort()})
	assertFingerprintEqual(t, mapper.mapFP(fp1, cm11), fp1)
	assertFingerprintEqual(t, mapper.mapFP(fp1, cm12), model.Fingerprint(1))
	assertFingerprintEqual(t, mapper.mapFP(fp1, cm13), model.Fingerprint(2))

	// Now add cm21 and cm22 in the same way, checking the mapped FPs.
	assertFingerprintEqual(t, mapper.mapFP(fp2, cm21), fp2)
	sm.Put(fp2, &MemorySeries{Metric: cm21.copyValuesAndSort()})
	assertFingerprintEqual(t, mapper.mapFP(fp2, cm21), fp2)
	assertFingerprintEqual(t, mapper.mapFP(fp2, cm22), model.Fingerprint(3))
	sm.Put(model.Fingerprint(3), &MemorySeries{Metric: cm22.copyValuesAndSort()})
	assertFingerprintEqual(t, mapper.mapFP(fp2, cm21), fp2)
	assertFingerprintEqual(t, mapper.mapFP(fp2, cm22), model.Fingerprint(3))

	// Map cm31, resulting in a mapping straight away.
	assertFingerprintEqual(t, mapper.mapFP(fp3, cm31), model.Fingerprint(4))
	sm.Put(model.Fingerprint(4), &MemorySeries{Metric: cm31.copyValuesAndSort()})

	// Map cm32, which is now mapped for two reasons...
	assertFingerprintEqual(t, mapper.mapFP(fp3, cm32), model.Fingerprint(5))
	sm.Put(model.Fingerprint(5), &MemorySeries{Metric: cm32.copyValuesAndSort()})

	// Now check ALL the mappings, just to be sure.
	assertFingerprintEqual(t, mapper.mapFP(fp1, cm11), fp1)
	assertFingerprintEqual(t, mapper.mapFP(fp1, cm12), model.Fingerprint(1))
	assertFingerprintEqual(t, mapper.mapFP(fp1, cm13), model.Fingerprint(2))
	assertFingerprintEqual(t, mapper.mapFP(fp2, cm21), fp2)
	assertFingerprintEqual(t, mapper.mapFP(fp2, cm22), model.Fingerprint(3))
	assertFingerprintEqual(t, mapper.mapFP(fp3, cm31), model.Fingerprint(4))
	assertFingerprintEqual(t, mapper.mapFP(fp3, cm32), model.Fingerprint(5))

	// Remove all the fingerprints from sm, which should change nothing, as
	// the existing mappings stay and should be detected.
	sm.Del(fp1)
	sm.Del(fp2)
	sm.Del(fp3)
	assertFingerprintEqual(t, mapper.mapFP(fp1, cm11), fp1)
	assertFingerprintEqual(t, mapper.mapFP(fp1, cm12), model.Fingerprint(1))
	assertFingerprintEqual(t, mapper.mapFP(fp1, cm13), model.Fingerprint(2))
	assertFingerprintEqual(t, mapper.mapFP(fp2, cm21), fp2)
	assertFingerprintEqual(t, mapper.mapFP(fp2, cm22), model.Fingerprint(3))
	assertFingerprintEqual(t, mapper.mapFP(fp3, cm31), model.Fingerprint(4))
	assertFingerprintEqual(t, mapper.mapFP(fp3, cm32), model.Fingerprint(5))
}

// assertFingerprintEqual asserts that two fingerprints are equal.
func assertFingerprintEqual(t *testing.T, gotFP, wantFP model.Fingerprint) {
	if gotFP != wantFP {
		t.Errorf("got fingerprint %v, want fingerprint %v", gotFP, wantFP)
	}
}
