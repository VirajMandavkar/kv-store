package main

import (
	"fmt"
	"testing"
)

func TestBloomFilterCorrectness(t *testing.T) {
	n := 1000
	p := 0.01
	bf := NewBloomFilter(n, p)

	for i := 0; i < n; i++ {
		bf.Add([]byte(fmt.Sprintf("key_%d", i)))
	}

	for i := 0; i < n; i++ {
		if !bf.Exists([]byte(fmt.Sprintf("key_%d", i))) {
			t.Fatalf("FALSE NEGATIVE DETECTED: key_%d was inserted but reported missing", i)
		}
	}

	falsePositives := 0
	testCount := 1000

	for i := n; i < n+testCount; i++ {
		if bf.Exists([]byte(fmt.Sprintf("key_%d", i))) {
			falsePositives++
		}
	}

	actualRate := float64(falsePositives) / float64(testCount)
	t.Logf("Measured False Positive Rate: %.4f (Target: %.2f)", actualRate, p)

	if actualRate > 0.03 { // Allowing small statistical margin above 1%
		t.Fatalf("False positive rate too high: got %.4f, expected ~0.01", actualRate)
	}

}
