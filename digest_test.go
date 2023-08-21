package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLinking(t *testing.T) {
	mep := LinkLogfile("./test/lapse.nerdvana-Feb-2021.log")
	assert.Equal(t, mep, nil)
}
