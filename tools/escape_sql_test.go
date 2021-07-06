package tools

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEscapeSQLParam(t *testing.T) {
	assert.Equal(t, "'\\\\ss\\\\'", EscapeSQLParam("\\ss\\"))
}
