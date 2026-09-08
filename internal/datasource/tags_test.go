package datasource

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestJoinSearchQuery(t *testing.T) {
	assert.Equal(t, "service:x env:prod muted:false",
		JoinSearchQuery([]string{"service:x", "env:prod"}, "muted:false"))
	assert.Equal(t, "service:x", JoinSearchQuery([]string{"service:x"}, ""))
	assert.Equal(t, "muted:false", JoinSearchQuery(nil, "muted:false"))
}

func TestMergeScopeTags(t *testing.T) {
	tags := []string{"service:x", "env:prod"}
	cases := []struct{ in, want string }{
		{"avg:cpu{*}", "avg:cpu{service:x,env:prod}"},
		{"avg:cpu{}", "avg:cpu{service:x,env:prod}"},
		{"avg:cpu{team:a}", "avg:cpu{team:a,service:x,env:prod}"},
		{"avg:cpu{*} by {host}", "avg:cpu{service:x,env:prod} by {host}"},
		{"sum:hits{team:a}.as_count()", "sum:hits{team:a,service:x,env:prod}.as_count()"},
		{"avg:cpu", "avg:cpu{service:x,env:prod}"}, // no brace: appended
	}
	for _, c := range cases {
		assert.Equal(t, c.want, MergeScopeTags(c.in, tags), c.in)
	}
	// no tags: query unchanged
	assert.Equal(t, "avg:cpu{*}", MergeScopeTags("avg:cpu{*}", nil))
}
