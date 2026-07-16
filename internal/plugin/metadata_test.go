package plugin

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetMetadata_ValidMetric(t *testing.T) {
	g := newTestPlugin(t, nil) // datasource not exercised by GetMetadata
	m := metricWith(t, `{"metrics":{"query":"avg:cpu{*}"}}`)
	md := g.GetMetadata(m)
	assert.Equal(t, "metrics", md["source"])
}

func TestGetMetadata_InvalidMetric(t *testing.T) {
	g := newTestPlugin(t, nil)
	m := metricWith(t, `{}`) // no source configured → ParseConfig error
	md := g.GetMetadata(m)
	assert.Empty(t, md)
}
