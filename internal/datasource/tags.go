package datasource

import "strings"

// JoinSearchQuery space-joins tags with an optional extra query (monitor/slo).
func JoinSearchQuery(tags []string, extra string) string {
	parts := make([]string, 0, len(tags)+1)
	parts = append(parts, tags...)
	if strings.TrimSpace(extra) != "" {
		parts = append(parts, strings.TrimSpace(extra))
	}
	return strings.Join(parts, " ")
}

// AppendQueryFilter ANDs tags onto a query string (apm/logs).
func AppendQueryFilter(query string, tags []string) string {
	parts := make([]string, 0, len(tags)+1)
	if strings.TrimSpace(query) != "" {
		parts = append(parts, strings.TrimSpace(query))
	}
	parts = append(parts, tags...)
	return strings.Join(parts, " ")
}

// MergeScopeTags merges tags into the first {…} scope brace of a metric query,
// preserving any trailing `by {…}` grouping and function suffix. A query with no
// scope brace gets one appended after the metric name.
func MergeScopeTags(query string, tags []string) string {
	if len(tags) == 0 {
		return query
	}
	joined := strings.Join(tags, ",")

	open := strings.IndexByte(query, '{')
	if open < 0 {
		return query + "{" + joined + "}"
	}
	close := strings.IndexByte(query[open:], '}')
	if close < 0 {
		// malformed; leave as-is rather than corrupt the query.
		return query
	}
	close += open

	inner := strings.TrimSpace(query[open+1 : close])
	var merged string
	if inner == "" || inner == "*" {
		merged = joined
	} else {
		merged = inner + "," + joined
	}
	return query[:open+1] + merged + query[close:]
}
