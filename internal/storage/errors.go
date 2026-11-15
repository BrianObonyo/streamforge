package storage

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// HTTPError is a non-200 reply from ClickHouse, with its native error code parsed
// out so callers can react to the CAUSE, not just the status.
type HTTPError struct {
	Status  int    // HTTP status
	Code    int    // ClickHouse exception code (0 if absent), e.g. 60 = UNKNOWN_TABLE
	Message string // server text, truncated
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("clickhouse: HTTP %d (code %d): %s", e.Status, e.Code, e.Message)
}

// Retryable reports whether trying the SAME request again could succeed: server
// overload/failure (5xx), throttling (429) and timeouts (408). Other 4xx replies
// mean the request itself is wrong (bad SQL, missing table, auth) and will fail
// identically forever.
func (e *HTTPError) Retryable() bool {
	return e.Status == 408 || e.Status == 429 || e.Status >= 500
}

// ClickHouse exception codes we handle specially.
const (
	chUnknownTable    = 60
	chUnknownDatabase = 81
)

var codeRe = regexp.MustCompile(`Code:\s*(\d+)`)

func newHTTPError(status int, body []byte) *HTTPError {
	msg := strings.TrimSpace(string(body))
	e := &HTTPError{Status: status}
	if m := codeRe.FindStringSubmatch(msg); m != nil {
		e.Code, _ = strconv.Atoi(m[1])
	}
	if len(msg) > 512 {
		msg = msg[:512] + "…"
	}
	e.Message = msg
	return e
}

// isUnknownObject reports whether err means "table or database does not exist".
func isUnknownObject(err error) bool {
	var he *HTTPError
	return errors.As(err, &he) && (he.Code == chUnknownTable || he.Code == chUnknownDatabase)
}
