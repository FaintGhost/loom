#!/usr/bin/env bash
# Exercise the real contracts against deliberate regressions, using the same tool as the project check.
set -euo pipefail
: "${EFFECTGO_BIN:?set EFFECTGO_BIN to the reviewed effectgo executable}"
EFFECTGO_BIN="$(realpath "$EFFECTGO_BIN")"
: "${EFFECTGO_SHA256:?set EFFECTGO_SHA256 to the reviewed executable digest}"
actual_sha256="$(sha256sum "$EFFECTGO_BIN")"
[[ "${actual_sha256%% *}" == "$EFFECTGO_SHA256" ]] || { echo 'effectgo executable digest mismatch' >&2; exit 1; }
printf 'effectgo binary: %s\nsha256: %s\n' "$EFFECTGO_BIN" "$EFFECTGO_SHA256"
go version
go version -m "$EFFECTGO_BIN"
root="$(cd "$(dirname "$0")" && pwd)"
probe="$(mktemp -d)"
trap 'rm -rf "$probe"' EXIT
printf 'module example.com/loom-contracts\n\ngo 1.26.0\n' > "$probe/go.mod"
cp "$root/internal/model/report.go" "$probe/report.go"
(cd "$probe" && env -u EFFECTGO_STUBS "$EFFECTGO_BIN" ./...)
cat > "$probe/astype.go" <<'GO'
package model

import "errors"

type busyError struct{}
func (*busyError) Error() string { return "busy" }
func busy() error { return &busyError{} }

//effectgo:errors ErrTransition
func AsTypeNarrowing() error {
    err := busy()
    if _, ok := errors.AsType[*busyError](err); ok { return ErrTransition }
    return err
}

//effectgo:errors ErrTransition
func AsTypeNegated() error {
    err := busy()
    _, ok := errors.AsType[*busyError](err)
    if !ok { return err }
    return ErrTransition
}
GO
(cd "$probe" && env -u EFFECTGO_STUBS "$EFFECTGO_BIN" ./...)
printf 'Go 1.26 errors.AsType narrowing reproducer accepted.\n'
cat > "$probe/regressions.go" <<'GO'
package model

import (
    "errors"
    "time"
)

const UnexpectedState Status = "unexpected"
var ErrUnexpected = errors.New("unexpected")

//effectgo:pure
func ImpurePolicy() int64 { return time.Now().Unix() }

//effectgo:errors ErrTransition
func WrongError() error { return ErrUnexpected }

//effectgo:errors ErrTransition
func AsTypeMustRetainUnmatched() error {
    err := busy()
    if _, ok := errors.AsType[interface{ error; Timeout() bool }](err); ok { return ErrTransition }
    return err
}
GO
set +e
output="$(cd "$probe" && env -u EFFECTGO_STUBS "$EFFECTGO_BIN" ./... 2>&1)"
status=$?
set -e
printf '%s\n' "$output"
[[ "$status" == 3 ]] || { echo "expected effectgo diagnostics, got $status" >&2; exit 1; }
for expected in ImpurePolicy WrongError UnexpectedState AsTypeMustRetainUnmatched; do
    case "$output" in
        *"$expected"*) ;;
        *) echo "effectgo missed $expected" >&2; exit 1 ;;
    esac
done
printf 'Contract regressions detected: impure policy, unexpected error, unhandled state, unmatched AsType error.\n'
