package apperrors

import "errors"

// ErrNotFound is returned when a saga cannot be found by the requested key.
var ErrNotFound = errors.New("saga not found")
