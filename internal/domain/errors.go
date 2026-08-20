package domain

import "errors"

var (
	ErrConflict      = errors.New("conflict")
	ErrInvalid       = errors.New("invalid input")
	ErrInvariant     = errors.New("invariant violation")
	ErrNotFound      = errors.New("not found")
	ErrScopeMismatch = errors.New("tenant or user scope mismatch")
	ErrUnavailable   = errors.New("unavailable")
)
