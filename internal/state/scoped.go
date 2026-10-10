// Package state provides provider-owned stores and a small cross-service index.
package state

import (
	"errors"
	"regexp"
	"sync"
)

const Global = "global"

// Scope sentinels.
var (
	errScopeIdentity = errors.New("an explicit project and location are required")
	errScopeGlobal   = errors.New("this service uses a global scope")
)

type Scope struct {
	Project  string `json:"project"`
	Location string `json:"location"`
}

var identifier = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]*$`)

func (s Scope) Validate() error {
	if !identifier.MatchString(s.Project) || !identifier.MatchString(s.Location) {
		return errScopeIdentity
	}
	return nil
}
func GlobalScope(s Scope) error {
	if err := s.Validate(); err != nil {
		return err
	}
	if s.Location != Global {
		return errScopeGlobal
	}
	return nil
}

type entry[T any] struct {
	mu    sync.Mutex
	value T
}

// Store serializes access per scope. A reset waits for in-flight callbacks and
// replaces only that scope. Callbacks must not retain references to their value,
// re-enter this store, or run backend operations; copy mutable values on reads.
type Store[T any] struct {
	mu       sync.RWMutex
	entries  map[Scope]*entry[T]
	newValue func() T
	validate func(Scope) error
}

func NewStore[T any](newValue func() T, validate func(Scope) error) *Store[T] {
	return &Store[T]{entries: map[Scope]*entry[T]{}, newValue: newValue, validate: validate}
}
func (s *Store[T]) check(scope Scope) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	if s.validate != nil {
		return s.validate(scope)
	}
	return nil
}
func (s *Store[T]) With(scope Scope, use func(*T) error) error {
	if err := s.check(scope); err != nil {
		return err
	}
	s.mu.RLock()
	value := s.entries[scope]
	if value == nil {
		s.mu.RUnlock()
		s.mu.Lock()
		if s.entries[scope] == nil {
			s.entries[scope] = &entry[T]{value: s.newValue()}
		}
		s.mu.Unlock()
		s.mu.RLock()
		value = s.entries[scope]
	}
	defer s.mu.RUnlock()
	value.mu.Lock()
	defer value.mu.Unlock()
	return use(&value.value)
}
func (s *Store[T]) Reset(scope Scope) error {
	if err := s.check(scope); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if value := s.entries[scope]; value != nil {
		value.value = s.newValue()
	}
	return nil
}
