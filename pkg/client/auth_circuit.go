/*
Copyright 2026 Nutanix

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

package client

import (
	"sync"
	"time"
)

const (
	// DefaultAuthFailureThreshold is the number of consecutive 401s after which
	// Prism Central calls for a cluster are paused.
	DefaultAuthFailureThreshold = 3
	// DefaultAuthBackoffBaseDelay is the delay after the first authentication failure.
	DefaultAuthBackoffBaseDelay = 30 * time.Second
	// DefaultAuthBackoffMaxDelay caps exponential backoff between Prism Central auth retries.
	DefaultAuthBackoffMaxDelay = 30 * time.Minute
)

// AuthCircuitConfig configures the per-cluster Prism Central authentication circuit breaker.
type AuthCircuitConfig struct {
	FailureThreshold int
	BaseDelay        time.Duration
	MaxDelay         time.Duration
	Now              func() time.Time
}

// AuthCircuitBreaker pauses Prism Central calls for a cluster after repeated 401s.
// It is keyed by NutanixCluster namespaced name and is shared across controllers
// in the process so one deleting cluster cannot lock a shared Prism Central account.
type AuthCircuitBreaker struct {
	mu      sync.Mutex
	entries map[string]*authCircuitState
	cfg     AuthCircuitConfig
}

type authCircuitState struct {
	failures     int
	fingerprint  string
	retryAfter   time.Time
	openNotified bool
}

// PrismAuthCircuit is the process-wide circuit used by CAPX reconcilers.
var PrismAuthCircuit = NewAuthCircuitBreaker(AuthCircuitConfig{})

// NewAuthCircuitBreaker returns a circuit breaker with the given config, filling defaults.
func NewAuthCircuitBreaker(cfg AuthCircuitConfig) *AuthCircuitBreaker {
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = DefaultAuthFailureThreshold
	}
	if cfg.BaseDelay <= 0 {
		cfg.BaseDelay = DefaultAuthBackoffBaseDelay
	}
	if cfg.MaxDelay <= 0 {
		cfg.MaxDelay = DefaultAuthBackoffMaxDelay
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &AuthCircuitBreaker{
		entries: make(map[string]*authCircuitState),
		cfg:     cfg,
	}
}

// Allow reports whether a Prism Central call may proceed for key.
// If not allowed, the remaining backoff delay is returned.
// A changed credential fingerprint resets the circuit so updated secrets take effect immediately.
func (b *AuthCircuitBreaker) Allow(key, fingerprint string) (time.Duration, bool) {
	if b == nil || key == "" {
		return 0, true
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	state, ok := b.entries[key]
	if !ok {
		return 0, true
	}
	if fingerprintChanged(state.fingerprint, fingerprint) {
		delete(b.entries, key)
		return 0, true
	}
	now := b.cfg.Now()
	if now.Before(state.retryAfter) {
		return state.retryAfter.Sub(now), false
	}
	return 0, true
}

// RecordFailure records an authentication failure and returns the backoff delay until the next attempt.
func (b *AuthCircuitBreaker) RecordFailure(key, fingerprint string) time.Duration {
	if b == nil || key == "" {
		return DefaultAuthBackoffBaseDelay
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	state, ok := b.entries[key]
	if !ok || fingerprintChanged(state.fingerprint, fingerprint) {
		state = &authCircuitState{}
		b.entries[key] = state
	}
	state.fingerprint = fingerprint
	state.failures++
	delay := b.backoffDelay(state.failures)
	state.retryAfter = b.cfg.Now().Add(delay)
	return delay
}

// FailureCount returns the consecutive authentication failures recorded for key.
func (b *AuthCircuitBreaker) FailureCount(key string) int {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	state, ok := b.entries[key]
	if !ok {
		return 0
	}
	return state.failures
}

// ShouldWarnLockout is true once consecutive failures have reached the pause threshold.
func (b *AuthCircuitBreaker) ShouldWarnLockout(key string) bool {
	if b == nil {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	state, ok := b.entries[key]
	if !ok {
		return false
	}
	if state.failures < b.cfg.FailureThreshold {
		return false
	}
	if state.openNotified {
		return false
	}
	state.openNotified = true
	return true
}

// RecordSuccess clears the circuit for key after a successful Prism Central call.
func (b *AuthCircuitBreaker) RecordSuccess(key string) {
	if b == nil || key == "" {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.entries, key)
}

// Reset removes all entries. Intended for tests.
func (b *AuthCircuitBreaker) Reset() {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.entries = make(map[string]*authCircuitState)
}

func (b *AuthCircuitBreaker) backoffDelay(failures int) time.Duration {
	if failures <= 0 {
		return b.cfg.BaseDelay
	}
	delay := b.cfg.BaseDelay
	for i := 1; i < failures; i++ {
		if delay > b.cfg.MaxDelay/2 {
			return b.cfg.MaxDelay
		}
		delay *= 2
	}
	if delay > b.cfg.MaxDelay {
		return b.cfg.MaxDelay
	}
	return delay
}

func fingerprintChanged(previous, current string) bool {
	return previous != "" && current != "" && previous != current
}
