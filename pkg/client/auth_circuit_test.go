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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuthCircuitBreakerBackoff(t *testing.T) {
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	breaker := NewAuthCircuitBreaker(AuthCircuitConfig{
		FailureThreshold: 3,
		BaseDelay:        30 * time.Second,
		MaxDelay:         4 * time.Minute,
		Now:              func() time.Time { return now },
	})

	const key = "default/cluster"

	delay, allowed := breaker.Allow(key, "fp1")
	assert.True(t, allowed)
	assert.Zero(t, delay)

	delay = breaker.RecordFailure(key, "fp1")
	assert.Equal(t, 30*time.Second, delay)
	assert.Equal(t, 1, breaker.FailureCount(key))
	assert.False(t, breaker.ShouldWarnLockout(key))

	delay, allowed = breaker.Allow(key, "fp1")
	assert.False(t, allowed)
	assert.Equal(t, 30*time.Second, delay)

	now = now.Add(30 * time.Second)
	delay, allowed = breaker.Allow(key, "fp1")
	assert.True(t, allowed)
	assert.Zero(t, delay)

	delay = breaker.RecordFailure(key, "fp1")
	assert.Equal(t, 60*time.Second, delay)
	delay = breaker.RecordFailure(key, "fp1")
	assert.Equal(t, 2*time.Minute, delay)
	assert.True(t, breaker.ShouldWarnLockout(key))
	assert.False(t, breaker.ShouldWarnLockout(key), "lockout warning should fire once")

	delay = breaker.RecordFailure(key, "fp1")
	assert.Equal(t, 4*time.Minute, delay, "delay should cap at max")
}

func TestAuthCircuitBreakerResetOnFingerprintChange(t *testing.T) {
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	breaker := NewAuthCircuitBreaker(AuthCircuitConfig{
		BaseDelay: 30 * time.Second,
		MaxDelay:  30 * time.Minute,
		Now:       func() time.Time { return now },
	})

	const key = "default/cluster"
	breaker.RecordFailure(key, "old")
	_, allowed := breaker.Allow(key, "old")
	require.False(t, allowed)

	_, allowed = breaker.Allow(key, "new")
	assert.True(t, allowed, "updated credentials should close the circuit")
	assert.Equal(t, 0, breaker.FailureCount(key))
}

func TestAuthCircuitBreakerRecordSuccess(t *testing.T) {
	breaker := NewAuthCircuitBreaker(AuthCircuitConfig{
		Now: func() time.Time { return time.Unix(0, 0) },
	})
	const key = "default/cluster"
	breaker.RecordFailure(key, "fp")
	breaker.RecordSuccess(key)
	_, allowed := breaker.Allow(key, "fp")
	assert.True(t, allowed)
}

func TestAuthCircuitBreakerSharedAcrossKeysIndependently(t *testing.T) {
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	breaker := NewAuthCircuitBreaker(AuthCircuitConfig{
		BaseDelay: 30 * time.Second,
		Now:       func() time.Time { return now },
	})

	breaker.RecordFailure("ns/a", "fp")
	_, allowedA := breaker.Allow("ns/a", "fp")
	_, allowedB := breaker.Allow("ns/b", "fp")
	assert.False(t, allowedA)
	assert.True(t, allowedB)
}

func TestAuthCircuitBreakerReset(t *testing.T) {
	breaker := NewAuthCircuitBreaker(AuthCircuitConfig{
		Now: func() time.Time { return time.Unix(0, 0) },
	})
	breaker.RecordFailure("ns/a", "fp")
	breaker.Reset()
	_, allowed := breaker.Allow("ns/a", "fp")
	assert.True(t, allowed)
}
