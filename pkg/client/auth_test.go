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
	"errors"
	"fmt"
	"testing"

	"github.com/nutanix-cloud-native/prism-go-client/converged"
	"github.com/nutanix-cloud-native/prism-go-client/environment/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeOpenAPIError struct {
	Status string
	Body   []byte
}

func (e *fakeOpenAPIError) Error() string {
	return e.Status
}

func TestIsUnauthorizedError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil",
			err:  nil,
			want: false,
		},
		{
			name: "unrelated",
			err:  errors.New("connection refused"),
			want: false,
		},
		{
			name: "uuid containing 401 is not treated as unauthorized",
			err:  errors.New("vm 401abc-def not found"),
			want: false,
		},
		{
			name: "invalid nutanix credentials",
			err:  errors.New("invalid Nutanix credentials"),
			want: true,
		},
		{
			name: "wrapped invalid credentials",
			err:  fmt.Errorf("nutanix prism v3 Client error: %w", errors.New("invalid Nutanix credentials")),
			want: true,
		},
		{
			name: "401 unauthorized string",
			err:  errors.New("GET /api/nutanix/v3/vms: 401 Unauthorized"),
			want: true,
		},
		{
			name: "openapi status 401",
			err:  &fakeOpenAPIError{Status: "401 Unauthorized", Body: []byte(`{"error_code":"401"}`)},
			want: true,
		},
		{
			name: "converged APIError wrapping 401",
			err: &converged.APIError{
				Cause:   &fakeOpenAPIError{Status: "401 Unauthorized"},
				Message: "authentication failed",
			},
			want: true,
		},
		{
			name: "wrapped converged APIError",
			err: fmt.Errorf("error finding VM: %w", &converged.APIError{
				Cause: &fakeOpenAPIError{Status: "401 Unauthorized"},
			}),
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, IsUnauthorizedError(tt.err))
		})
	}
}

func TestCredentialFingerprint(t *testing.T) {
	t.Parallel()

	assert.Empty(t, CredentialFingerprint(nil))

	a := &types.ManagementEndpoint{ApiCredentials: types.ApiCredentials{Username: "admin", Password: "old"}}
	b := &types.ManagementEndpoint{ApiCredentials: types.ApiCredentials{Username: "admin", Password: "new"}}
	same := &types.ManagementEndpoint{ApiCredentials: types.ApiCredentials{Username: "admin", Password: "old"}}

	require.NotEmpty(t, CredentialFingerprint(a))
	assert.Equal(t, CredentialFingerprint(a), CredentialFingerprint(same))
	assert.NotEqual(t, CredentialFingerprint(a), CredentialFingerprint(b))
}
