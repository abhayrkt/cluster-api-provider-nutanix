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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"

	"github.com/nutanix-cloud-native/prism-go-client/converged"
	v4converged "github.com/nutanix-cloud-native/prism-go-client/converged/v4"
	"github.com/nutanix-cloud-native/prism-go-client/environment/types"

	"github.com/nutanix-cloud-native/cluster-api-provider-nutanix/api/v1beta1"
)

const (
	unauthorizedStatusPrefix = "401"
	invalidCredentialsMsg    = "invalid nutanix credentials"
	unauthorizedToken        = "unauthorized"
	httpUnauthorizedToken    = "401 unauthorized"
)

// CredentialFingerprint returns a stable hash of the Prism Central credentials
// so the auth circuit can reset when the secret is updated.
func CredentialFingerprint(endpoint *types.ManagementEndpoint) string {
	if endpoint == nil {
		return ""
	}
	sum := sha256.Sum256([]byte(endpoint.Username + "\x00" + endpoint.Password + "\x00" + endpoint.APIKey))
	return hex.EncodeToString(sum[:])
}

// DeleteCachedClients drops cached Prism clients for the cluster so the next
// attempt rebuilds them from the current credentials secret.
func DeleteCachedClients(cluster *v1beta1.NutanixCluster) {
	if cluster == nil {
		return
	}
	params := &CacheParams{NutanixCluster: cluster}
	NutanixClientCache.Delete(params)
	NutanixClientCacheV4.Delete(params)
	NutanixConvergedClientV4Cache.Delete(params)
}

// IsUnauthorizedError reports whether err represents an HTTP 401 / invalid Prism Central credentials failure.
func IsUnauthorizedError(err error) bool {
	if err == nil {
		return false
	}

	var apiErr *converged.APIError
	if errors.As(err, &apiErr) && apiErr != nil {
		if unauthorizedFromMessage(apiErr.Error()) {
			return true
		}
		if isUnauthorizedStatus(apiErr.Cause) {
			return true
		}
	}

	for e := err; e != nil; e = errors.Unwrap(e) {
		if unauthorizedFromMessage(e.Error()) || isUnauthorizedStatus(e) {
			return true
		}
	}
	return false
}

func isUnauthorizedStatus(err error) bool {
	if err == nil {
		return false
	}
	status, _ := v4converged.GetStatusAndBody(err)
	return strings.HasPrefix(status, unauthorizedStatusPrefix)
}

func unauthorizedFromMessage(msg string) bool {
	m := strings.ToLower(msg)
	switch {
	case strings.Contains(m, invalidCredentialsMsg):
		return true
	case strings.Contains(m, httpUnauthorizedToken):
		return true
	case strings.Contains(m, unauthorizedToken) && strings.Contains(m, unauthorizedStatusPrefix):
		return true
	default:
		return false
	}
}
