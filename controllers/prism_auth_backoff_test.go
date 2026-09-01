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

package controllers

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	v1beta1conditions "sigs.k8s.io/cluster-api/util/deprecated/v1beta1/conditions" //nolint:staticcheck // suppress complaining on Deprecated package

	infrav1 "github.com/nutanix-cloud-native/cluster-api-provider-nutanix/api/v1beta1"
	nutanixclient "github.com/nutanix-cloud-native/cluster-api-provider-nutanix/pkg/client"
)

func TestIsObjectOrClusterDeleting(t *testing.T) {
	t.Parallel()

	now := metav1.Now()
	obj := &infrav1.NutanixMachine{ObjectMeta: metav1.ObjectMeta{DeletionTimestamp: &now}}
	cluster := &infrav1.NutanixCluster{ObjectMeta: metav1.ObjectMeta{DeletionTimestamp: &now}}
	liveCluster := &infrav1.NutanixCluster{ObjectMeta: metav1.ObjectMeta{Name: "live"}}
	liveMachine := &infrav1.NutanixMachine{ObjectMeta: metav1.ObjectMeta{Name: "live"}}

	assert.True(t, isObjectOrClusterDeleting(obj, liveCluster))
	assert.True(t, isObjectOrClusterDeleting(liveMachine, cluster))
	assert.False(t, isObjectOrClusterDeleting(liveMachine, liveCluster))
	assert.False(t, isObjectOrClusterDeleting(nil, nil))
}

func TestSkipPrismCallsDueToAuthBackoff(t *testing.T) {
	nutanixclient.PrismAuthCircuit.Reset()
	t.Cleanup(nutanixclient.PrismAuthCircuit.Reset)

	ctx := context.Background()
	cluster := &infrav1.NutanixCluster{ObjectMeta: metav1.ObjectMeta{Name: "workload", Namespace: "ns"}}
	recorder := events.NewFakeRecorder(8)

	result, paused := skipPrismCallsDueToAuthBackoff(ctx, recorder, cluster, cluster, true, nil, nil)
	assert.False(t, paused)
	assert.Zero(t, result)

	result, paused = skipPrismCallsDueToAuthBackoff(ctx, recorder, cluster, cluster, false, nil, nil)
	assert.False(t, paused)
	assert.Zero(t, result)

	nutanixclient.PrismAuthCircuit.RecordFailure(cluster.GetNamespacedName(), "")
	result, paused = skipPrismCallsDueToAuthBackoff(ctx, recorder, cluster, cluster, true, nil, nil)
	require.True(t, paused)
	assert.Greater(t, result.RequeueAfter, time.Duration(0))
	assert.Equal(t, infrav1.PrismCentralAuthenticationFailed, v1beta1conditions.GetReason(cluster, infrav1.PrismCentralClientCondition))
	require.NotEmpty(t, recorder.Events)
	assert.Contains(t, <-recorder.Events, corev1.EventTypeWarning)
}

func TestRequeueUnauthorizedPrismError(t *testing.T) {
	nutanixclient.PrismAuthCircuit.Reset()
	t.Cleanup(nutanixclient.PrismAuthCircuit.Reset)

	ctx := context.Background()
	cluster := &infrav1.NutanixCluster{ObjectMeta: metav1.ObjectMeta{Name: "workload", Namespace: "ns"}}
	recorder := events.NewFakeRecorder(8)
	authErr := errors.New("invalid Nutanix credentials")

	result, handled := requeueUnauthorizedPrismError(ctx, recorder, cluster, cluster, false, authErr, nil, nil)
	assert.False(t, handled)
	assert.Zero(t, result)

	result, handled = requeueUnauthorizedPrismError(ctx, recorder, cluster, cluster, true, errors.New("connection refused"), nil, nil)
	assert.False(t, handled)
	assert.Zero(t, result)

	result, handled = requeueUnauthorizedPrismError(ctx, recorder, cluster, cluster, true, authErr, nil, nil)
	require.True(t, handled)
	assert.Equal(t, 30*time.Second, result.RequeueAfter)
	assert.Equal(t, 1, nutanixclient.PrismAuthCircuit.FailureCount(cluster.GetNamespacedName()))
	assert.Equal(t, infrav1.PrismCentralAuthenticationFailed, v1beta1conditions.GetReason(cluster, infrav1.PrismCentralClientCondition))
	require.NotEmpty(t, recorder.Events)

	result, paused := skipPrismCallsDueToAuthBackoff(ctx, recorder, cluster, cluster, true, nil, nil)
	require.True(t, paused)
	assert.Greater(t, result.RequeueAfter, time.Duration(0))
}

func TestRequeueUnauthorizedPrismErrorWarnsAfterThreshold(t *testing.T) {
	nutanixclient.PrismAuthCircuit.Reset()
	t.Cleanup(nutanixclient.PrismAuthCircuit.Reset)

	ctx := context.Background()
	cluster := &infrav1.NutanixCluster{ObjectMeta: metav1.ObjectMeta{Name: "workload", Namespace: "ns"}}
	recorder := events.NewFakeRecorder(8)
	authErr := errors.New("401 Unauthorized")

	for i := 0; i < nutanixclient.DefaultAuthFailureThreshold; i++ {
		_, handled := requeueUnauthorizedPrismError(ctx, recorder, cluster, cluster, true, authErr, nil, nil)
		require.True(t, handled)
	}

	foundLockoutMsg := false
	for len(recorder.Events) > 0 {
		ev := <-recorder.Events
		if strings.Contains(ev, "pausing reconciliation") && strings.Contains(ev, "locking") {
			foundLockoutMsg = true
		}
	}
	assert.True(t, foundLockoutMsg)
}
