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
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v1 "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/tools/record"
	capiv1beta1 "sigs.k8s.io/cluster-api/api/core/v1beta1"                                 //nolint:staticcheck // suppress complaining on Deprecated package
	v1beta1conditions "sigs.k8s.io/cluster-api/util/deprecated/v1beta1/conditions"         //nolint:staticcheck // suppress complaining on Deprecated package
	v1beta2conditions "sigs.k8s.io/cluster-api/util/deprecated/v1beta1/conditions/v1beta2" //nolint:staticcheck // suppress complaining on Deprecated package
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	infrav1 "github.com/nutanix-cloud-native/cluster-api-provider-nutanix/api/v1beta1"
	nutanixclient "github.com/nutanix-cloud-native/cluster-api-provider-nutanix/pkg/client"
)

func isObjectOrClusterDeleting(obj client.Object, cluster *infrav1.NutanixCluster) bool {
	if obj != nil && !obj.GetDeletionTimestamp().IsZero() {
		return true
	}
	return cluster != nil && !cluster.DeletionTimestamp.IsZero()
}

func credentialFingerprintForCluster(ctx context.Context, cluster *infrav1.NutanixCluster, secretInformer v1.SecretInformer, mapInformer v1.ConfigMapInformer) string {
	if cluster == nil || secretInformer == nil || mapInformer == nil {
		return ""
	}
	helper := nutanixclient.NewHelper(secretInformer, mapInformer)
	endpoint, err := helper.BuildManagementEndpoint(ctx, cluster)
	if err != nil || endpoint == nil {
		return ""
	}
	return nutanixclient.CredentialFingerprint(endpoint)
}

func emitWarningEvent(recorder record.EventRecorder, obj client.Object, reason, message string) {
	if recorder == nil || obj == nil {
		return
	}
	recorder.Event(obj, corev1.EventTypeWarning, reason, message)
}

func markPrismAuthFailed(cluster *infrav1.NutanixCluster, message string) {
	if cluster == nil {
		return
	}
	v1beta1conditions.MarkFalse(cluster, infrav1.PrismCentralClientCondition, infrav1.PrismCentralAuthenticationFailed, capiv1beta1.ConditionSeverityWarning, "%s", message)
	v1beta2conditions.Set(cluster, metav1.Condition{
		Type:    string(infrav1.PrismCentralClientCondition),
		Status:  metav1.ConditionFalse,
		Reason:  infrav1.PrismCentralAuthenticationFailed,
		Message: message,
	})
}

// skipPrismCallsDueToAuthBackoff returns a requeue result when the cluster is deleting
// and the shared auth circuit is open. Callers must not contact Prism Central when paused is true.
func skipPrismCallsDueToAuthBackoff(ctx context.Context, recorder record.EventRecorder, eventObj client.Object, cluster *infrav1.NutanixCluster, deleting bool, secretInformer v1.SecretInformer, mapInformer v1.ConfigMapInformer) (ctrl.Result, bool) {
	if !deleting || cluster == nil {
		return ctrl.Result{}, false
	}

	key := cluster.GetNamespacedName()
	fingerprint := credentialFingerprintForCluster(ctx, cluster, secretInformer, mapInformer)
	delay, allowed := nutanixclient.PrismAuthCircuit.Allow(key, fingerprint)
	if allowed {
		return ctrl.Result{}, false
	}

	message := fmt.Sprintf("pausing Prism Central calls for deleting cluster %s after authentication failures to avoid locking the Prism Central account; retrying in %s. Update the cluster credentials secret to resume deletion", key, delay.Round(time.Second))
	log := ctrl.LoggerFrom(ctx)
	log.Info(message, "consecutiveFailures", nutanixclient.PrismAuthCircuit.FailureCount(key))
	emitWarningEvent(recorder, eventObj, infrav1.PrismCentralAuthenticationFailed, message)
	markPrismAuthFailed(cluster, message)
	return reconcile.Result{RequeueAfter: delay}, true
}

// requeueUnauthorizedPrismError records a 401 during deletion, drops cached clients, and
// requeues with exponential backoff instead of immediately retrying Prism Central.
func requeueUnauthorizedPrismError(ctx context.Context, recorder record.EventRecorder, eventObj client.Object, cluster *infrav1.NutanixCluster, deleting bool, err error, secretInformer v1.SecretInformer, mapInformer v1.ConfigMapInformer) (ctrl.Result, bool) {
	if !deleting || cluster == nil || !nutanixclient.IsUnauthorizedError(err) {
		return ctrl.Result{}, false
	}

	key := cluster.GetNamespacedName()
	fingerprint := credentialFingerprintForCluster(ctx, cluster, secretInformer, mapInformer)
	nutanixclient.DeleteCachedClients(cluster)
	delay := nutanixclient.PrismAuthCircuit.RecordFailure(key, fingerprint)

	message := fmt.Sprintf("Prism Central authentication failed for deleting cluster %s: %v; backing off %s before retrying to avoid locking the Prism Central account. Update the cluster credentials secret to resume deletion", key, err, delay.Round(time.Second))
	if nutanixclient.PrismAuthCircuit.ShouldWarnLockout(key) {
		message = fmt.Sprintf("repeated Prism Central authentication failures for deleting cluster %s; pausing reconciliation to avoid locking the Prism Central account. Last error: %v. Will retry in %s after the credentials secret is updated or the backoff elapses", key, err, delay.Round(time.Second))
	}

	log := ctrl.LoggerFrom(ctx)
	log.Error(err, "Prism Central authentication failed during deletion; applying backoff", "cluster", key, "requeueAfter", delay, "consecutiveFailures", nutanixclient.PrismAuthCircuit.FailureCount(key))
	emitWarningEvent(recorder, eventObj, infrav1.PrismCentralAuthenticationFailed, message)
	markPrismAuthFailed(cluster, message)
	return reconcile.Result{RequeueAfter: delay}, true
}
