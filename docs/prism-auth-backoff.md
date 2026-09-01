# Prism Central authentication backoff during cluster deletion

This document describes how CAPX avoids locking a Prism Central account when a
**deleting** workload cluster keeps retrying with stale credentials.

The usual failure mode is: Prism Central password is rotated globally, secrets
for healthy clusters are updated, but a cluster already in `Deleting` is
missed. Without backoff, every `NutanixCluster`, `NutanixMachine`, and
`NutanixVirtualHADomain` reconcile logs into Prism Central with the old
password and can lock the shared admin account.

## Scope

Backoff applies only when **the object being reconciled is deleting**, or its
owning **NutanixCluster is deleting**.

Healthy (non-deleting) clusters keep the existing retry behavior. They are not
paused by this circuit.

The circuit is **per NutanixCluster** (`namespace/name`), shared in-process
across the cluster, machine, and virtual HA domain controllers. One deleting
cluster cannot keep hammering Prism Central while others continue normally.

State lives **in the CAPX manager process memory**. It is not stored on the
objects. Restarting the CAPX deployment clears the circuit (the next 401s
rebuild it).

## What happens on a 401

On each deletion reconcile, CAPX:

1. Checks whether the cluster already has an open auth backoff.
   If yes, it **does not create Prism clients** and does not call Prism Central.
2. Otherwise it builds clients and continues deletion (find VM, delete VM,
   delete categories, clean vHA resources, and so on).
3. If Prism Central returns **401 Unauthorized** (or `invalid Nutanix credentials`):
   - Cached Prism clients for that cluster are dropped.
   - A consecutive-failure counter is incremented.
   - Reconciliation returns `RequeueAfter` with exponential delay and **no error**.
   - A Warning event `PrismCentralAuthenticationFailed` is emitted.
   - The NutanixCluster `PrismClientInit` condition is set to False with reason
     `PrismCentralAuthenticationFailed`.

After **3 consecutive 401s**, the Warning text states that reconciliation is
paused to avoid locking the Prism Central account.

### Backoff schedule

| Consecutive 401s | Wait before next Prism Central attempt |
| --- | --- |
| 1 | 30 seconds |
| 2 | 1 minute |
| 3 | 2 minutes |
| 4 | 4 minutes |
| 5 | 8 minutes |
| 6 | 16 minutes |
| 7+ | **30 minutes** (cap) |

While waiting, other controllers for the **same** deleting cluster also skip
Prism Central. A different cluster is unaffected.

## How the reconcile cycle works

CAPX controllers are **level-based**. Each reconcile looks at current cluster
state and tries to make it match the desired state (here: finish deletion).
There is no saved “in-progress Prism task pointer” for auth backoff.

Typical loop for a deleting object:

```
Reconcile starts
  → cluster/object is deleting?
  → circuit open?  yes → RequeueAfter remaining delay, exit (no Prism call)
  → circuit open?  no  → call Prism Central
       → 401 → record failure, drop client cache, RequeueAfter backoff, exit
       → success → clear circuit, continue deletion
```

Returning `RequeueAfter` **without an error** means the workqueue schedules the
next run after that delay. The controller-runtime error rate limiter is not
stacked on top of this backoff.

Kubernetes still owns the objects. Finalizers stay until deletion actually
completes. CAPX is only slowing Prism Central login attempts.

## Does a pending action restart?

**Yes, the next reconcile starts deletion work from current observed state.
It does not resume a mid-flight function.**

Examples:

- If CAPX never successfully issued a VM delete because every login was 401,
  the next successful reconcile looks up the VM again and issues delete.
- If a delete task was already accepted by Prism Central before credentials
  broke, the next successful reconcile sees the VM/task state and continues
  (wait for the task, or treat the VM as gone).
- Category cleanup, vHA cleanup, and finalizer removal run only after Prism
  Central calls succeed again (or are no longer needed).

Nothing is “cancelled” by backoff except **further login attempts during the
wait**. The delete request on the Kubernetes objects remains in force.

## After credentials are rotated

Updating the deleting cluster’s Prism Central Secret (username/password or API
key) changes the **credential fingerprint**.

On the **next reconcile** for that cluster:

1. CAPX reads the secret from the informer cache (no Prism Central call).
2. The fingerprint no longer matches the one stored on the circuit.
3. The circuit entry is **cleared immediately**.
4. Cached clients were already dropped on the last 401, so a new client is
   built from the new secret.
5. Deletion continues with the new credentials.

If the new credentials work, `RecordSuccess` clears any remaining circuit
state and deletion proceeds as usual.

### When that “next reconcile” runs

The cluster/machine controllers **do not watch Secrets**. Patching the secret
does not by itself enqueue an immediate reconcile.

Resume happens on the first of:

- The scheduled `RequeueAfter` (could still be up to 30 minutes if the circuit
  is at the cap).
- Any other watch that already enqueues the object (for example a change to
  the `NutanixCluster` / `NutanixMachine`, or a related Cluster event).

To resume **immediately** after rotating the secret, touch the deleting
cluster so a reconcile runs, for example:

```bash
kubectl annotate nutanixcluster <name> -n <namespace> \
  infrastructure.cluster.x-k8s.io/resume-auth-backoff="$(date +%s)" --overwrite
```

Then confirm:

```bash
kubectl describe nutanixcluster <name> -n <namespace>
# Warning event: PrismCentralAuthenticationFailed should stop repeating
# PrismClientInit should become True once login succeeds
```

If the new password is still wrong, CAPX records a fresh 401 and starts
backoff again at 30 seconds.

## What happens after 30 minutes

**30 minutes is the maximum wait between Prism Central attempts, not a
permanent stop.**

When the 30-minute timer elapses:

1. The next reconcile is allowed to contact Prism Central **once** (half-open).
2. If login succeeds, the circuit is cleared and deletion continues.
3. If login is still 401, CAPX records another failure, waits **another 30
   minutes**, and repeats.

So a forgotten deleting cluster with a stale secret probes Prism Central at
most about **once per 30 minutes per cluster**, not in a tight loop, until
either:

- the secret is updated and a reconcile runs, or
- Prism Central starts accepting the stored credentials again, or
- CAPX is scaled down / the objects are force-removed.

After a CAPX pod restart, in-memory backoff is lost, so the first reconciles
may 401 again until the circuit rebuilds (30s, 1m, … up to 30m).

## Operator signals

| Signal | Meaning |
| --- | --- |
| Event `PrismCentralAuthenticationFailed` (Warning) | Deletion hit 401; CAPX is backing off. |
| Condition `PrismClientInit` = False, reason `PrismCentralAuthenticationFailed` | Same, persisted on the NutanixCluster. |
| CAPX logs `Prism Central authentication failed during deletion; applying backoff` | Includes `requeueAfter` and `consecutiveFailures`. |

## Related code

- Circuit breaker: `pkg/client/auth_circuit.go`
- 401 detection and client cache drop: `pkg/client/auth.go`
- Reconcile integration: `controllers/prism_auth_backoff.go`
- Wired from `NutanixCluster`, `NutanixMachine`, and `NutanixVirtualHADomain` reconcilers
