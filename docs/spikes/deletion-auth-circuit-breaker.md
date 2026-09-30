# Spike: stop deletion retries from locking the Prism Central account

Status: proposal. No controller behavior changes in this change.

## Problem

A workload cluster stuck in deletion keeps its own Prism Central credential Secret. After a global password rotation, operators update the Secrets for healthy clusters and often miss the one still referenced by the deleting cluster. CAPX keeps reconciling that deletion and logs into Prism Central with the stale password.

Prism Central account lockout is per user, not per cluster. Repeated failed logins from the deleting cluster lock the shared admin (or API) account. Healthy clusters then fail as well, and the NKP environment goes down. Recovery today is to scale CAPX to zero, patch the overlooked Secret, or force-remove the cluster.

## What the code does today

Deletion does not have its own client or its own retry policy. Every reconcile builds Prism clients before it decides it is deleting, then any Prism error is returned to controller-runtime and retried forever.

Entry points that construct a session-auth client from the cluster Secret:

- `NutanixClusterReconciler.Reconcile` calls `getPrismCentralClientForCluster` and `getPrismCentralConvergedV4ClientForCluster` before `reconcileDelete` (`controllers/nutanixcluster_controller.go`).
- `NutanixMachineReconciler.Reconcile` does the same before `reconcileDelete` (`controllers/nutanixmachine_controller.go`). Machine deletion is the hot path: the cluster reconciler only waits (`RequeueAfter: 5s`) until those machines are gone, and each machine calls Prism to find and delete its VM.
- `NutanixVirtualHADomainReconciler` builds the same clients and calls Prism while it cleans up protection policies and recovery plans. Cluster deletion waits on those objects too.

Clients are cached process-wide with session auth enabled (`pkg/client/cache.go`):

```go
var NutanixClientCache = v3.NewClientCache(v3.WithSessionAuth(true))
var NutanixClientCacheV4 = v4.NewClientCache(v4.WithSessionAuth(true))
var NutanixConvergedClientV4Cache = v4Converged.NewClientCache(v4.WithSessionAuth(true))
```

`prism-go-client` v0.8.2 (`internal/client.go`) does three things that matter here:

1. `NewClient` calls `refreshCookies` immediately when session auth is on. That is `GET /users/me` with the username and password (or API key). A wrong password fails client creation, and a failed create is not stored in the cache, so the next reconcile tries to log in again.
2. The cache key is the NutanixCluster namespace/name. The validation hash is the whole management endpoint, including the password. An unchanged stale Secret keeps hitting the same cached client. The password is copied into that client at creation time.
3. On HTTP 401, `Client.do` clears cookies and calls `refreshCookies` once. Session cookies are documented as living about 15 minutes, and a password change on Prism Central invalidates them sooner. After that, every Prism call from the cached client performs a fresh basic-auth login with the snapshotted stale password. `CheckResponse` turns that 401 into `fmt.Errorf("invalid Nutanix credentials")`, dropping the status code.

The manager already installs a workqueue limiter (`main.go`): per-item exponential backoff from 500ms to 15 minutes, capped by a shared bucket of 10 QPS and burst 100, with up to 10 concurrent reconciles. That limiter does not stop the lockout:

- Backoff is per workqueue item. A deleting cluster with N machines is N independent loops, plus the cluster reconciler and any virtual HA domain reconcilers.
- The first failures are fast (500ms, 1s, 2s, …) and run in parallel. Ten machines produce on the order of ten failed logins before any of them has backed off past a typical lockout threshold.
- Items that fail together stay roughly in phase, so later retries arrive as a burst. A burst of failed logins every 15 minutes re-locks the account as soon as Prism Central unlocks it.
- `RequeueAfter` with a nil error (the “still waiting for machines” path) calls `Forget` and clears that item’s failure count. It does not by itself log in, but it also does not coordinate the machine reconcilers that do.
- Nothing records a Warning event. CAPX has no `EventRecorder` on these reconcilers. The only signal is log lines and a `PrismClientInit` / `DeletionFailed` condition that is rewritten on the next attempt.

There is also no watch on the credential Secret. Patching the Secret does not enqueue the cluster; resumption waits for the next retry, and the cached client is rebuilt only because the validation hash changes.

## Requirement

After a small number of authentication failures for one Prism identity, CAPX must stop sending that identity’s credentials to Prism Central. It must surface a Warning that names the cluster and the Secret to update. Updating that Secret must resume deletion without a controller restart. Healthy clusters that already use the new password must keep reconciling.

## Options

### 1. Rely on the existing rate limiter, with a longer max delay

Rejected. Per-item backoff cannot serialize logins that share one Prism user. A longer cap (for example 1 hour) only lengthens the window during which a burst of machines re-locks the account. It also slows unrelated failures.

### 2. Fixed `RequeueAfter` on any delete error

Rejected. Deletion also fails for tasks in progress, missing projects, and Prism 5xx. Those should keep today’s retry. Only authentication failures are unsafe to retry quickly, and only the shared identity needs to pause.

### 3. Per-cluster exponential backoff inside the reconciler

Matches the wording of the request, and it is not sufficient on its own. The cluster reconciler is not the loop that logs in most often; each `NutanixMachine` is. Pausing only the cluster object leaves the machines running. Pausing each machine independently recreates option 1.

### 4. Recommended: one circuit breaker per Prism credential, shared by every controller

A process-local breaker, keyed by Prism address plus a fingerprint of the secret material, shared by the cluster, machine, and virtual HA domain reconcilers. When the breaker is open, reconcilers return before `GetOrCreate`, so `refreshCookies` never runs. A single half-open probe, not one probe per machine, decides whether to close.

This is the circuit breaker the request asks for. The exponential backoff is the open interval of that breaker, not the workqueue limiter.

## Design

### Breaker

New type in `pkg/client`, next to the client caches, with a package-level instance the reconcilers share. In-memory state is enough: a restart clears it, and the first few failures after a restart re-open it before lockout. Do not store the password or its hash in a Secret or CR status.

```text
key = prism host:port + NUL + auth kind + NUL + fingerprint
```

- Basic auth fingerprint: SHA-256 of `username + NUL + password`.
- API key fingerprint: SHA-256 of the key.

The username alone is the wrong key. Healthy clusters and the deleting cluster often use the same Prism user with different passwords after a rotation. Keying only on user would open the breaker for the healthy clusters too. Keying on the secret material isolates the stale Secret. Those healthy clusters keep calling Prism with the new password.

State per key:

| Field | Meaning |
| --- | --- |
| consecutiveFailures | Auth failures since the last success or fingerprint change |
| state | closed, open, half-open |
| openUntil | When the single probe is allowed |
| probeInFlight | Set while the half-open probe is running |

Transitions:

1. **Closed.** Calls proceed. A non-auth error does not change the breaker. An auth failure increments `consecutiveFailures`.
2. **Open.** At 3 consecutive auth failures, open the breaker. Three stays under the usual Prism lockout of about five attempts, including the parallel burst already in flight. While open, `Allow` returns a `RequeueAfter` and does not call Prism.
3. **Half-open.** After `openUntil`, the next caller wins `probeInFlight` and may call Prism once. Every other caller, including other machines in the same cluster, keeps waiting.
4. **Close** on probe success, and zero the failure count. **Re-open** on probe failure. The open interval doubles: 1m, 2m, 4m, 8m, then caps at 15m. The cap matches the existing rate-limiter max so a dead Secret is quiet without stalling forever if the probe error was misclassified.

`Allow` and `Record` must be safe for the 10 concurrent reconciles. The probe flag is the part that prevents the thundering herd the workqueue limiter cannot prevent.

### Where to call it

Gate it on deletion. The incident is a deleting cluster whose Secret was not rotated. Healthy reconcilers should keep current retry behavior so a transient 401 during session refresh is not turned into a 15 minute pause.

Treat the cluster as in scope when any of these is set:

- `NutanixCluster.deletionTimestamp`
- `NutanixMachine.deletionTimestamp` (the machine reconciler already loaded the owning `NutanixCluster`)
- `NutanixVirtualHADomain.deletionTimestamp`

Both client helpers (`getPrismCentralClientForCluster`, `getPrismCentralConvergedV4ClientForCluster`) should consult the breaker before `GetOrCreate`. On an open breaker, return a typed `ErrAuthCircuitOpen` that carries `RequeueAfter`. Reconcilers must recognize that error and return `reconcile.Result{RequeueAfter: d}, nil` rather than a generic error. Returning a nil error avoids another workqueue failure backoff on top of the breaker. The condition and event are still written in the deferred patch.

On any other error from a Prism call made in scope, classify it. Only an auth failure calls `RecordFailure`. A successful Prism call, or a successful client create, calls `RecordSuccess`.

Classification has to live in CAPX for now: `CheckResponse` in prism-go-client v0.8.2 returns the plain string `invalid Nutanix credentials` and does not wrap a typed error or the HTTP status. Match that string, and also match errors that still expose status 401. A follow-up in prism-go-client should return a typed unauthorized error so this does not depend on error text. v4 clients in this repo use the same `internal.Client` session-auth path, so the same string covers the converged client’s login. SDK errors returned after a session exists still need a unit-test fixture; if they do not contain that string, extend the classifier with the fixture rather than treating every Prism error as auth.

### What the user sees

CAPX does not emit Kubernetes events today. Add `record.EventRecorder` to the cluster, machine, and virtual HA domain reconcilers from `mgr.GetEventRecorderFor(...)`.

On the transition to open, one Warning on the `NutanixCluster`:

```text
PrismAuthenticationPaused: Prism Central rejected credentials from Secret <ns>/<name>; deletion retries are paused until the Secret is updated or the backoff elapses
```

Fire the event on the transition, not on every reconcile. Also set a condition on `NutanixCluster` so `kubectl describe` shows it without depending on event TTL:

- type: `PrismCentralAuthentication`
- status: False
- reason: `AuthenticationPaused`
- severity: Warning

On close, set the condition True with reason `Authenticated` and emit a Normal event `PrismAuthenticationRestored`.

Machine conditions can keep `DeletionFailed` while the breaker is open, with the same message, so a machine in `kubectl get nutanixmachine` does not look idle.

### Resuming after the Secret is patched

Two mechanisms, both required:

1. The breaker key includes the secret fingerprint. The patched Secret is a new key in the closed state, so the next reconcile logs in with the new password. The old key ages out of the map.
2. Watch `Secret` from the cluster reconciler (and map it to `NutanixCluster` objects whose `spec.prismCentral.credentialRef` points at it). A Secret update enqueues the cluster immediately. The machine reconciler already watches `NutanixCluster`, so the cluster status change wakes the machines. Without the watch, resumption waits for the current open interval.

Do not log the Secret data. Log the Secret namespace/name, the cluster name, and the open interval.

### Out of scope for the first change

- Pausing healthy clusters that still have a wrong password. Worth a follow-up: the same breaker can be armed for non-delete reconciles, but that changes steady-state retry and should be a separate decision.
- A multi-replica CAPX deployment. Each process has its own breaker. Leader election already keeps a single active manager; if that assumption changes, the breaker would need a shared store.
- Changing prism-go-client retry-on-401. The typed error is a useful follow-up. Disabling session auth would make every API call send the password, which is worse.
- Force-delete or skipping Prism cleanup. The breaker pauses cleanup; it does not drop finalizers. Objects stay until credentials work or an operator removes them.

## Flow

```text
reconcile delete
  -> breaker.Allow(fingerprint)
       open      -> event once, set condition, return RequeueAfter, no Prism call
       half-open -> one caller probes; others wait
       closed    -> GetOrCreate / Prism call
            401 / "invalid Nutanix credentials" -> RecordFailure (may open)
            success                             -> RecordSuccess
            anything else                       -> existing error handling

Secret updated
  -> watch enqueues NutanixCluster
  -> new fingerprint, breaker closed
  -> deletion calls Prism again
```

## Test plan

Unit tests, no Prism and no e2e:

- Breaker: three failures open it; the fourth `Allow` does not grant a probe before `openUntil`; exactly one caller gets the half-open probe under `parallel` goroutines; probe failure doubles the interval and caps at 15m; probe success closes and clears the count; a different fingerprint is closed even while the old one is open.
- Classifier: the prism-go-client sentence `invalid Nutanix credentials` is an auth failure; a wrapped 401 is an auth failure; a category-delete 404 or a task-in-progress error is not.
- `getPrismCentralClientForCluster` with the breaker open does not call `GetOrCreate` (inject a cache or a fake breaker). The cluster and machine delete paths return `RequeueAfter` and a nil error, set `PrismCentralAuthentication=False`, and record one Warning.
- A second reconcile while still open does not record a second event.
- After `RecordSuccess`, the condition becomes True.

Run `make unit-test` for `pkg/client` and `controllers`. No CRD change is required if the new condition is a Cluster API condition with the existing string pattern; if a kubebuilder marker is added to the Go type, run `make generate` and `make manifests`. No template change, so `make template-test` is not required.

## Implementation order

1. `pkg/client` breaker and auth-error classifier, with tests.
2. Consult the breaker inside the two `getPrismCentral*ClientForCluster` helpers, behind a deletion-scoped context flag set by the three reconcilers.
3. Map `ErrAuthCircuitOpen` to `RequeueAfter`, the condition, and a single Warning event.
4. Secret watch so a patched credential resumes deletion on the next sync.

## Risks

- Misclassifying a non-auth error as auth would pause deletion for up to 15 minutes at a time. The half-open probe limits that damage; the classifier tests are the guard. Prefer missing a strange 401 shape over pausing on every error.
- In-flight reconciles that already passed `Allow` can still log in a handful of times after the breaker opens. The threshold of three leaves room for that burst. Do not raise concurrency as part of this work.
- API-key auth locks the key, not a password. The same breaker applies; the fingerprint is the key material, and the event text should say “credentials” so it covers both.
