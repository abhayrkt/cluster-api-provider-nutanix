# Metro Scale-Down Balancer

> Documents the feature introduced in
> [PR #761](https://github.com/nutanix-cloud-native/cluster-api-provider-nutanix/pull/761)
> (`feat: controller for metro scale-in`).

## Overview

The **Metro Scale-Down Balancer** (`MetroScaleDownBalancerReconciler`) keeps a
stretched `NutanixMetro` worker `MachineSet` evenly distributed across its two
Prism Element (PE) sites when the pool scales down.

It does **not** change replica counts and does **not** delete machines. Cluster
API (CAPI) remains the only actor that removes machines. The balancer only
biases *which* machine CAPI picks by maintaining the
`cluster.x-k8s.io/delete-machine` annotation on machines of the over-represented
site.

| Touches | Does not touch |
|---|---|
| Stretched `NutanixMetro/` worker `MachineSet`s | `NutanixMetroSite/` (single-site) pools |
| Annotations on CAPI `Machine`s | Replica counts / MachineDeployments |
| | Control-plane pools |
| | Non-metro failure domains |

## Motivation

A worker pool placed on a stretched metro failure domain exposes a **single**
`spec.failureDomain` to CAPI (for example `NutanixMetro/metro0`). From CAPI’s
point of view the pool is one failure domain, so the `MachineSet` delete policy
is **site-blind**.

On scale-down, CAPI can preferentially delete machines from one PE site. That:

1. **Starves that Prism Element** of worker capacity and defeats the purpose of
   stretching the pool across two sites.
2. For **Rook-Ceph** clusters, can wedge OSD scheduling when one site loses too
   many nodes.

CAPI documents `cluster.x-k8s.io/delete-machine` as the **top-priority** signal
on every delete policy (`Random`, `Newest`, `Oldest`): annotated machines are
always chosen first. The balancer uses that lever to steer deletions toward the
fuller site.

```text
Without balancer (site-blind):          With balancer:

  PE-A: ●●●●●●   PE-B: ●●●●●●             PE-A: ●●●●●●   PE-B: ●●●●●●
         scale 12 → 6                              scale 12 → 6
  PE-A: ●●●●●●   PE-B: (empty)            PE-A: ●●●      PE-B: ●●●
         ↑ one-sided collapse                     ↑ sites stay balanced
```

## Scope and gating

The controller reconciles CAPI `MachineSet`s and only acts when **all** of the
following hold:

1. `MachineSet.spec.template.spec.failureDomain` has the `NutanixMetro/` prefix
   (checked via `isNutanixMetroFailureDomain`).
2. The `MachineSet` is **not** being deleted (`DeletionTimestamp` is zero).

Pools with `NutanixMetroSite/…`, empty/non-metro failure domains, or terminating
`MachineSet`s are no-ops — zero behavior change for them.

## How it works

### Watches

| Primary | Secondary |
|---|---|
| `MachineSet` | `Machine` → mapped back to owning `MachineSet` via `cluster.x-k8s.io/set-name` |

A create, placement-label update, or deletion on any machine re-evaluates the
whole balancing group.

### Reconcile flow

```text
MachineSet reconcile
        │
        ▼
 Is failureDomain "NutanixMetro/*"? ──no──► exit (no-op)
        │ yes
        ▼
 MachineSet deleting? ──yes──► exit (no-op)
        │ no
        ▼
 List Machines owned by the MachineSet
        │
        ▼
 selectVictims()  ── group by site, compute K, greedy pick
        │
        ▼
 applyDeleteAnnotations()  ── mark / clear managed annotations
```

### Site attribution

Each live machine is attributed to a PE site by reading the owning
`NutanixMachine` label:

| Label | Meaning |
|---|---|
| `metro.nutanix.com/native-failuredomain` | Native `NutanixFailureDomain` (site) where the VM was placed |

Machines that are terminating, missing an infra ref, or not yet labeled are
skipped for that pass and reconsidered once CAPX records placement.

### Victim count `K`

```text
liveTotal  = count of non-terminating Machines in the MachineSet
excess     = max(site counts) − min(site counts)   # 0 if fewer than 2 sites
pendingDelete = liveTotal − spec.replicas          # only when replicas < liveTotal

K = pendingDelete > 0 ? pendingDelete : excess
K = min(K, knownTotal)                             # machines with a known site
```

- **Pending scale-down:** `K` equals the number of machines CAPI is about to
  remove, so the exact victims CAPI deletes are the balanced ones.
- **Steady state:** `K` equals the current imbalance, so excess machines on the
  fuller site stay pre-marked for the next scale-down.

### Greedy balanced pick

For each of `K` steps:

1. Pick the site that currently has the most remaining machines.
2. On a tie, pick the lexicographically smaller site name.
3. Within a site, prefer the **newest** machine (creationTimestamp descending,
   name as tie-break) — aligning with CAPI’s default preference and minimizing
   disruption to long-lived workloads.

That keeps the remainder as balanced as possible for any `K` (for an odd final
count the best achievable balance is a difference of 1).

### Annotations

| Annotation | Owner | Role |
|---|---|---|
| `cluster.x-k8s.io/delete-machine` | CAPI (signaled by balancer or operator) | Top-priority delete signal |
| `metro.nutanix.com/managed-delete-machine` | Balancer only | Ownership marker for annotations the balancer set |

Rules:

- **Want victim, no delete annotation** → set both annotations.
- **Want victim, already has delete annotation** → leave alone (respect operator).
- **Not a victim, has managed marker** → clear both (stale cleanup).
- **Not a victim, operator-set delete only** → never clear.

## Worked examples

### Example 1 — pending scale-down `6 → 2` (`K = 4`)

Start balanced `fd0=3 / fd1=3`, `spec.replicas=2`.

- `pendingDelete = 4`, `excess = 0` → `K = 4`
- Greedy pick: one from each site, alternating → **2 from each site**
- After CAPI deletes: `fd0=1 / fd1=1`

### Example 2 — steady-state bias, then scale `3 → 2`

Pool at `fd0=1 / fd1=2` (no pending delete).

- `excess = 1` → pre-mark the excess machine on `fd1`
- Operator scales to 2 → CAPI deletes the pre-marked `fd1` machine
- Result: `fd0=1 / fd1=1`

### Example 3 — odd remainder `9 → 3` (`K = 6`)

After scale-up: `fd0=5 / fd1=4`. Scale to 3.

- Remainder lands at best-possible balance for 3 machines (e.g. `1/2`)
- The single excess machine retains the delete + managed markers so the next
  scale-down converges to `1/1`

## RBAC

Kubebuilder markers regenerate into `config/rbac/role.yaml`:

| API group | Resource | Verbs |
|---|---|---|
| `cluster.x-k8s.io` | `machinesets`, `machinesets/status` | `get`, `list`, `watch` |
| `cluster.x-k8s.io` | `machines`, `machines/status` | `get`, `list`, `watch`, `update`, `patch` |
| `infrastructure.cluster.x-k8s.io` | `nutanixmachines` | `get`, `list`, `watch` |

Write access is required only on `machines` (to patch annotations). Everything
else is read-only.

## Code map

| Path | Role |
|---|---|
| `controllers/nutanixmetro_scaledown_controller.go` | Reconciler, victim selection, annotation apply |
| `controllers/nutanixmetro_scaledown_controller_test.go` | Unit tests (balanced / imbalanced / pending delete / operator respect / skip gates) |
| `main.go` (`setupMetroScaleDownBalancerController`) | Registers `MetroScaleDownBalancer-controller` with the manager |
| `config/rbac/role.yaml` | Regenerated RBAC |

Controller name in logs / manager: `MetroScaleDownBalancer-controller`.

## Non-goals and known limitations

1. **Does not own replica counts.** The user, autoscaler, or CAPI decides *how
   many* machines to remove; this controller only influences *which* ones.
2. **Does not delete machines.** CAPI remains the sole deleter.
3. **Does not touch** control-plane, `NutanixMetroSite/`, or non-metro pools.
4. **Large single-step scale-down past balance:** marking corrects imbalance
   first. Victims past the balance point fall back to CAPI’s site-blind policy
   for that batch; the controller re-reconciles and re-marks afterward. This is
   a strict improvement over the default and does not recreate one-sided
   collapse for normal (incremental) scale-downs.
5. **Race with CAPI on large sudden deletions:** for a big scale-down with no
   pre-existing imbalance, the balancer must patch many machines while CAPI’s
   `MachineSet` controller may already be deleting from a snapshot that lacks
   those annotations. Prefer incremental scale-downs, or rely on steady-state
   pre-marking where possible. Transient over-mark mid-flight during a large
   scale-down is expected and converges on later reconciles.

## Observability tips

After deploy, confirm the controller started:

```text
MetroScaleDownBalancer-controller … Starting workers
```

On machines in a stretched metro worker pool, inspect:

```bash
kubectl get machine -n <ns> -o custom-columns=\
NAME:.metadata.name,\
DELETE:.metadata.annotations.cluster\\.x-k8s\\.io/delete-machine,\
MANAGED:.metadata.annotations.metro\\.nutanix\\.com/managed-delete-machine
```

- Both annotations present → balancer chose that victim.
- Only `delete-machine` present → operator (or another actor) marked it; balancer
  will not clear it.
- Site attribution (on the infra object):

```bash
kubectl get nutanixmachine -n <ns> -o custom-columns=\
NAME:.metadata.name,\
SITE:.metadata.labels.metro\\.nutanix\\.com/native-failuredomain
```

## Related constants

Defined in `controllers/helpers.go` (and the balancer source):

| Constant / value | Purpose |
|---|---|
| `NutanixMetro/` | Stretched-metro failure-domain prefix (in scope) |
| `NutanixMetroSite/` | Single-site metro prefix (out of scope) |
| `metro.nutanix.com/native-failuredomain` | Per-machine PE site label on `NutanixMachine` |
| `metro.nutanix.com/managed-delete-machine` | Balancer ownership of delete annotations |
| `cluster.x-k8s.io/delete-machine` | CAPI top-priority delete annotation |

## References

- Pull request: https://github.com/nutanix-cloud-native/cluster-api-provider-nutanix/pull/761
- CAPI delete-machine annotation (MachineSet delete priority)
- CAPX metro placement / `NutanixMetro` controllers in `controllers/`
