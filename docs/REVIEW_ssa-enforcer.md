# Review — the server-side-apply enforcer (branch feat/ssa-enforcer)

Adversarial second-opinion passes, 2026-09-05, on `LockedResourceReconciler` enforcing with server-side apply
(commit 1bab849 on top of upstream master 09e27b5, then the fixes below). Reviewers: Codex (gpt-5.6-sol, xhigh)
and Cursor (Grok 4.6 high fast, ask mode). Neither could run a process or create a file this run, so every verdict
is traced from source; every runtime claim was measured here against envtest (a real API server) and, for the
operator, on a live cluster. Briefs and raw outputs: session scratchpad `adv/review_brief_ssa*.md`,
`adv/review_{codex,cursor}_ssa*.txt`.

## Contract under review

Every reconcile GETs the live object. Absent: the whole rendered object is applied under `FieldManager` with force
(excluded paths included: "set once"). Present: the reconciler releases its ownership of every excluded path
(and, if the consumer named legacy managers, folds their client-side entries into `FieldManager` with client-go's
csaupgrade) with one JSON patch sent only when something changes, then applies the rendered object minus the
excluded paths. Consequences: drift on an owned field restored; a field the template stops rendering removed; an
excluded field left as it is, owned by nobody; other managers' fields untouched; a no-op apply writes nothing.

## First pass (1bab849)

| Claim | Cursor | Codex | Decision |
|---|---|---|---|
| C1 release-then-apply safe under concurrency | CONFIRMED | CONFIRMED | — (the patch replaces resourceVersion: a concurrent write conflicts; force apply only takes fields it sends) |
| C2 excluded-path release is complete | REFUTED | REFUTED | **Accepted, two defects**: a quoted numeric key (`.data['0']`) was taken for a list index and released all of `data` (fix: `FieldPath` returns an Index flag per segment; measured with a ConfigMap key "0"); a child of an atomic map (a Pod's `nodeSelector.zone`) cannot be released alone, the map stayed owned and the apply deleted the child (Codex; fix: each exclusion widens to the unit the server tracks when nothing is owned at or below it but a strict ancestor is, and that unit is left out of the apply; measured with a Pod) |
| C3 creation "sets once", no window | CONFIRMED | REFUTED | **Accepted in Codex's form**: `.rules[0]` released the whole list and then applied the list without element 0, deleting it (as the merge patch did). With widening the list is the unit: released and not applied; both elements stay (measured with a Role). The cost, stated in the code: a non-excluded sibling inside the unit is no longer enforced after creation |
| C4 legacy fold cannot remove another actor's field | REFUTED | REFUTED | **Accepted as a policy, not as code the reviewers proposed**: a whole entry of the legacy name is folded, so a same-named foreign actor's field would go. Cursor's "fold only what the template renders" and Codex's "remove the fold" both give up removing stale fields on legacy objects, the failure #194 reported. Decision: the fold is opt-in (`LegacyFieldManagers` empty by default; the operator names "manager" because it knows its history), the residual risk is written next to the variable |
| C5 lists are atomic; drift restored, dropped element removed | PLAUSIBLE | REFUTED (unmeasured) | **Measured** with a Role: drift inside the list restored, a dropped element removed |
| C6 status/conditions unchanged; "equal" log exact | REFUTED | REFUTED | **Accepted**: conditions unchanged; the "equal" verdict compared the apply's result with the first GET, so a release patch followed by a no-op apply logged "NOT equal". `releaseOwnership` returns the object the apply races. Test with a capturing log sink |
| C7 dependencies clean | PLAUSIBLE | PLAUSIBLE | `go mod tidy` clean: jsondiff gone, structured-merge-diff v4.2.3 direct, one copy |
| C8 tests not vacuous | CONFIRMED | PLAUSIBLE | — |

**Found here, fixed:** a locked resource's identity was its object alone, so editing a template's excludedPaths on
a CR changed nothing until the operator restarted (measured on a cluster: `.metadata` removed from a
NamespaceConfig, a tampered label not restored). `GetKey` now carries the sorted excluded paths; a changed key
restarts the manager without deleting anything (the deletion set keys on kind, namespace and name).

**Rejected:** Codex's second GET after the release plus a resourceVersion precondition on the apply (the release
returns the patched object; server-side apply cannot delete another manager's field whatever interleaves, and a
precondition only adds conflicts under churn).

## Measured on a live cluster (operator build against this library)

Every object the operator had created carried one entry, manager `manager`, operation Update. After one pass
with `LegacyFieldManagers = ["manager"]`: that entry gone, `lockedresourcecontroller`/Apply owning `roleRef` and
`subjects` only, all 10 labels and the annotations kept (owned by nobody), a hand-added subject removed within one
reconcile, a hand-added label kept with its own manager, a forced reconcile afterwards changing no
resourceVersion, 40 one-time ownership patches, every CR ReconcileSuccess. A local build named otherwise did not
fold (the user-agent-derived default), which is why the operator names the manager explicitly and the library's
default is empty. With `.metadata` removed from a NamespaceConfig's excludedPaths (after the `GetKey` fix): the
reconciler took ownership of the rendered labels and annotations, a tampered rendered label was restored, a
foreign label left alone.

## Second pass (f86b9a8)

Pending: the widening (a map-keyed list such as Deployment containers), the pointer round-trip of `desired`,
the opt-in fold, `FieldPath`'s grammar.
