package lockedresourcecontroller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/go-logr/logr"
	"github.com/redhat-cop/operator-utils/pkg/util"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/structured-merge-diff/v4/fieldpath"
)

// The reconciler's contract is the API server's server-side-apply behaviour, so these tests run
// against a real one (envtest). `make test` sets KUBEBUILDER_ASSETS; without it they are skipped
// with a message, never silently.
var testCfg *rest.Config

func TestMain(m *testing.M) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		fmt.Println("KUBEBUILDER_ASSETS is not set: the envtest-backed reconciler tests are skipped (run through `make test`)")
		os.Exit(m.Run())
	}
	env := &envtest.Environment{}
	cfg, err := env.Start()
	if err != nil {
		fmt.Println("envtest failed to start:", err)
		os.Exit(1)
	}
	testCfg = cfg
	code := m.Run()
	_ = env.Stop()
	os.Exit(code)
}

func needEnvtest(t *testing.T) {
	t.Helper()
	if testCfg == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
}

func configMap(name string, data map[string]string, labels map[string]string) *unstructured.Unstructured {
	obj := map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]interface{}{"name": name, "namespace": "default"},
	}
	if labels != nil {
		obj["metadata"].(map[string]interface{})["labels"] = toInterface(labels)
	}
	if data != nil {
		obj["data"] = toInterface(data)
	}
	return &unstructured.Unstructured{Object: obj}
}

func toInterface(m map[string]string) map[string]interface{} {
	out := map[string]interface{}{}
	for k, v := range m {
		out[k] = v
	}
	return out
}

func newReconciler(t *testing.T, resource *unstructured.Unstructured, excluded []string) *LockedResourceReconciler {
	t.Helper()
	c, err := client.New(testCfg, client.Options{Scheme: scheme.Scheme})
	if err != nil {
		t.Fatal(err)
	}
	return &LockedResourceReconciler{
		Resource:       *resource,
		ExcludePaths:   excluded,
		ReconcilerBase: util.NewReconcilerBase(c, scheme.Scheme, testCfg, nil, c),
		log:            logr.Discard(),
	}
}

func reconcileOnce(t *testing.T, r *LockedResourceReconciler) {
	t.Helper()
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: r.Resource.GetName(), Namespace: r.Resource.GetNamespace()}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
}

func clientset(t *testing.T) *kubernetes.Clientset {
	t.Helper()
	cs, err := kubernetes.NewForConfig(testCfg)
	if err != nil {
		t.Fatal(err)
	}
	return cs
}

func live(t *testing.T, name string) *corev1.ConfigMap {
	t.Helper()
	cm, err := clientset(t).CoreV1().ConfigMaps("default").Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return cm
}

func ownedBy(t *testing.T, cm *corev1.ConfigMap, manager string, op metav1.ManagedFieldsOperationType) *fieldpath.Set {
	t.Helper()
	for _, e := range cm.ManagedFields {
		if e.Manager == manager && e.Operation == op && e.FieldsV1 != nil {
			set := fieldpath.NewSet()
			if err := set.FromJSON(bytes.NewReader(e.FieldsV1.Raw)); err != nil {
				t.Fatal(err)
			}
			return set
		}
	}
	return nil
}

func owns(set *fieldpath.Set, parts ...interface{}) bool {
	return set != nil && set.Has(fieldpath.MakePathOrDie(parts...))
}

// Another actor writing through server-side apply under its own manager name.
func applyAs(t *testing.T, manager string, obj *unstructured.Unstructured) {
	t.Helper()
	data, err := json.Marshal(obj.Object)
	if err != nil {
		t.Fatal(err)
	}
	force := true
	_, err = clientset(t).CoreV1().ConfigMaps("default").Patch(context.Background(), obj.GetName(), types.ApplyPatchType, data, metav1.PatchOptions{FieldManager: manager, Force: &force})
	if err != nil {
		t.Fatal(err)
	}
}

// An actor writing the old way: a client-side update under its own manager name.
func updateAs(t *testing.T, manager string, mutate func(*corev1.ConfigMap)) func(name string) {
	return func(name string) {
		t.Helper()
		cs := clientset(t)
		cm, err := cs.CoreV1().ConfigMaps("default").Get(context.Background(), name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		mutate(cm)
		if _, err := cs.CoreV1().ConfigMaps("default").Update(context.Background(), cm, metav1.UpdateOptions{FieldManager: manager}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSSA_CreatesThenRestoresDrift(t *testing.T) {
	needEnvtest(t)
	r := newReconciler(t, configMap("ssa-drift", map[string]string{"a": "1"}, nil), nil)
	reconcileOnce(t, r)
	cm := live(t, "ssa-drift")
	if cm.Data["a"] != "1" {
		t.Fatalf("created object must carry the template's data, got %v", cm.Data)
	}
	if !owns(ownedBy(t, cm, FieldManager, metav1.ManagedFieldsOperationApply), "data", "a") {
		t.Fatalf("the reconciler must own data.a after creating, managedFields=%v", cm.ManagedFields)
	}
	updateAs(t, "someone", func(cm *corev1.ConfigMap) { cm.Data["a"] = "changed" })("ssa-drift")
	reconcileOnce(t, r)
	if got := live(t, "ssa-drift").Data["a"]; got != "1" {
		t.Fatalf("drift on an owned field must be restored, got %q", got)
	}
}

// The #194 case: the template stops rendering a field. Same manager, so the server removes it.
func TestSSA_FieldDroppedFromTemplateIsRemoved(t *testing.T) {
	needEnvtest(t)
	r := newReconciler(t, configMap("ssa-dropped", map[string]string{"a": "1", "b": "1"}, nil), nil)
	reconcileOnce(t, r)
	r.Resource = *configMap("ssa-dropped", map[string]string{"a": "1"}, nil)
	reconcileOnce(t, r)
	cm := live(t, "ssa-dropped")
	if _, still := cm.Data["b"]; still {
		t.Fatalf("a field the template no longer renders must be removed, got %v", cm.Data)
	}
	if cm.Data["a"] != "1" {
		t.Fatalf("the rendered field must remain, got %v", cm.Data)
	}
}

// An excluded path is set when the object is created and never enforced again: the reconciler
// releases its ownership before the next apply, so the server keeps whatever is there.
func TestSSA_ExcludedPathIsSetOnceThenLeftAlone(t *testing.T) {
	needEnvtest(t)
	r := newReconciler(t, configMap("ssa-excluded", map[string]string{"a": "1", "b": "from-template"}, nil), []string{".data.b"})
	reconcileOnce(t, r)
	if got := live(t, "ssa-excluded").Data["b"]; got != "from-template" {
		t.Fatalf("an excluded field is still set at creation, got %q", got)
	}
	updateAs(t, "someone", func(cm *corev1.ConfigMap) { cm.Data["b"] = "theirs"; cm.Data["a"] = "drift" })("ssa-excluded")
	reconcileOnce(t, r)
	cm := live(t, "ssa-excluded")
	if cm.Data["b"] != "theirs" {
		t.Fatalf("an excluded field must be left as another actor set it, got %q", cm.Data["b"])
	}
	if cm.Data["a"] != "1" {
		t.Fatalf("a non-excluded sibling must still be enforced, got %q", cm.Data["a"])
	}
	if owns(ownedBy(t, cm, FieldManager, metav1.ManagedFieldsOperationApply), "data", "b") {
		t.Fatalf("the reconciler must not own the excluded field, managedFields=%v", cm.ManagedFields)
	}
	// and a second reconcile, with nothing left to release, does not delete it either
	reconcileOnce(t, r)
	if got := live(t, "ssa-excluded").Data["b"]; got != "theirs" {
		t.Fatalf("an excluded field must survive every reconcile, got %q", got)
	}
}

// The operator's default exclusion. Labels from the template are set at creation, and later left
// to whoever changes them; a label another actor adds is never touched.
func TestSSA_ExcludedMetadataKeepsLabelsFromCreation(t *testing.T) {
	needEnvtest(t)
	r := newReconciler(t, configMap("ssa-metadata", map[string]string{"a": "1"}, map[string]string{"owner": "template"}), []string{".metadata"})
	reconcileOnce(t, r)
	if got := live(t, "ssa-metadata").Labels["owner"]; got != "template" {
		t.Fatalf("labels are still set at creation with .metadata excluded, got %v", live(t, "ssa-metadata").Labels)
	}
	updateAs(t, "someone", func(cm *corev1.ConfigMap) { cm.Labels["owner"] = "changed"; cm.Labels["extra"] = "x" })("ssa-metadata")
	reconcileOnce(t, r)
	reconcileOnce(t, r)
	cm := live(t, "ssa-metadata")
	if cm.Labels["owner"] != "changed" || cm.Labels["extra"] != "x" {
		t.Fatalf("with .metadata excluded, labels must be left alone after creation, got %v", cm.Labels)
	}
	if owns(ownedBy(t, cm, FieldManager, metav1.ManagedFieldsOperationApply), "metadata", "labels") {
		t.Fatalf("the reconciler must not own metadata.labels, managedFields=%v", cm.ManagedFields)
	}
}

// An object created by an earlier version of this library through a client-side update: the
// stale field it owns must go once the template no longer renders it, after the entry is folded.
func TestSSA_LegacyClientSideEntryIsFolded(t *testing.T) {
	needEnvtest(t)
	saved := LegacyFieldManagers
	LegacyFieldManagers = []string{"legacy-manager"}
	t.Cleanup(func() { LegacyFieldManagers = saved })

	cs := clientset(t)
	if _, err := cs.CoreV1().ConfigMaps("default").Create(context.Background(), &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "ssa-legacy", Namespace: "default", Labels: map[string]string{"owner": "legacy"}},
		Data:       map[string]string{"a": "1", "stale": "1"},
	}, metav1.CreateOptions{FieldManager: "legacy-manager"}); err != nil {
		t.Fatal(err)
	}
	before := live(t, "ssa-legacy")
	if ownedBy(t, before, "legacy-manager", metav1.ManagedFieldsOperationUpdate) == nil {
		t.Fatalf("precondition: the legacy Update entry must exist, managedFields=%v", before.ManagedFields)
	}

	r := newReconciler(t, configMap("ssa-legacy", map[string]string{"a": "1"}, nil), []string{".metadata"})
	reconcileOnce(t, r)
	cm := live(t, "ssa-legacy")
	if _, still := cm.Data["stale"]; still {
		t.Fatalf("a stale field owned by the folded legacy entry must be removed, got %v", cm.Data)
	}
	if cm.Labels["owner"] != "legacy" {
		t.Fatalf("labels under the excluded .metadata must survive the fold, got %v", cm.Labels)
	}
	if ownedBy(t, cm, "legacy-manager", metav1.ManagedFieldsOperationUpdate) != nil {
		t.Fatalf("the legacy entry must be gone, managedFields=%v", cm.ManagedFields)
	}
	set := ownedBy(t, cm, FieldManager, metav1.ManagedFieldsOperationApply)
	if !owns(set, "data", "a") || owns(set, "metadata", "labels") {
		t.Fatalf("after the fold the reconciler owns the rendered data and not the excluded metadata, got %v", set)
	}
}

func TestSSA_NoOpApplyKeepsResourceVersion(t *testing.T) {
	needEnvtest(t)
	r := newReconciler(t, configMap("ssa-noop", map[string]string{"a": "1"}, map[string]string{"l": "v"}), []string{".metadata"})
	reconcileOnce(t, r)
	reconcileOnce(t, r) // releases ownership of the labels taken at creation
	rv := live(t, "ssa-noop").ResourceVersion
	reconcileOnce(t, r)
	reconcileOnce(t, r)
	if got := live(t, "ssa-noop").ResourceVersion; got != rv {
		t.Fatalf("a reconcile that changes nothing must not write: resourceVersion %s -> %s", rv, got)
	}
}

// A field another manager applied, which the template never renders, is not this reconciler's.
func TestSSA_OtherManagersFieldIsLeft(t *testing.T) {
	needEnvtest(t)
	r := newReconciler(t, configMap("ssa-other", map[string]string{"a": "1"}, nil), nil)
	reconcileOnce(t, r)
	applyAs(t, "other-controller", configMap("ssa-other", map[string]string{"theirs": "x"}, nil))
	reconcileOnce(t, r)
	cm := live(t, "ssa-other")
	if cm.Data["theirs"] != "x" || cm.Data["a"] != "1" {
		t.Fatalf("another manager's field must be left and ours enforced, got %v", cm.Data)
	}
}

// Pure: the trimming of a managedFields set by excluded prefixes.
func TestWithoutExcluded(t *testing.T) {
	set := fieldpath.NewSet()
	raw := `{"f:data":{".":{},"f:a":{},"f:b":{}},"f:metadata":{"f:labels":{".":{},"f:x":{}},"f:annotations":{".":{},"f:y":{}}},"f:rules":{}}`
	if err := set.FromJSON(bytes.NewReader([]byte(raw))); err != nil {
		t.Fatal(err)
	}
	kept := withoutExcluded(set, [][]string{{"data", "b"}, {"metadata", "labels"}, {"rules"}})
	want := map[string]bool{".data": true, ".data.a": true, ".metadata.annotations": true, ".metadata.annotations.y": true}
	got := map[string]bool{}
	kept.Iterate(func(p fieldpath.Path) { got[p.String()] = true })
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("kept %v, want %v", got, want)
	}
}

// Pure: an index in an excluded path releases the list it indexes, whole.
func TestExcludedFieldPaths_IndexStopsAtTheList(t *testing.T) {
	r := &LockedResourceReconciler{ExcludePaths: []string{".rules[0].verbs", ".metadata.labels", "$.data['a.b']", "unrooted.path", ".spec.containers[2]"}}
	got, err := r.excludedFieldPaths()
	if err != nil {
		t.Fatal(err)
	}
	want := [][]string{{"rules"}, {"metadata", "labels"}, {"data", "a.b"}, {"spec", "containers"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if _, err := (&LockedResourceReconciler{ExcludePaths: []string{".rules[-1]"}}).excludedFieldPaths(); err == nil {
		t.Fatal("a malformed excluded path must be reported")
	}
}
