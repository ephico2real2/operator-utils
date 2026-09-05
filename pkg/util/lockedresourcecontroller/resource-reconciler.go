package lockedresourcecontroller

import (
	"bytes"
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"sync"

	"github.com/go-logr/logr"
	"github.com/redhat-cop/operator-utils/pkg/util"
	"github.com/redhat-cop/operator-utils/pkg/util/apis"
	"github.com/redhat-cop/operator-utils/pkg/util/dynamicclient"
	"github.com/redhat-cop/operator-utils/pkg/util/lockedresourcecontroller/lockedresource"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/csaupgrade"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
	"sigs.k8s.io/structured-merge-diff/v4/fieldpath"
)

// FieldManager is the server-side-apply field manager every LockedResourceReconciler applies with.
// The API server keys field ownership by this name, so it is one name for the whole process: a
// second name would look like a second actor and the two would take fields from each other.
var FieldManager = "lockedresourcecontroller"

// LegacyFieldManagers names field managers whose client-side `Update` entries are folded into
// FieldManager the first time this reconciler meets an object. Before it applied server-side, this
// library created and patched objects through the manager's client, whose field manager is the
// binary's name (the API server takes it from the user agent, up to the first "/"); on such an
// object a field the template no longer renders is owned by that entry, and a plain apply would
// never remove it. Measured on a live cluster: every object the operator had created carried one
// entry, manager "manager", operation Update. A consumer with a different history sets this before
// starting its managers; an empty list folds nothing.
var LegacyFieldManagers = []string{legacyManagerFromUserAgent()}

func legacyManagerFromUserAgent() string {
	ua := rest.DefaultKubernetesUserAgent()
	if i := strings.Index(ua, "/"); i > 0 {
		return ua[:i]
	}
	return ua
}

// LockedResourceReconciler is a reconciler that will lock down a resource to prevent changes from external events.
// This reconciler can be configured to ignore a set of json path. Changed occurring on the ignored path will be ignored, and therefore allowed by the reconciler.
//
// It enforces with server-side apply: every reconcile applies the rendered object under FieldManager
// with force, so a field another actor changed is restored, a field the template no longer renders
// is removed (the server deletes what its owner stops sending), and a field nobody but another
// manager owns is left alone. An excluded path is set when the object is created and never applied
// again; before each apply the reconciler releases its ownership of every excluded path, so the
// server keeps the field as it is instead of deleting it.
type LockedResourceReconciler struct {
	Resource     unstructured.Unstructured
	ExcludePaths []string
	util.ReconcilerBase
	status         []metav1.Condition
	statusChange   chan<- event.GenericEvent
	statusLock     sync.Mutex
	parentObject   client.Object
	firstReconcile chan event.GenericEvent
	log            logr.Logger
}

// NewLockedObjectReconciler returns a new reconcile.Reconciler
func NewLockedObjectReconciler(mgr manager.Manager, object unstructured.Unstructured, excludePaths []string, statusChange chan<- event.GenericEvent, parentObject client.Object) (*LockedResourceReconciler, error) {

	controllername := "resource-reconciler"

	reconciler := &LockedResourceReconciler{
		log:            ctrl.Log.WithName(controllername).WithName(apis.GetKeyShort(parentObject)).WithName(apis.GetKeyLong(&object)),
		ReconcilerBase: util.NewFromManager(mgr, mgr.GetEventRecorderFor(controllername+"_"+apis.GetKeyLong(&object))),
		Resource:       object,
		ExcludePaths:   excludePaths,
		statusChange:   statusChange,
		parentObject:   parentObject,
		statusLock:     sync.Mutex{},
		firstReconcile: make(chan event.GenericEvent),
		status: []metav1.Condition([]metav1.Condition{{
			Type:               "Initializing",
			LastTransitionTime: metav1.Now(),
			Status:             metav1.ConditionTrue,
			ObservedGeneration: object.GetGeneration(),
			Reason:             "ReconcilerManagerRestarting",
		}}),
	}

	go func() {
		reconciler.firstReconcile <- event.GenericEvent{
			Object: &object,
		}
	}()

	controller, err := controller.New("controller_locked_object_"+apis.GetKeyLong(&object), mgr, controller.Options{Reconciler: reconciler})
	if err != nil {
		reconciler.log.Error(err, "unable to create new controller", "with reconciler", reconciler)
		return &LockedResourceReconciler{}, err
	}

	gvk := object.GetObjectKind().GroupVersionKind()
	groupVersion := schema.GroupVersion{Group: gvk.Group, Version: gvk.Version}

	mgr.GetScheme().AddKnownTypes(groupVersion, &object)

	err = controller.Watch(source.Kind(mgr.GetCache(), &object), &handler.EnqueueRequestForObject{}, &resourceModifiedPredicate{
		name:      object.GetName(),
		namespace: object.GetNamespace(),
		lrr:       reconciler,
	})
	if err != nil {
		reconciler.log.Error(err, "unable to create new watch", "with source", object)
		return &LockedResourceReconciler{}, err
	}

	err = controller.Watch(
		&source.Channel{Source: reconciler.firstReconcile},
		&handler.EnqueueRequestForObject{},
	)
	if err != nil {
		return &LockedResourceReconciler{}, err
	}

	return reconciler, nil
}

// Reconcile contains the reconcile logic for LockedResourceReconciler
func (lor *LockedResourceReconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	lor.log.Info("reconcile called for", "object", apis.GetKeyLong(&lor.Resource), "request", request)
	ctx = context.WithValue(ctx, "restConfig", lor.GetRestConfig())
	ctx = log.IntoContext(ctx, lor.log)
	client, err := dynamicclient.GetDynamicClientOnUnstructured(ctx, &lor.Resource)
	if err != nil {
		lor.log.Error(err, "unable to get dynamicClient", "on object", lor.Resource)
		return lor.manageErrorNoInstance(err)
	}
	instance, err := client.Get(ctx, lor.Resource.GetName(), v1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			// Creation applies the whole rendered object, excluded paths included: an excluded
			// path is "set once, then left alone", as it always was. The next reconcile releases
			// the ownership this apply takes of it (see releaseOwnership).
			applied, err := lor.apply(ctx, client, lor.Resource.DeepCopy())
			if err != nil {
				lor.log.Error(err, "unable to create", "object", lor.Resource)
				return lor.manageErrorNoInstance(err)
			}
			lor.log.V(1).Info("created")
			return lor.manageSuccess(applied)
		}
		lor.log.Error(err, "unable to lookup", "object", lor.Resource)
		return lor.manageErrorNoInstance(err)
	}
	if err := lor.releaseOwnership(ctx, client, instance); err != nil {
		lor.log.Error(err, "unable to release ownership of excluded paths", "object", lor.Resource)
		return lor.manageError(instance, err)
	}
	desired, err := lor.desired()
	if err != nil {
		lor.log.Error(err, "unable to filter out", "excluded paths", lor.ExcludePaths, "from object", lor.Resource)
		return lor.manageError(instance, err)
	}
	applied, err := lor.apply(ctx, client, desired)
	if err != nil {
		lor.log.Error(err, "unable to apply", "object", desired)
		return lor.manageError(instance, err)
	}
	if applied.GetResourceVersion() == instance.GetResourceVersion() {
		lor.log.V(1).Info("determined that resources are equal")
	} else {
		lor.log.V(1).Info("determined that resources are NOT equal; applied", "resourceVersion", applied.GetResourceVersion())
	}
	return lor.manageSuccess(applied)
}

// desired is what the reconciler applies after creation: the rendered object without its excluded
// paths, so this manager never sends them, with the identity the apply needs restored in case an
// exclusion covered it (".metadata" is a common one).
func (lor *LockedResourceReconciler) desired() (*unstructured.Unstructured, error) {
	desired, err := lockedresource.FilterOutPaths(&lor.Resource, lor.ExcludePaths)
	if err != nil {
		return nil, err
	}
	desired.SetAPIVersion(lor.Resource.GetAPIVersion())
	desired.SetKind(lor.Resource.GetKind())
	desired.SetName(lor.Resource.GetName())
	desired.SetNamespace(lor.Resource.GetNamespace())
	return desired, nil
}

func (lor *LockedResourceReconciler) apply(ctx context.Context, client dynamic.ResourceInterface, obj *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	data, err := json.Marshal(obj.Object)
	if err != nil {
		return nil, err
	}
	force := true
	return client.Patch(ctx, obj.GetName(), types.ApplyPatchType, data, v1.PatchOptions{FieldManager: FieldManager, Force: &force})
}

// releaseOwnership rewrites this manager's entry in the live object's managedFields before the
// apply, and sends nothing when there is nothing to change:
//
//  1. a client-side Update entry left by an earlier version of this library (LegacyFieldManagers)
//     is folded into FieldManager with client-go's csaupgrade, so a field that entry owns and the
//     template no longer renders is removed by the apply that follows, instead of surviving;
//  2. every path under an excluded path is dropped from FieldManager's set. Server-side apply
//     deletes a field its owner stops sending; an excluded field must stay as it is, owned by
//     nobody, and removing it from the set is exactly that.
//
// The patch replaces resourceVersion too, so a concurrent write makes it fail with a conflict and
// the reconcile is retried against the newer object rather than overwriting its managed fields.
func (lor *LockedResourceReconciler) releaseOwnership(ctx context.Context, client dynamic.ResourceInterface, live *unstructured.Unstructured) error {
	upgraded := live.DeepCopy()
	if err := csaupgrade.UpgradeManagedFields(upgraded, sets.New(LegacyFieldManagers...), FieldManager); err != nil {
		return err
	}
	excluded, err := lor.excludedFieldPaths()
	if err != nil {
		return err
	}
	entries := upgraded.GetManagedFields()
	for i := range entries {
		entry := &entries[i]
		if entry.Manager != FieldManager || entry.Operation != v1.ManagedFieldsOperationApply || entry.FieldsV1 == nil {
			continue
		}
		owned := fieldpath.NewSet()
		if err := owned.FromJSON(bytes.NewReader(entry.FieldsV1.Raw)); err != nil {
			return err
		}
		kept := withoutExcluded(owned, excluded)
		if kept.Size() == owned.Size() {
			continue
		}
		raw, err := kept.ToJSON()
		if err != nil {
			return err
		}
		entry.FieldsV1 = &v1.FieldsV1{Raw: raw}
	}
	before, err := json.Marshal(live.GetManagedFields())
	if err != nil {
		return err
	}
	after, err := json.Marshal(entries)
	if err != nil {
		return err
	}
	if bytes.Equal(before, after) {
		return nil
	}
	patch, err := json.Marshal([]map[string]interface{}{
		{"op": "replace", "path": "/metadata/managedFields", "value": entries},
		{"op": "replace", "path": "/metadata/resourceVersion", "value": live.GetResourceVersion()},
	})
	if err != nil {
		return err
	}
	lor.log.V(1).Info("releasing ownership", "legacyManagers", LegacyFieldManagers, "excludedPaths", lor.ExcludePaths)
	_, err = client.Patch(ctx, live.GetName(), types.JSONPatchType, patch, v1.PatchOptions{})
	return err
}

// excludedFieldPaths converts the excluded paths into managedFields prefixes. An index stops the
// prefix at the list: server-side apply owns a list whole (RBAC rules and subjects are atomic), so
// one element cannot be released and the whole list is.
func (lor *LockedResourceReconciler) excludedFieldPaths() ([][]string, error) {
	out := make([][]string, 0, len(lor.ExcludePaths))
	for _, jsonPath := range lor.ExcludePaths {
		segments, err := lockedresource.FieldPath(jsonPath)
		if err != nil {
			return nil, err
		}
		for i, segment := range segments {
			if _, err := strconv.Atoi(segment); err == nil {
				segments = segments[:i]
				break
			}
		}
		if len(segments) > 0 {
			out = append(out, segments)
		}
	}
	return out, nil
}

// withoutExcluded returns the members of set that are not under any excluded prefix.
func withoutExcluded(set *fieldpath.Set, excluded [][]string) *fieldpath.Set {
	kept := fieldpath.NewSet()
	set.Iterate(func(p fieldpath.Path) {
		for _, prefix := range excluded {
			if fieldPathHasPrefix(p, prefix) {
				return
			}
		}
		kept.Insert(p)
	})
	return kept
}

func fieldPathHasPrefix(p fieldpath.Path, prefix []string) bool {
	if len(p) < len(prefix) {
		return false
	}
	for i, name := range prefix {
		if p[i].FieldName == nil || *p[i].FieldName != name {
			return false
		}
	}
	return true
}

type resourceModifiedPredicate struct {
	name      string
	namespace string
	lrr       *LockedResourceReconciler
	predicate.Funcs
}

// Update implements default UpdateEvent filter for validating resource version change
func (p *resourceModifiedPredicate) Update(e event.UpdateEvent) bool {
	if e.ObjectNew.GetNamespace() == p.namespace && e.ObjectNew.GetName() == p.name {
		return true
	}
	return false
}

func (p *resourceModifiedPredicate) Create(e event.CreateEvent) bool {
	if e.Object.GetNamespace() == p.namespace && e.Object.GetName() == p.name {
		return true
	}
	return false
}

func (p *resourceModifiedPredicate) Delete(e event.DeleteEvent) bool {
	if e.Object.GetNamespace() == p.namespace && e.Object.GetName() == p.name {
		// we return true only if the enclosing namespace is not also being deleted
		if e.Object.GetNamespace() != "" {
			namespace := corev1.Namespace{}
			// Use non-cached client since client's cache may be namespaced
			err := p.lrr.GetAPIReader().Get(context.TODO(), types.NamespacedName{Name: e.Object.GetNamespace()}, &namespace)
			if err != nil {
				p.lrr.log.Error(err, "unable to retrieve ", "namespace", "e.Meta.GetNamespace()")
				// If the request failed return "true" as the k8s API will deny any create/update operation in a
				// Namespace that's marked for termination. Returning false here causes resources not being reconciled
				// in namespaced installations (Namespace requires a client with cluster scoped permissions)
				return true
			}
			if util.IsBeingDeleted(&namespace) {
				return false
			}
		}
		return true
	}
	return false
}

func (lor *LockedResourceReconciler) manageError(instance *unstructured.Unstructured, err error) (reconcile.Result, error) {
	condition := metav1.Condition{
		Type:               apis.ReconcileError,
		LastTransitionTime: metav1.Now(),
		Message:            err.Error(),
		Reason:             apis.ReconcileErrorReason,
		Status:             metav1.ConditionTrue,
		ObservedGeneration: func() int64 {
			if instance != nil {
				return instance.GetGeneration()
			} else {
				return 0
			}
		}(),
	}
	lor.setStatus(apis.AddOrReplaceCondition(condition, lor.GetStatus()))
	return reconcile.Result{}, err
}

func (lor *LockedResourceReconciler) manageErrorNoInstance(err error) (reconcile.Result, error) {
	condition := metav1.Condition{
		Type:               apis.ReconcileError,
		LastTransitionTime: metav1.Now(),
		Message:            err.Error(),
		Reason:             apis.ReconcileErrorReason,
		Status:             metav1.ConditionTrue,
		ObservedGeneration: 0,
	}
	lor.setStatus(apis.AddOrReplaceCondition(condition, lor.GetStatus()))
	return reconcile.Result{}, err
}

func (lor *LockedResourceReconciler) manageSuccess(instance *unstructured.Unstructured) (reconcile.Result, error) {
	condition := metav1.Condition{
		Type:               apis.ReconcileSuccess,
		LastTransitionTime: metav1.Now(),
		Reason:             apis.ReconcileSuccessReason,
		Status:             metav1.ConditionTrue,
		ObservedGeneration: instance.GetGeneration(),
	}
	lor.setStatus(apis.AddOrReplaceCondition(condition, lor.GetStatus()))
	return reconcile.Result{}, nil
}

func (lor *LockedResourceReconciler) manageSuccessNoInstance() (reconcile.Result, error) {
	condition := metav1.Condition{
		Type:               apis.ReconcileSuccess,
		LastTransitionTime: metav1.Now(),
		Reason:             apis.ReconcileSuccessReason,
		Status:             metav1.ConditionTrue,
		ObservedGeneration: 0,
	}
	lor.setStatus(apis.AddOrReplaceCondition(condition, lor.GetStatus()))
	return reconcile.Result{}, nil
}

func (lor *LockedResourceReconciler) setStatus(status []metav1.Condition) {
	lor.statusLock.Lock()
	defer lor.statusLock.Unlock()
	lor.status = status
	if lor.statusChange != nil {
		lor.statusChange <- event.GenericEvent{
			Object: lor.parentObject,
		}
	}
}

// GetStatus returns the latest reconcile status
func (lor *LockedResourceReconciler) GetStatus() []metav1.Condition {
	lor.statusLock.Lock()
	defer lor.statusLock.Unlock()
	status := lor.status
	return status
}
