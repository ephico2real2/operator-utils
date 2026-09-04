package lockedresource

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"text/template"

	"github.com/go-logr/logr"
	utilsapi "github.com/redhat-cop/operator-utils/api/v1alpha1"
	utilstemplates "github.com/redhat-cop/operator-utils/pkg/util/templates"
	"github.com/scylladb/go-set/strset"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/yaml"
)

var innerlog = ctrl.Log.WithName("lockedresource")

// LockedResource represents a resource to be locked down by a LockedResourceReconciler within a LockedResourceManager
type LockedResource struct {
	// unstructured.Unstructured is the resource to be locked
	unstructured.Unstructured `json:"usntructured,omitempty"`
	// ExcludedPaths are the jsonPaths to be excluded when consider whether the resource has changed
	ExcludedPaths []string `json:"excludedPaths,omitempty"`
}

// AsListOfUnstructured given a list of LockedResource, returns a list of unstructured.Unstructured
func AsListOfUnstructured(lockedResources []LockedResource) []unstructured.Unstructured {
	unstructuredList := []unstructured.Unstructured{}
	for _, lockedResource := range lockedResources {
		unstructuredList = append(unstructuredList, lockedResource.Unstructured)
	}
	return unstructuredList
}

// GetKey returns the marshalled resource
func (lr *LockedResource) GetKey() string {
	bb, err := lr.Unstructured.MarshalJSON()
	if err != nil {
		innerlog.Error(err, "unable to marshall", "unstructured", lr.Unstructured)
		panic(err)
	}
	return string(bb)
}

// GetLockedResources turns an array of Resources as read from an API into an array of LockedResources, usable by the LockedResourceManager
func GetLockedResources(resources []utilsapi.LockedResource) ([]LockedResource, error) {
	lockedResources := []LockedResource{}
	for _, resource := range resources {
		bb, err := yaml.YAMLToJSON(resource.Object.Raw)
		if err != nil {
			innerlog.Error(err, "Error transforming yaml to json", "raw", resource.Object.Raw)
			return []LockedResource{}, err
		}
		obj := &unstructured.Unstructured{}
		err = json.Unmarshal(bb, obj)
		if err != nil {
			innerlog.Error(err, "Error unmarshalling json manifest", "manifest", string(bb))
			return []LockedResource{}, err
		}
		lockedResources = append(lockedResources, LockedResource{
			Unstructured:  *obj,
			ExcludedPaths: resource.ExcludedPaths,
		})
	}
	return lockedResources, nil
}

// templates caches parsed templates. It is read and written from every controller that renders
// through this package, concurrently, so it must be a sync.Map: the plain map it replaced was a
// data race and, under load at operator start, a fatal "concurrent map writes". The key includes
// the rest config, because the FuncMap (the lookup function in particular) is bound at parse time;
// a text parsed once with one config must not be served to a caller with another.
var templates sync.Map

type templateKey struct {
	text      string
	configKey string
}

// restConfigCacheKey identifies a rest config by the material that changes what `lookup` would
// see, not by pointer: callers that build or copy a config per reconcile would otherwise add an
// entry per call and grow the cache without bound (measured: 50 calls, 50 entries).
func restConfigCacheKey(config *rest.Config) string {
	if config == nil {
		return ""
	}
	return strings.Join([]string{
		config.Host,
		config.APIPath,
		config.Username,
		config.BearerToken,
		config.BearerTokenFile,
		string(config.CAData),
		config.CAFile,
		string(config.CertData),
		config.CertFile,
		config.Impersonate.UserName,
	}, "\x00")
}

// GetLockedResourcesFromTemplates Keep backwards compatability with existing consumers
func GetLockedResourcesFromTemplates(resources []utilsapi.LockedResourceTemplate, params interface{}) ([]LockedResource, error) {

	return GetLockedResourcesFromTemplatesWithRestConfig(resources, nil, params)
}

// GetLockedResourcesFromTemplatesWithRestConfig turns an array of ResourceTemplates as read from an API into an array of LockedResources using a params to process the templates
func GetLockedResourcesFromTemplatesWithRestConfig(resources []utilsapi.LockedResourceTemplate, config *rest.Config, params interface{}) ([]LockedResource, error) {
	lockedResources := []LockedResource{}
	ctx := context.TODO()
	ctx = context.WithValue(ctx, "restConfig", config)
	ctx = log.IntoContext(ctx, innerlog)
	for _, resource := range resources {
		template, err := getTemplate(&resource, config, innerlog)
		if err != nil {
			innerlog.Error(err, "unable to retrieve template for", "resource", resource)
			return []LockedResource{}, nil
		}
		objs, err := utilstemplates.ProcessTemplateArray(ctx, params, template)
		if err != nil {
			innerlog.Error(err, "unable to process template for", "resource", resource, "params", params)
			return []LockedResource{}, nil
		}
		for _, obj := range objs {
			lockedResources = append(lockedResources, LockedResource{
				Unstructured:  obj,
				ExcludedPaths: resource.ExcludedPaths,
			})
		}
	}
	return lockedResources, nil
}

func getTemplate(resource *utilsapi.LockedResourceTemplate, config *rest.Config, logger logr.Logger) (*template.Template, error) {
	key := templateKey{text: resource.ObjectTemplate, configKey: restConfigCacheKey(config)}
	if cached, ok := templates.Load(key); ok {
		return cached.(*template.Template), nil
	}
	tmpl, err := template.New(resource.ObjectTemplate).Funcs(utilstemplates.AdvancedTemplateFuncMap(config, logger)).Parse(resource.ObjectTemplate)
	if err != nil {
		innerlog.Error(err, "unable to parse", "template", resource.ObjectTemplate)
		return nil, err
	}
	// Two goroutines may parse the same text at once; both results are equivalent, keep the first.
	actual, _ := templates.LoadOrStore(key, tmpl)
	return actual.(*template.Template), nil
}

// DefaultExcludedPaths represents paths that are exlcuded by default in all resources
var DefaultExcludedPaths = []string{".metadata", ".status", ".spec.replicas"}

// DefaultExcludedPathsSet represents paths that are exlcuded by default in all resources
var DefaultExcludedPathsSet = strset.New(DefaultExcludedPaths...)

// GetResources returs an arrays of apis.Resources from an arya of LockedResources, useful for mass operations on the LockedResources
func GetResources(lockedResources []LockedResource) []client.Object {
	resources := []client.Object{}
	for _, lockedResource := range lockedResources {
		resources = append(resources, &lockedResource.Unstructured)
	}
	return resources
}
