package lockedresource

import (
	"reflect"
	"strings"
	"sync"
	"testing"
	"text/template"

	utilsapi "github.com/redhat-cop/operator-utils/api/v1alpha1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/rest"
)

func TestGetMergePathFromJSONPath(t *testing.T) {
	cases := map[string]string{
		".spec.replicas":            "/spec/replicas",
		"$.spec.replicas":           "/spec/replicas",
		".metadata":                 "/metadata",
		".rules[0]":                 "/rules/0",
		".rules[10]":                "/rules/10",
		".spec.containers[2].image": "/spec/containers/2/image",
		".data['a.b']":              "/data/a.b",
		`.data["x/y"]`:              "/data/x~1y",
		".rules[0].verbs[1]":        "/rules/0/verbs/1",
	}
	for in, want := range cases {
		if got := getMergePathFromJSONPath(in); got != want {
			t.Errorf("%q -> %q, want %q", in, got, want)
		}
	}
}

func TestNormalizeJSONPaths(t *testing.T) {
	got := NormalizeJSONPaths([]string{"$.rules[0]", ".data['a.b']", ".spec"})
	want := []string{".rules.0", ".data.a.b", ".spec"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestFilterOutPaths_IndexedPathRemovesTheRightElement(t *testing.T) {
	obj := &unstructured.Unstructured{Object: map[string]interface{}{
		"kind": "Role",
		"rules": []interface{}{
			map[string]interface{}{"verbs": []interface{}{"get"}},
			map[string]interface{}{"verbs": []interface{}{"list", "watch"}},
		},
	}}
	out, err := FilterOutPaths(obj, []string{".rules[0]"})
	if err != nil {
		t.Fatalf("an indexed path must be a valid remove, got %v", err)
	}
	rules := out.Object["rules"].([]interface{})
	if len(rules) != 1 || !reflect.DeepEqual(rules[0].(map[string]interface{})["verbs"], []interface{}{"list", "watch"}) {
		t.Errorf("expected only the second rule to remain, got %v", rules)
	}
	out, err = FilterOutPaths(obj, []string{".rules[1].verbs[0]"})
	if err != nil {
		t.Fatal(err)
	}
	if verbs := out.Object["rules"].([]interface{})[1].(map[string]interface{})["verbs"]; !reflect.DeepEqual(verbs, []interface{}{"watch"}) {
		t.Errorf("expected the first verb of the second rule removed, got %v", verbs)
	}
	// A path that does not exist is still ignored, as before.
	if _, err := FilterOutPaths(obj, []string{".rules[5]", ".nope"}); err != nil {
		t.Errorf("missing paths must be ignored, got %v", err)
	}
}

// The parse cache is shared by every controller in the process; it must survive concurrent use and
// must not hand a template parsed with one rest config to a caller with another.
func TestGetTemplate_ConcurrentAndKeyedByConfig(t *testing.T) {
	templates = sync.Map{} // the cache is process-global; count only what this test inserts
	cfgA, cfgB := &rest.Config{Host: "https://a"}, &rest.Config{Host: "https://b"}
	texts := []string{"a: 1\n", "b: 2\n", "c: {{ .Name }}\n", "d: 4\n"}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		for _, text := range texts {
			for _, cfg := range []*rest.Config{cfgA, cfgB, nil} {
				wg.Add(1)
				go func(text string, cfg *rest.Config) {
					defer wg.Done()
					if _, err := getTemplate(&utilsapi.LockedResourceTemplate{ObjectTemplate: text}, cfg, innerlog); err != nil {
						t.Errorf("getTemplate: %v", err)
					}
				}(text, cfg)
			}
		}
	}
	wg.Wait()
	a, _ := templates.Load(templateKey{text: texts[0], configKey: restConfigCacheKey(cfgA)})
	b, _ := templates.Load(templateKey{text: texts[0], configKey: restConfigCacheKey(cfgB)})
	if a == nil || b == nil || a.(*template.Template) == b.(*template.Template) {
		t.Errorf("the same text must be cached separately per rest config")
	}
	entries := 0
	templates.Range(func(_, _ any) bool { entries++; return true })
	if entries != len(texts)*3 {
		t.Errorf("expected %d cache entries (texts x configs), got %d", len(texts)*3, entries)
	}
}

// A caller that builds a fresh *rest.Config per call (rest.CopyConfig, a literal) must not add a
// cache entry per call: the key is the config's material, not its address.
func TestGetTemplate_KeyIsConfigMaterialNotPointer(t *testing.T) {
	templates = sync.Map{}
	const text = "kind: ConfigMap\n"
	for i := 0; i < 50; i++ {
		if _, err := getTemplate(&utilsapi.LockedResourceTemplate{ObjectTemplate: text}, &rest.Config{Host: "https://same"}, innerlog); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 2; i++ {
		if _, err := getTemplate(&utilsapi.LockedResourceTemplate{ObjectTemplate: text}, nil, innerlog); err != nil {
			t.Fatal(err)
		}
	}
	entries := 0
	templates.Range(func(_, _ any) bool { entries++; return true })
	if entries != 2 {
		t.Errorf("expected one entry for the identical configs and one for nil, got %d", entries)
	}
}

func TestFilterOutPaths_NegativeIndexIsAnError(t *testing.T) {
	obj := &unstructured.Unstructured{Object: map[string]interface{}{"kind": "Role", "rules": []interface{}{map[string]interface{}{"id": 0}, map[string]interface{}{"id": 1}}}}
	for _, path := range []string{".rules.-2", ".rules[-1]", ".rules.-1.id"} {
		_, err := FilterOutPaths(obj, []string{path})
		if err == nil || !strings.Contains(err.Error(), "negative index") {
			t.Errorf("%q: a negative index is a misconfiguration and must be reported as such, got %v", path, err)
		}
	}
	// a non-negative past-the-end index stays a no-op
	if _, err := FilterOutPaths(obj, []string{".rules[5]"}); err != nil {
		t.Errorf("past-the-end index must be ignored, got %v", err)
	}
}

// An unrooted path kept its pre-fix meaning (no-op); only ".x" and "$.x" name the root.
func TestGetMergePathFromJSONPath_UnrootedStaysUnrooted(t *testing.T) {
	if got := getMergePathFromJSONPath("spec.replicas"); got != "spec/replicas" {
		t.Errorf("unrooted path must stay unrooted, got %q", got)
	}
	obj := &unstructured.Unstructured{Object: map[string]interface{}{"kind": "Deployment", "spec": map[string]interface{}{"replicas": int64(3)}}}
	out, err := FilterOutPaths(obj, []string{"spec.replicas"})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := out.Object["spec"].(map[string]interface{})["replicas"]; !ok {
		t.Error("unrooted spec.replicas must remain a no-op as before")
	}
}

// A typo in an excluded path is reported, not retargeted: before this check ".data." removed all of
// "data" and ".data[" removed nothing, both silently (measured in review).
func TestFilterOutPaths_MalformedPathIsAnError(t *testing.T) {
	obj := &unstructured.Unstructured{Object: map[string]interface{}{"kind": "ConfigMap", "data": map[string]interface{}{"key": "value", "a.b": "dotted"}}}
	for _, path := range []string{".data.", ".data[", ".data[]", ".data..key", "$.data[key]", ".data['a.b']."} {
		_, err := FilterOutPaths(obj, []string{path})
		if err == nil || !(strings.Contains(err.Error(), "malformed bracket") || strings.Contains(err.Error(), "empty segment")) {
			t.Errorf("%q must be reported as malformed, got %v", path, err)
		}
	}
	// well-formed spellings, including a quoted key that contains a dot, still work
	for _, path := range []string{".data.key", "$.data.key", ".data['a.b']", ".data[\"a.b\"]", "/data/key"} {
		out, err := FilterOutPaths(obj, []string{path})
		if err != nil {
			t.Errorf("%q is well formed, got %v", path, err)
			continue
		}
		if data := out.Object["data"].(map[string]interface{}); len(data) != 1 {
			t.Errorf("%q must remove exactly one key, got %v", path, data)
		}
	}
}
