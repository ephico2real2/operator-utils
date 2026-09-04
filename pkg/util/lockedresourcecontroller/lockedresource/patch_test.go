package lockedresource

import (
	"reflect"
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
	a, _ := templates.Load(templateKey{text: texts[0], config: cfgA})
	b, _ := templates.Load(templateKey{text: texts[0], config: cfgB})
	if a == nil || b == nil || a.(*template.Template) == b.(*template.Template) {
		t.Errorf("the same text must be cached separately per rest config")
	}
	entries := 0
	templates.Range(func(_, _ any) bool { entries++; return true })
	if entries != len(texts)*3 {
		t.Errorf("expected %d cache entries (texts x configs), got %d", len(texts)*3, entries)
	}
}
