package lockedresourcecontroller

import (
	"encoding/json"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func patchFor(t *testing.T, expected, actual map[string]interface{}, excluded []string) map[string]interface{} {
	t.Helper()
	lor := &LockedResourceReconciler{Resource: unstructured.Unstructured{Object: expected}, ExcludePaths: excluded}
	b, err := lor.createPatchWithNullFields(&lor.Resource, &unstructured.Unstructured{Object: actual})
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]interface{}
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestCreatePatchWithNullFields(t *testing.T) {
	cm := func(extra map[string]interface{}) map[string]interface{} {
		m := map[string]interface{}{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]interface{}{"name": "x"}}
		for k, v := range extra {
			m[k] = v
		}
		return m
	}
	t.Run("a live-only field outside any excluded path is nulled (issue 194 behaviour kept)", func(t *testing.T) {
		p := patchFor(t, cm(nil), cm(map[string]interface{}{"data": map[string]interface{}{"a": "1"}}), []string{".metadata", ".status"})
		if v, ok := p["data"]; !ok || v != nil {
			t.Errorf("expected data: null, got %v", p)
		}
	})
	t.Run("a live-only parent of an excluded path is left alone", func(t *testing.T) {
		p := patchFor(t, cm(nil), cm(map[string]interface{}{"data": map[string]interface{}{"foo": "1", "bar": "2"}}), []string{".metadata", ".data.foo"})
		if _, ok := p["data"]; ok {
			t.Errorf("data must not appear in the patch when .data.foo is excluded, got %v", p)
		}
	})
	t.Run("an excluded path itself is never nulled, siblings still are", func(t *testing.T) {
		expected := cm(map[string]interface{}{"data": map[string]interface{}{"keep": "1"}})
		actual := cm(map[string]interface{}{"data": map[string]interface{}{"keep": "1", "foo": "x", "gone": "y"}})
		p := patchFor(t, expected, actual, []string{".metadata", ".data.foo"})
		data := p["data"].(map[string]interface{})
		if _, ok := data["foo"]; ok {
			t.Errorf("the excluded key must not be in the patch, got %v", data)
		}
		if v, ok := data["gone"]; !ok || v != nil {
			t.Errorf("the non-excluded live-only key must be nulled, got %v", data)
		}
	})
	t.Run("indexed excluded paths are understood", func(t *testing.T) {
		expected := cm(nil)
		actual := cm(map[string]interface{}{"rules": []interface{}{map[string]interface{}{"verbs": []interface{}{"get"}}}})
		p := patchFor(t, expected, actual, []string{".rules[0]"})
		if _, ok := p["rules"]; ok {
			t.Errorf("rules must be left alone when .rules[0] is excluded, got %v", p)
		}
	})
	t.Run("metadata and status are never nulled", func(t *testing.T) {
		p := patchFor(t, cm(nil), cm(map[string]interface{}{"status": map[string]interface{}{"phase": "Active"}}), []string{".metadata", ".status"})
		if _, ok := p["status"]; ok {
			t.Errorf("status must not be in the patch, got %v", p)
		}
	})
}
