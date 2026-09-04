package lockedresource

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	jsonpatch "github.com/evanphx/json-patch"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func FilterOutPaths(obj *unstructured.Unstructured, jsonPaths []string) (*unstructured.Unstructured, error) {
	doc, err := obj.MarshalJSON()
	if err != nil {
		innerlog.Error(err, "unable to marshall", "unstructured", obj)
		return &unstructured.Unstructured{}, err
	}

	patches, err := createPatchesFromJSONPaths(jsonPaths)
	if err != nil {
		innerlog.Error(err, "unable to create patches from", "jsonPaths", jsonPaths)
		return &unstructured.Unstructured{}, err
	}
	for _, patch := range patches {
		decodedPatch, err := jsonpatch.DecodePatch(patch)
		if err != nil {
			innerlog.Error(err, "unable to decode", "patch", string(patch))
			return &unstructured.Unstructured{}, err
		}
		doc1, err := decodedPatch.Apply(doc)
		if err != nil {
			// An excluded path that the document does not have (a missing key, or an array index past
			// the end) is not an error: there is nothing to exclude.
			if strings.Contains(err.Error(), "Unable to remove nonexistent key") ||
				strings.Contains(err.Error(), "remove operation does not apply: doc is missing path") ||
				strings.Contains(err.Error(), "Unable to access invalid index") {
				continue
			}
			innerlog.Error(err, "unable to apply", "patch", patch, "to json", string(doc))
			return &unstructured.Unstructured{}, err
		}
		doc = doc1
	}

	var result = &unstructured.Unstructured{}

	err = result.UnmarshalJSON(doc)

	if err != nil {
		innerlog.Error(err, "unable to unMarshall", "json", doc)
		return &unstructured.Unstructured{}, err
	}

	return result, nil
}

// Patch represents a patch operation
type Patch struct {
	Operation string `json:"op"`
	Path      string `json:"path"`
}

var negativeIndexSegment = regexp.MustCompile(`/-\d+(/|$)`)

func createPatchesFromJSONPaths(jsonPaths []string) ([][]byte, error) {
	result := [][]byte{}
	for _, jsonPath := range jsonPaths {
		if err := malformedPath(jsonPath); err != nil {
			return [][]byte{}, err
		}
		pointer := getMergePathFromJSONPath(jsonPath)
		// json-patch accepts a negative index (counting from the end) and reports one that is out
		// of range with the same text as a past-the-end index. An excluded path is a fixed
		// location, never "the last element", so a negative index is a misconfiguration to report,
		// not a missing element to ignore.
		if negativeIndexSegment.MatchString(pointer) {
			return [][]byte{}, fmt.Errorf("excluded path %q has a negative index; indexes count from 0", jsonPath)
		}
		patch := []Patch{
			{
				Operation: "remove",
				Path:      pointer,
			},
		}
		mpatch, err := json.Marshal(patch)
		if err != nil {
			innerlog.Error(err, "unable to marshal", "patch", patch)
			return [][]byte{}, err
		}
		result = append(result, mpatch)
	}
	return result, nil
}

var indexedSegment = regexp.MustCompile(`\[\s*(?:(-?\d+)|'([^']*)'|"([^"]*)")\s*\]`)

// malformedPath reports an excluded path that cannot name a field: a bracket that is not a complete
// [n], ['key'] or ["key"] expression, or an empty segment (a trailing dot, or two dots in a row).
// Measured in review before this check: ".data[" removed nothing, silently, and ".data." removed all
// of "data" because a trailing dot was trimmed. A typo in an excluded path must be reported, not
// retargeted. The lone root path "." (or "$") is left alone; json-patch rejects it on its own.
func malformedPath(jsonPath string) error {
	stripped := indexedSegment.ReplaceAllString(strings.TrimPrefix(jsonPath, "$"), "")
	if strings.ContainsAny(stripped, "[]") {
		return fmt.Errorf("excluded path %q has a malformed bracket expression; use [n], ['key'] or [\"key\"]", jsonPath)
	}
	if len(stripped) > 1 && (strings.HasSuffix(stripped, ".") || strings.Contains(stripped, "..")) {
		return fmt.Errorf("excluded path %q has an empty segment", jsonPath)
	}
	return nil
}

// getMergePathFromJSONPath turns a dotted JSON path (".spec.replicas", ".rules[0]", "$.data['a.b']")
// into the RFC 6901 JSON pointer a remove operation needs ("/spec/replicas", "/rules/0", "/data/a.b").
// The previous conversion dropped the last two characters of any path ending in "]", so ".rules[0]"
// became "/rules/" (an error on every reconcile) and ".rules[10]" became "/rules/1" (the wrong
// element removed silently).
func getMergePathFromJSONPath(jsonPath string) string {
	// Only a path that names the root (".x" or "$.x") gets the root slash. Before the index fix an
	// unrooted "spec.replicas" produced "spec/replicas", which json-patch treated as a no-op; keeping
	// that (rather than silently turning it into "/spec/replicas") preserves what existing callers
	// observed. The rooted spelling is the documented one.
	rooted := strings.HasPrefix(jsonPath, "$") || strings.HasPrefix(jsonPath, ".")
	jsonPath = strings.TrimPrefix(jsonPath, "$")
	// [n] and ['key'] / ["key"] become dotted segments; a bracketed key may itself contain dots or
	// slashes, so it is escaped per RFC 6901 before the dots are turned into separators.
	jsonPath = indexedSegment.ReplaceAllStringFunc(jsonPath, func(m string) string {
		sub := indexedSegment.FindStringSubmatch(m)
		seg := sub[1]
		if seg == "" {
			seg = sub[2]
			if seg == "" {
				seg = sub[3]
			}
			seg = strings.NewReplacer("~", "~0", "/", "~1", ".", "\x00").Replace(seg)
		}
		return "." + seg
	})
	pointer := strings.ReplaceAll(jsonPath, ".", "/")
	if rooted && !strings.HasPrefix(pointer, "/") {
		pointer = "/" + pointer
	}
	// Restore the dots that belonged to bracketed keys.
	return strings.ReplaceAll(pointer, "\x00", ".")
}

// NormalizeJSONPaths rewrites excluded paths into the dotted form the null-field logic compares
// against: no leading "$", indexes and bracketed keys as ".segment", so ".rules[0]" and "$.rules.0"
// are the same path.
func NormalizeJSONPaths(jsonPaths []string) []string {
	out := make([]string, 0, len(jsonPaths))
	for _, jp := range jsonPaths {
		out = append(out, strings.ReplaceAll(getMergePathFromJSONPath(jp), "/", "."))
	}
	return out
}
