package spec

import (
	"errors"
	"testing"
)

func TestParseValid(t *testing.T) {
	d, err := Parse([]byte(`{
	  "resources": [
	    {"name": "a", "kind": "Namespace"},
	    {"name": "b", "kind": "ConfigMap", "dependsOn": ["a"], "spec": {"k": "v"}}
	  ]
	}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(d.Resources) != 2 {
		t.Fatalf("want 2 resources, got %d", len(d.Resources))
	}
}

func TestValidateRejects(t *testing.T) {
	cases := []struct {
		name string
		doc  string
	}{
		{"empty", `{"resources": []}`},
		{"empty name", `{"resources": [{"kind": "K"}]}`},
		{"empty kind", `{"resources": [{"name": "a"}]}`},
		{"duplicate name", `{"resources": [{"name": "a", "kind": "K"}, {"name": "a", "kind": "K"}]}`},
		{"unknown dependency", `{"resources": [{"name": "a", "kind": "K", "dependsOn": ["ghost"]}]}`},
		{"self dependency", `{"resources": [{"name": "a", "kind": "K", "dependsOn": ["a"]}]}`},
		{"duplicate dependency entry", `{"resources": [
			{"name": "a", "kind": "K"},
			{"name": "b", "kind": "K", "dependsOn": ["a", "a"]}]}`},
		{"cycle", `{"resources": [
			{"name": "a", "kind": "K", "dependsOn": ["b"]},
			{"name": "b", "kind": "K", "dependsOn": ["a"]}]}`},
		{"unknown field", `{"resources": [{"name": "a", "kind": "K", "bogus": 1}]}`},
		{"malformed", `{"resources": [`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(tc.doc))
			if err == nil {
				t.Fatalf("expected rejection, got nil error")
			}
			if !errors.Is(err, ErrInvalid) {
				t.Fatalf("error %v does not wrap ErrInvalid", err)
			}
			t.Logf("rejected as expected: %v", err)
		})
	}
}
