package packertest

import (
	"os"
	"testing"
)

func tempDir(t *testing.T) string {
	t.Helper()

	root, err := os.MkdirTemp("", "packer-*")
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("preserving test dir for inspection: %s", root)
			return
		}
		if err := os.RemoveAll(root); err != nil {
			t.Logf("cleanup of %s failed: %v", root, err)
		}
	})

	return root
}
