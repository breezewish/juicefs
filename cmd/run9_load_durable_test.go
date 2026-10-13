//go:build !nobadger

package cmd

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/juicedata/juicefs/pkg/meta"
)

func TestBadgerLoadSurvivesProcessExit(t *testing.T) {
	const childDir = "JUICEFS_TEST_LOAD_BADGER_DIR"
	if dir := os.Getenv(childDir); dir != "" {
		if err := Main([]string{"juicefs", "load", "badger://" + dir, "../pkg/meta/metadata.sample"}); err != nil {
			t.Fatal(err)
		}
		return
	}
	dir := filepath.Join(t.TempDir(), "meta")
	child := exec.Command(os.Args[0], "-test.run=^TestBadgerLoadSurvivesProcessExit$", "-test.count=1")
	child.Env = append(os.Environ(), childDir+"="+dir)
	if output, err := child.CombinedOutput(); err != nil {
		t.Fatalf("load subprocess: %v\n%s", err, output)
	}
	m := meta.NewClient("badger://"+dir+"?readonly=1", nil)
	defer m.Shutdown()
	format, err := m.Load(true)
	if err != nil {
		t.Fatalf("load after process exit: %v", err)
	}
	if format.Name != "load-dump-test" {
		t.Fatalf("format name=%q", format.Name)
	}
	var attr meta.Attr
	if eno := m.GetAttr(meta.Background(), 4, &attr); eno != 0 || attr.Nlink != 2 {
		t.Fatalf("restored hardlink: errno=%v nlink=%d", eno, attr.Nlink)
	}
}
