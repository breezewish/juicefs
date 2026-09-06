package fs

import (
	"syscall"
	"testing"

	"github.com/juicedata/juicefs/pkg/meta"
)

func TestReadDirPageUsesStableNameCursor(t *testing.T) {
	jfs := createTestFS(t)
	ctx := meta.NewContext(1, 0, []uint32{0})
	if err := jfs.Mkdir(ctx, "/site", 0o755, 0); err != 0 {
		t.Fatalf("mkdir: %s", err)
	}
	for _, name := range []string{"z.css", "a.html", "m.js"} {
		file, err := jfs.Create(ctx, "/site/"+name, 0o644, 0)
		if err != 0 {
			t.Fatalf("create %s: %s", name, err)
		}
		_ = file.Close(ctx)
	}

	first, cursor, err := jfs.ReadDirPage(ctx, "/site", 2, "")
	if err != 0 {
		t.Fatalf("first page: %s", err)
	}
	if len(first) != 2 || first[0].Name() != "a.html" || first[1].Name() != "m.js" {
		t.Fatalf("unexpected first page: %+v", first)
	}
	if cursor != "m.js" {
		t.Fatalf("unexpected cursor %q", cursor)
	}

	last, cursor, err := jfs.ReadDirPage(ctx, "/site", 2, cursor)
	if err != 0 {
		t.Fatalf("last page: %s", err)
	}
	if len(last) != 1 || last[0].Name() != "z.css" {
		t.Fatalf("unexpected last page: %+v", last)
	}
	if cursor != "" {
		t.Fatalf("expected terminal cursor, got %q", cursor)
	}

	between, _, err := jfs.ReadDirPage(ctx, "/site", 1, "b")
	if err != 0 || len(between) != 1 || between[0].Name() != "m.js" {
		t.Fatalf("nonexistent name cursor skipped a real entry: entries=%v error=%v", between, err)
	}
	end, cursor, err := jfs.ReadDirPage(ctx, "/site", 1, "zzz")
	if err != 0 || len(end) != 0 || cursor != "" {
		t.Fatalf("cursor after last entry: entries=%v cursor=%q error=%v", end, cursor, err)
	}
}

func TestReadDirPageValidatesDirectoryAndLimit(t *testing.T) {
	jfs := createTestFS(t)
	ctx := meta.NewContext(1, 0, []uint32{0})
	file, err := jfs.Create(ctx, "/index.html", 0o644, 0)
	if err != 0 {
		t.Fatalf("create file: %s", err)
	}
	_ = file.Close(ctx)

	if _, _, err := jfs.ReadDirPage(ctx, "/index.html", 10, ""); err != syscall.ENOTDIR {
		t.Fatalf("expected ENOTDIR, got %s", err)
	}
	if _, _, err := jfs.ReadDirPage(ctx, "/", 0, ""); err != syscall.EINVAL {
		t.Fatalf("expected EINVAL for zero limit, got %s", err)
	}
	if _, _, err := jfs.ReadDirPage(ctx, "/", maxReadDirPageSize+1, ""); err != syscall.EINVAL {
		t.Fatalf("expected EINVAL for oversized limit, got %s", err)
	}
}
