//go:build !nobadger

package meta

import (
	"bytes"
	"fmt"
	"testing"
)

func TestBadgerLoadManyHardlinks(t *testing.T) {
	dir := t.TempDir()
	client, err := newKVMeta("badger", dir, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	m := client.(*kvMeta)
	t.Cleanup(func() { _ = m.Shutdown() })
	// Every inode requires both an attribute and a parent-count update. This
	// exceeds the real backend limit without relying on a production backup.
	count := int(m.client.(*badgerClient).client.MaxBatchCount()/2) + 1
	var dump bytes.Buffer
	fmt.Fprintf(&dump, `{"Setting":{"Name":"hardlinks","MetaVersion":1},"Counters":{"usedInodes":%d},"FSTree":{"attr":{"inode":1,"type":"directory","mode":493},"entries":{`, count)
	for i := 0; i < count; i++ {
		if i != 0 {
			dump.WriteByte(',')
		}
		fmt.Fprintf(&dump, `"a%d":{"attr":{"inode":%d,"type":"regular","mode":420}},"b%d":{"attr":{"inode":%d,"type":"regular","mode":420}}`, i, i+2, i, i+2)
	}
	dump.WriteString(`}}}`)
	if err := m.LoadMeta(&dump); err != nil {
		t.Fatalf("load %d hardlinked inodes: %v", count, err)
	}
	if err := m.Shutdown(); err != nil {
		t.Fatal(err)
	}
	reopened, err := newKVMeta("badger", dir+"?readonly=1", testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Shutdown()
	for _, inode := range []Ino{2, Ino(count/2 + 2), Ino(count + 1)} {
		var attr Attr
		if eno := reopened.GetAttr(Background(), inode, &attr); eno != 0 {
			t.Fatalf("read inode %d: %v", inode, eno)
		}
		if attr.Nlink != 2 || attr.Parent != 0 {
			t.Fatalf("inode %d: nlink=%d parent=%d", inode, attr.Nlink, attr.Parent)
		}
		parents := reopened.GetParents(Background(), inode)
		if len(parents) != 1 || parents[1] != 2 {
			t.Fatalf("inode %d: parents=%v", inode, parents)
		}
	}
}
