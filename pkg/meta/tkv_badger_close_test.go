//go:build !nobadger
// +build !nobadger

package meta

import "testing"

func TestBadgerClientCloseIsIdempotent(t *testing.T) {
	client, err := newBadgerClient(t.TempDir())
	if err != nil {
		t.Fatalf("newBadgerClient: %v", err)
	}
	badgerClient, ok := client.(*badgerClient)
	if !ok {
		t.Fatalf("unexpected client type %T", client)
	}
	if err := badgerClient.close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := badgerClient.close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}
