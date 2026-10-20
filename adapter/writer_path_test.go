package adapter

import (
	"context"
	"testing"
)

func TestWriter_DynamicRemotePath(t *testing.T) {
	w := &baiduRemoteWriter{path: "/old"}
	w.SetPathProvider(func() (string, bool) { return "/new", true })
	if got, linked := w.remotePathLocked(); got != "/new" || !linked {
		t.Fatalf("remotePathLocked=(%q,%v), want /new,true", got, linked)
	}
}

func TestWriter_UnlinkedDiscardsData(t *testing.T) {
	w := &baiduRemoteWriter{
		session: &writeSession{},
		path:    "/old",
		pathFn:  func() (string, bool) { return "/old", false },
	}
	w.session.MarkDirty()
	if err := w.syncLocked(context.Background(), false); err != nil {
		t.Fatalf("syncLocked err=%v", err)
	}
	if w.session.Dirty() {
		t.Fatal("unlinked writer should discard dirty data without upload")
	}
}
