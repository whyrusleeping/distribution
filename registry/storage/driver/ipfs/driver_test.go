package ipfs

import (
	"bytes"
	"testing"

	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/testsuites"
	. "gopkg.in/check.v1"

	"github.com/docker/distribution/context"

	u "github.com/ipfs/go-ipfs/util"
)

// Hook up gocheck into the "go test" runner.
func BadTest(t *testing.T) { TestingT(t) }

func init() {
	addr := "localhost:5001" //os.Getenv("REGISTRY_STORAGE_IPFS_ADDR")
	root := "/ipns/local/"   // os.Getenv("REGISTRY_STORAGE_IPFS_ROOT")

	testsuites.RegisterSuite(func() (storagedriver.StorageDriver, error) {
		return New(addr, root), nil
	}, testsuites.NeverSkip)

	// BUG(stevvooe): IPC is broken so we're disabling for now. Will revisit later.
	// testsuites.RegisterIPCSuite(driverName, map[string]string{"rootdirectory": root}, testsuites.NeverSkip)
}

func TestBasic(t *testing.T) {
	d := New("localhost:5001", "/ipns/local")

	err := d.PutContent(context.Background(), "/a/b/c", []byte("hello world"))
	if err != nil {
		t.Fatal(err)
	}

	out, err := d.GetContent(context.Background(), "/a/b/c")
	if err != nil {
		t.Fatal(err)
	}

	if string(out) != "hello world" {
		t.Fatal("wrong data")
	}

	err = d.Delete(context.Background(), "/a/b/c")
	if err != nil {
		t.Fatal(err)
	}

	out, err = d.GetContent(context.Background(), "/a/b/c")
	if err == nil {
		t.Fatal("expected not found")
	}
	if _, ok := err.(storagedriver.PathNotFoundError); !ok {
		t.Fatal("expected path not found error but got: ", err)
	}

	// alright, that stuff works, lets turn it up a notch

	b := int64(10000000)
	buf := make([]byte, b)
	u.NewTimeSeededRand().Read(buf)
	r := bytes.NewReader(buf)

	coolpath := "/this/is/a/cool/path"
	nn, err := d.WriteStream(context.Background(), coolpath, 0, r)
	if err != nil {
		t.Fatal(err)
	}
	if nn != b {
		t.Fatal("didnt write enough bytes", nn, b)
	}

	dataout, err := d.GetContent(context.Background(), coolpath)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(dataout, buf) {
		t.Fatal("data wasnt right!")
	}

	// now lets *move* that data, yeah, difficult stuff here
	coolerpath := "/slightly/cooler/path"
	err = d.Move(context.Background(), coolpath, coolerpath)
	if err != nil {
		t.Fatal(err)
	}

	dataout, err = d.GetContent(context.Background(), coolerpath)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(dataout, buf) {
		t.Fatal("data wasnt right!")
	}
}
