package ipfs

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	_path "path"
	"runtime"
	"strings"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/docker/distribution/context"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/base"
	"github.com/docker/distribution/registry/storage/driver/factory"

	shell "github.com/whyrusleeping/ipfs-shell"
)

const driverName = "ipfs"
const defaultAddr = "localhost:5001"
const defaultRoot = "/ipns/local/docker-registry"

func debugTime() func() {
	before := time.Now()
	pc, _, _, ok := runtime.Caller(1)
	if !ok {
		panic("this is not okay")
	}

	f := runtime.FuncForPC(pc)
	fmt.Printf("starting %s\n", f.Name())
	return func() {
		fmt.Printf("%s took %s\n", f.Name(), time.Now().Sub(before))
	}
}

func init() {
	factory.Register(driverName, &ipfsDriverFactory{})
}

// ipfsDriverFactory implements the factory.StorageDriverFactory interface
type ipfsDriverFactory struct{}

func (factory *ipfsDriverFactory) Create(parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	return FromParameters(parameters), nil
}

type driver struct {
	root     string
	shell    *shell.Shell
	roothash string
	rootlock sync.Mutex

	publish chan<- string
}

func (d *driver) publishHash(hash string) {
	log.Error("PUBLISH: ", hash)
	d.publish <- hash
}

func (d *driver) runPublisher(ipnskey string) chan<- string {
	out := make(chan string, 32)
	go func() {
		var topub string
		var long <-chan time.Time
		var short <-chan time.Time
		for {
			select {
			case k := <-out:
				if topub == "" {
					long = time.After(time.Second * 5)
					short = time.After(time.Second * 1)
				} else {
					short = time.After(time.Second * 1)
				}
				topub = k
			case <-long:
				k := topub
				topub = ""
				long = nil
				short = nil

				err := d.publishChild(ipnskey, "docker-registry", k)
				if err != nil {
					log.Error("failed to publish: ", err)
				}
			case <-short:
				k := topub
				topub = ""
				long = nil
				short = nil

				err := d.publishChild(ipnskey, "docker-registry", k)
				if err != nil {
					log.Error("failed to publish: ", err)
				}
			}
		}
	}()
	return out
}

func (d *driver) publishChild(ipnskey, dirname, hash string) error {
	val, err := d.shell.Resolve(ipnskey)
	if err != nil {
		return err
	}

	newIpnsRoot, err := d.shell.PatchLink(val, dirname, hash, true)
	if err != nil {
		return err
	}

	err = d.shell.Publish(ipnskey, "/ipfs/"+newIpnsRoot)
	if err != nil {
		log.Error("failed to publish: ", err)
	}

	return nil
}

type baseEmbed struct {
	base.Base
}

// Driver is a storagedriver.StorageDriver implementation backed by a local
// IPFS daemon.
type Driver struct {
	baseEmbed
}

// FromParameters constructs a new Driver with a given parameters map
// Optional Parameters:
// - addr
// - root
func FromParameters(parameters map[string]interface{}) *Driver {
	var addr = defaultAddr
	var root = defaultRoot
	if parameters != nil {
		addrInterface, ok := parameters["addr"]
		if ok {
			addr = fmt.Sprint(addrInterface)
		}
		rootInterface, ok := parameters["root"]
		if ok {
			root = fmt.Sprint(rootInterface)
		}
	}
	return New(addr, root)
}

// New constructs a new Driver with a given addr (address) and root (IPNS root)
func New(addr string, root string) *Driver {
	defer debugTime()()
	shell := shell.NewShell(addr)
	info, err := shell.ID()
	if err != nil {
		log.Error("error constructing node: ", err)
		return nil
	}
	if strings.HasPrefix(root, "/ipns/local/") {
		root = strings.Replace(root, "local", info.ID, 1)
	}
	if !strings.HasPrefix(root, "/ipns/") {
		log.Error("tried to use non-ipns root")
		return nil
	}

	ipnsroot, err := shell.Resolve(info.ID)
	if err != nil {
		log.Error("failed to resolve ipns root: ", err)
		return nil
	}

	log.Error("ID: ", info.ID)
	log.Error("IPNSROOT: ", ipnsroot)
	hash, err := shell.ResolvePath(ipnsroot + "/docker-registry")
	if err != nil {
		if !strings.Contains(err.Error(), "no link named") {
			log.Error("failed to resolve docker-registry dir: ", err)
			return nil
		}

		h, err := shell.NewObject("unixfs-dir")
		if err != nil {
			log.Error("failed to get new empty dir: ", err)
			return nil
		}

		hash = h
	}

	d := &driver{
		shell:    shell,
		root:     root,
		roothash: hash,
	}
	d.publish = d.runPublisher(info.ID)

	return &Driver{
		baseEmbed: baseEmbed{
			Base: base.Base{
				StorageDriver: d,
			},
		},
	}
}

// Implement the storagedriver.StorageDriver interface

func (d *driver) Name() string {
	return driverName
}

// GetContent retrieves the content stored at "path" as a []byte.
func (d *driver) GetContent(ctx context.Context, path string) ([]byte, error) {
	defer debugTime()()
	reader, err := d.shell.Cat(d.fullPath(path))
	if err != nil {
		if strings.HasPrefix(err.Error(), "no link named") {
			return nil, storagedriver.PathNotFoundError{Path: path}
		}
		return nil, err
	}

	content, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	log.Debugf("Got content %s: %s", path, content)

	return content, nil
}

// PutContent stores the []byte content at a location designated by "path".
func (d *driver) PutContent(ctx context.Context, path string, contents []byte) error {
	defer debugTime()()
	contentHash, err := d.shell.Add(bytes.NewReader(contents))
	if err != nil {
		return err
	}

	// strip off leading slash
	path = path[1:]

	d.rootlock.Lock()
	defer d.rootlock.Unlock()
	nroot, err := d.shell.PatchLink(d.roothash, path, contentHash, true)
	if err != nil {
		return err
	}

	d.roothash = nroot
	d.publishHash(nroot)
	return nil
}

// ReadStream retrieves an io.ReadCloser for the content stored at "path" with a
// given byte offset.
func (d *driver) ReadStream(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	defer debugTime()()
	reader, err := d.shell.Cat(d.fullPath(path))
	if err != nil {
		return nil, err
	}

	_, err = io.CopyN(ioutil.Discard, reader, offset)
	if err != nil {
		return nil, err
	}

	return ioutil.NopCloser(reader), nil
}

// WriteStream stores the contents of the provided io.Reader at a location
// designated by the given path.
func (d *driver) WriteStream(ctx context.Context, path string, offset int64, reader io.Reader) (nn int64, err error) {
	defer debugTime()()
	fullPath := d.fullPath(path)

	if offset > 0 {
		oldReader, err := d.shell.Cat(fullPath)
		if err == nil {
			var buf bytes.Buffer

			nn, err = io.CopyN(&buf, oldReader, offset)
			if err != nil {
				return 0, err
			}

			_, err := io.Copy(&buf, reader)
			if err != nil {
				return 0, err
			}

			reader = &buf
		} else {
			if strings.HasPrefix(err.Error(), "no link named") {
				nn = 0
			} else {
				return 0, err
			}
		}
	}

	cr := &countReader{r: reader}
	contentHash, err := d.shell.Add(cr)
	if err != nil {
		return 0, err
	}

	log.Errorf("Wrote content (after %d) %s: %s", nn, path, contentHash)

	// strip off leading slash
	path = path[1:]

	d.rootlock.Lock()
	defer d.rootlock.Unlock()
	k, err := d.shell.PatchLink(d.roothash, path, contentHash, true)
	if err != nil {
		return 0, err
	}

	d.roothash = k
	d.publishHash(k)

	return nn + cr.n, nil
}

// Stat retrieves the FileInfo for the given path, including the current size
// in bytes and the creation time.
func (d *driver) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {
	defer debugTime()()
	d.rootlock.Lock()
	defer d.rootlock.Unlock()

	output, err := d.shell.FileList(d.fullPath(path))
	if err != nil {
		if strings.HasPrefix(err.Error(), "no link named") {
			return nil, storagedriver.PathNotFoundError{Path: path}
		}
		return nil, err
	}

	fi := storagedriver.FileInfoFields{
		Path:    path,
		IsDir:   output.Type == "Directory",
		ModTime: time.Time{},
	}

	if !fi.IsDir {
		fi.Size = int64(output.Size)
	}

	return storagedriver.FileInfoInternal{FileInfoFields: fi}, nil
}

// List returns a list of the objects that are direct descendants of the given
// path.
func (d *driver) List(ctx context.Context, path string) ([]string, error) {
	defer debugTime()()
	output, err := d.shell.FileList(d.fullPath(path))
	if err != nil {
		if strings.HasPrefix(err.Error(), "no link named") {
			return nil, storagedriver.PathNotFoundError{Path: path}
		}
		return nil, err
	}

	keys := make([]string, 0, len(output.Links))
	for _, link := range output.Links {
		keys = append(keys, _path.Join(path, link.Name))
	}

	return keys, nil
}

// Move moves an object stored at source to dest, removing the
// original object.
func (d *driver) Move(ctx context.Context, source string, dest string) error {
	defer debugTime()()
	sourceobj := d.fullPath(source)
	srchash, err := d.shell.ResolvePath(sourceobj)
	if err != nil {
		if strings.HasPrefix(err.Error(), "no link named") {
			return storagedriver.PathNotFoundError{Path: source}
		}
		return err
	}

	d.rootlock.Lock()
	defer d.rootlock.Unlock()
	newroot, err := d.shell.Patch(d.roothash, "rm-link", source[1:])
	if err != nil {
		if err.Error() == "merkledag: not found" {
			return storagedriver.PathNotFoundError{Path: source}
		} else {
			return err
		}
	}

	// remove leading slash
	dest = dest[1:]
	newroot, err = d.shell.PatchLink(newroot, dest, srchash, true)
	if err != nil {
		return err
	}

	d.roothash = newroot
	fmt.Println("HASH AFTER MOVE: ", newroot)
	d.publishHash(newroot)
	return nil
}

// Delete recursively deletes all objects stored at "path" and its subpaths.
func (d *driver) Delete(ctx context.Context, path string) error {
	defer debugTime()()
	d.rootlock.Lock()
	defer d.rootlock.Unlock()
	log.Error("roothash: ", d.roothash)
	newParentHash, err := d.shell.Patch(d.roothash, "rm-link", path[1:])
	if err != nil {
		log.Error("delete err: ", err)
		if err.Error() == "merkledag: not found" {
			fmt.Println("PATHNOTFOUND HAPPY HAPPY JOY JOY")
			return storagedriver.PathNotFoundError{Path: path}
		} else {
			fmt.Println("GOT A BAD ERROR: ", err)
			return err
		}
	}

	d.roothash = newParentHash
	d.publishHash(newParentHash)
	return nil
}

// URLFor returns a URL which may be used to retrieve the content
// stored at the given path.  It may return an UnsupportedMethodErr in
// certain StorageDriver implementations.
func (d *driver) URLFor(ctx context.Context, path string, options map[string]interface{}) (string, error) {
	return "", storagedriver.ErrUnsupportedMethod
}

// fullPath returns the absolute path of a key within the Driver's
// storage.
func (d *driver) fullPath(path string) string {
	return _path.Join("/ipfs", d.roothash, path)
}
