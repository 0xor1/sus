package sus

import(
	`os`
	`sync`
	`io/ioutil`
	`golang.org/x/net/context`
)

type Marshaler func(src interface{}) ([]byte, error)
type Unmarshaler func(data []byte, dst interface{}) error

func NewFileStore(storeDir string, fileExtension string, m Marshaler, um Unmarshaler, idf IdFactory, vf VersionFactory) VersionStore {
	return &fileStore{
		sd: storeDir,
		fe: fileExtension,
		m: m,
		um: um,
		idf: idf,
		vf: vf,
	}
}

type fileStore struct {
	sd	string
	fe	string
	m	Marshaler
	um	Unmarshaler
	idf IdFactory
	vf VersionFactory
	mtx sync.Mutex
}

func (fs *fileStore) getFileName(id string) string {
	return fs.sd + `/` + id + `.` + fs.fe
}

func (fs *fileStore) Create(ctx context.Context) (id string, v Version, err error) {
	fs.mtx.Lock()
	defer fs.mtx.Unlock()
	id = fs.idf()
	v = fs.vf()
	d, err := fs.m(v)
	if err == nil {
		err = ioutil.WriteFile(fs.getFileName(id), d, 0644)
	}
	return
}

func (fs *fileStore) Read(ctx context.Context, id string) (v Version, err error) {
	fs.mtx.Lock()
	defer fs.mtx.Unlock()
	d, err := ioutil.ReadFile(fs.getFileName(id))
	if err == nil {
		v = fs.vf()
		err = fs.um(d, v)
	}
	return
}

func (fs *fileStore) Update(ctx context.Context, id string, v Version) (err error) {
	fs.mtx.Lock()
	defer fs.mtx.Unlock()
	oldD, err := ioutil.ReadFile(fs.getFileName(id))
	if err == nil {
		oldV := fs.vf()
		err = fs.um(oldD, oldV)
		if oldV.getVersion() != v.getVersion() {
			err = NonsequentialUpdate
		}
		if err == nil {
			v.incrementVersion()
			var d []byte
			d, err = fs.m(v)
			if err == nil {
				err = ioutil.WriteFile(fs.getFileName(id), d, 0644)
			}
		}
	}
	return
}

func (fs *fileStore) Delete(ctx context.Context, id string) (err error) {
	fs.mtx.Lock()
	defer fs.mtx.Unlock()
	err = os.Remove(fs.getFileName(id))
	return
}