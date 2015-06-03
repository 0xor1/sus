package sus

import(
	`os`
	`sync`
	`io/ioutil`
	`encoding/json`
	`golang.org/x/net/context`
)

func NewFileStore(storeDir string, idf IdFactory, vf VersionableFactory) VersionableStore {
	return &fileStore{
		storeDir: storeDir,
		idf: idf,
		vf: vf,
	}
}

type fileStore struct {
	storeDir		string
	vf				VersionableFactory
	idf     		IdFactory
	mtx     		sync.Mutex
}

func (fs *fileStore) getFileName(id string) string {
	return fs.storeDir + `/` + id + `.sus`
}

func (fs *fileStore) Create(ctx context.Context) (id string, v Versionable, err error) {
	fs.mtx.Lock()
	defer fs.mtx.Unlock()
	id = fs.idf()
	v = fs.vf()
	js, err := json.Marshal(v)
	if err == nil {
		err = ioutil.WriteFile(fs.getFileName(id), js, nil)
	}
	return
}

func (fs *fileStore) Read(ctx context.Context, id string) (v Versionable, err error) {
	fs.mtx.Lock()
	defer fs.mtx.Unlock()
	js, err := ioutil.ReadFile(fs.getFileName(id))
	if err == nil {
		v = fs.vf()
		err = json.Unmarshal(js, v)
	}
	return
}

func (fs *fileStore) Update(ctx context.Context, id string, v Versionable) (err error) {
	fs.mtx.Lock()
	defer fs.mtx.Unlock()
	oldJs, err := ioutil.ReadFile(fs.getFileName(id))
	if err == nil {
		oldV := fs.vf()
		err = json.Unmarshal(oldJs, oldV)
		if oldV.getVersion() != v.getVersion() {
			err = NonsequentialUpdate
		}
		if err == nil {
			v.incrementVersion()
			var js []byte
			js, err = json.Marshal(v)
			if err == nil {
				err = ioutil.WriteFile(fs.getFileName(id), js, nil)
			}
		}
	}
	return
}

func (fs *fileStore) Delete(ctx context.Context, id string) (err error) {
	fs.mtx.Lock()
	defer fs.mtx.Unlock()
	os.Remove(fs.getFileName(id))
	return
}