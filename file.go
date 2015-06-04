package sus

import(
	`os`
	`io/ioutil`
	`golang.org/x/net/context`
)

func NewFileStore(storeDir string, fileExt string, m Marshaler, un Unmarshaler, idf IdFactory, vf VersionFactory) (VersionStore, error) {
	err := os.MkdirAll(storeDir, os.ModeDir)
	if err == nil {
		getFileName := func(id string) string {
			return storeDir + `/` + id + `.` + fileExt
		}
		get := func(ctx context.Context, id string) ([]byte, error) {
			return ioutil.ReadFile(getFileName(id))
		}
		put := func(ctx context.Context, id string, d []byte) error {
			return ioutil.WriteFile(getFileName(id), d, os.ModeAppend)
		}
		del := func(ctx context.Context, id string) error {
			return os.Remove(getFileName(id))
		}
		return NewMutexByteStore(
			get,
			put,
			del,
			m,
			un,
			idf,
			vf,
		), nil
	}
	return nil, err
}