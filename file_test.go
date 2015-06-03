package sus

import(
	`fmt`
	`testing`
	`golang.org/x/net/context`
	`github.com/stretchr/testify/assert`
	"encoding/json"
)

func Test_FileStore_Create(t *testing.T){
	ffs := newFooFileStore(nil, nil)

	id1, f1, err1 := ffs.Create(nil)

	assert.NotEqual(t, ``, id1, `id1 should be a non empty string`)
	assert.NotNil(t, f1, `f1 should not be nil`)
	assert.Equal(t, 0, f1.getVersion(), `f1's version should be 0`)
	assert.Nil(t, err1, `err1 should be nil`)

	id2, f2, err2 := ffs.Create(nil)

	assert.NotEqual(t, ``, id2, `id2 should be a non empty string`)
	assert.NotEqual(t, id1, id2, `id2 should not be id1`)
	assert.NotNil(t, f2, `f2 should not be nil`)
	assert.Equal(t, 0, f2.getVersion(), `f2's version should be 0`)
	assert.True(t, f2 != f1, `f2 should not be f1`)
	assert.Nil(t, err2, `err2 should be nil`)
}

func Test_FileStore_Read_success(t *testing.T){
	ffs := newFooFileStore(nil, nil)

	id, f1, err1 := ffs.Create(nil)

	assert.NotEqual(t, ``, id, `id should be a non empty string`)
	assert.NotNil(t, f1, `f1 should not be nil`)
	assert.Equal(t, 0, f1.getVersion(), `f1's version should be 0`)
	assert.Nil(t, err1, `err1 should be nil`)

	f2, err2 := ffs.Read(nil, id)

	assert.NotNil(t, f2, `f2 should not be nil`)
	assert.Equal(t, f1, f2, `f2 should be f1`)
	assert.Nil(t, err2, `err2 should be nil`)
}

func Test_FileStore_Read_EntityDoesNotExist_failure(t *testing.T){
	ffs := newFooFileStore(nil, nil)

	f, err := ffs.Read(nil, ``)

	assert.Nil(t, f, `f should not be nil`)
	assert.Equal(t, EntityDoesNotExist, err, `err should be EntityDoesNotExist`)
}

func Test_FileStore_Update_success(t *testing.T){
	ffs := newFooFileStore(nil, nil)
	id, f, err := ffs.Create(nil)

	err = ffs.Update(nil, id, f)

	assert.Equal(t, 1, f.getVersion(), `f's version should be 1`)
	assert.Nil(t, err, `err should be nil`)
}

func Test_FileStore_Update_EntityDoesNotExist_failure(t *testing.T){
	ffs := newFooFileStore(nil, nil)
	_, f, _ := ffs.Create(nil)

	err := ffs.Update(nil, ``, f)

	assert.Equal(t, EntityDoesNotExist, err, `err should be EntityDoesNotExist`)
}

func Test_FileStore_Update_NonsequentialUpdate_failure(t *testing.T){
	ffs := newFooFileStore(nil, nil)
	id, f1, _ := ffs.Create(nil)
	_, f2, _ := ffs.Create(nil)
	f1.incrementVersion()

	err := ffs.Update(nil, id, f2)

	assert.Equal(t, NonsequentialUpdate, err, `err should be NonsequentialUpdate`)
}

func Test_FileStore_Delete_success(t *testing.T){
	ffs := newFooFileStore(nil, nil)
	id, f, err := ffs.Create(nil)

	err = ffs.Delete(nil, id)

	assert.Nil(t, err, `err should be nil`)

	f, err = ffs.Read(nil, id)

	assert.Nil(t, f, `f should be nil`)
	assert.Equal(t, EntityDoesNotExist, err, `err should be EntityDoesNotExist`)
}

func newFooFileStore(m Marshaler, um Unmarshaler) *fooFileStore {
	idSrc := 0
	if m == nil {
		m = json.Marshal
	}
	if um == nil {
		um = json.Unmarshal
	}
	return &fooFileStore{
		inner: NewFileStore(
			`./testData`,
			`json`,
			m,
			um,
			func() string {
				idSrc++
				return fmt.Sprintf(`%d`, idSrc)
			},
			func() Version {
				return &foo{NewVersion()}
			},
		),
	}
}

type fooFileStore struct {
	inner VersionStore
}

func (ffs *fooFileStore) Create(ctx context.Context) (id string, f *foo, err error) {
	id, v, err := ffs.inner.Create(ctx)
	if v != nil {
		f = v.(*foo)
	}
	return
}

func (ffs *fooFileStore) Read(ctx context.Context, id string) (f *foo, err error) {
	v, err := ffs.inner.Read(ctx, id)
	if v != nil {
		f = v.(*foo)
	}
	return
}

func (ffs *fooFileStore) Update(ctx context.Context, id string, f *foo) (err error) {
	return ffs.inner.Update(ctx, id, f)
}

func (ffs *fooFileStore) Delete(ctx context.Context, id string) (err error) {
	return ffs.inner.Delete(ctx, id)
}