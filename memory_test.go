package sus

import(
	`fmt`
	`testing`
	`golang.org/x/net/context`
	`github.com/stretchr/testify/assert`
)

func Test_MemoryStore_Create(t *testing.T){
	fms := newFooMemoryStore()

	id1, f1, err1 := fms.Create(nil)

	assert.NotEqual(t, ``, id1, `id1 should be a non empty string`)
	assert.NotNil(t, f1, `f1 should not be nil`)
	assert.Equal(t, 0, f1.getVersion(), `f1's version should be 0`)
	assert.Nil(t, err1, `err1 should be nil`)

	id2, f2, err2 := fms.Create(nil)

	assert.NotEqual(t, ``, id2, `id2 should be a non empty string`)
	assert.NotEqual(t, id1, id2, `id2 should not be id1`)
	assert.NotNil(t, f2, `f2 should not be nil`)
	assert.Equal(t, 0, f2.getVersion(), `f2's version should be 0`)
	assert.True(t, f2 != f1, `f2 should not be f1`)
	assert.Nil(t, err2, `err2 should be nil`)
}

func Test_MemoryStore_Read_success(t *testing.T){
	fms := newFooMemoryStore()

	id, f1, err1 := fms.Create(nil)

	assert.NotEqual(t, ``, id, `id should be a non empty string`)
	assert.NotNil(t, f1, `f1 should not be nil`)
	assert.Equal(t, 0, f1.getVersion(), `f1's version should be 0`)
	assert.Nil(t, err1, `err1 should be nil`)

	f2, err2 := fms.Read(nil, id)

	assert.NotNil(t, f2, `f2 should not be nil`)
	assert.Equal(t, f1, f2, `f2 should be f1`)
	assert.Nil(t, err2, `err2 should be nil`)
}

func Test_MemoryStore_Read_EntityDoesNotExist_failure(t *testing.T){
	fms := newFooMemoryStore()

	f, err := fms.Read(nil, ``)

	assert.Nil(t, f, `f should be nil`)
	assert.Equal(t, EntityDoesNotExist, err, `err should be EntityDoesNotExist`)
}

func Test_MemoryStore_Update_success(t *testing.T){
	fms := newFooMemoryStore()
	id, f, err := fms.Create(nil)

	err = fms.Update(nil, id, f)

	assert.Equal(t, 1, f.getVersion(), `f's version should be 1`)
	assert.Nil(t, err, `err should be nil`)
}

func Test_MemoryStore_Update_EntityDoesNotExist_failure(t *testing.T){
	fms := newFooMemoryStore()
	_, f, _ := fms.Create(nil)

	err := fms.Update(nil, ``, f)

	assert.Equal(t, EntityDoesNotExist, err, `err should be EntityDoesNotExist`)
}

func Test_MemoryStore_Update_NonsequentialUpdate_failure(t *testing.T){
	fms := newFooMemoryStore()
	id, f, _ := fms.Create(nil)
	f.incrementVersion()

	err := fms.Update(nil, id, f)

	assert.Equal(t, NonsequentialUpdate, err, `err should be NonsequentialUpdate`)
}

func Test_MemoryStore_Delete_success(t *testing.T){
	fms := newFooMemoryStore()
	id, f, err := fms.Create(nil)

	err = fms.Delete(nil, id)

	assert.Nil(t, err, `err should be nil`)

	f, err = fms.Read(nil, id)

	assert.Nil(t, f, `f should be nil`)
	assert.Equal(t, EntityDoesNotExist, err, `err should be EntityDoesNotExist`)
}

type foo struct{
	Version	`json:"version"`
}

func newFooMemoryStore() *fooMemoryStore {
	idSrc := 0
	return &fooMemoryStore{
		inner: NewJsonMemoryStore(
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

type fooMemoryStore struct {
	inner VersionStore
}

func (fms *fooMemoryStore) Create(ctx context.Context) (id string, f *foo, err error) {
	id, v, err := fms.inner.Create(ctx)
	if v != nil {
		f = v.(*foo)
	}
	return
}

func (fms *fooMemoryStore) Read(ctx context.Context, id string) (f *foo, err error) {
	v, err := fms.inner.Read(ctx, id)
	if v != nil {
		f = v.(*foo)
	}
	return
}

func (fms *fooMemoryStore) Update(ctx context.Context, id string, f *foo) (err error) {
	return fms.inner.Update(ctx, id, f)
}

func (fms *fooMemoryStore) Delete(ctx context.Context, id string) (err error) {
	return fms.inner.Delete(ctx, id)
}