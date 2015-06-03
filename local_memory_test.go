package sus

import(
	`fmt`
	`testing`
	`golang.org/x/net/context`
	`github.com/stretchr/testify/assert`
)

func Test_Create(t *testing.T){
	flms := newFooLocalMemoryStore()

	id1, f1, err1 := flms.Create(nil)

	assert.NotEqual(t, ``, id1, `id1 should be a non empty string`)
	assert.NotNil(t, f1, `f1 should not be nil`)
	assert.Equal(t, 0, f1.getVersion(), `f1's version should be 0`)
	assert.Nil(t, err1, `err1 should be nil`)

	id2, f2, err2 := flms.Create(nil)

	assert.NotEqual(t, ``, id2, `id2 should be a non empty string`)
	assert.NotEqual(t, id1, id2, `id2 should not be id1`)
	assert.NotNil(t, f2, `f2 should not be nil`)
	assert.Equal(t, 0, f2.getVersion(), `f2's version should be 0`)
	assert.True(t, f2 != f1, `f2 should not be f1`)
	assert.Nil(t, err2, `err2 should be nil`)
}

func Test_Read_success(t *testing.T){
	flms := newFooLocalMemoryStore()

	id, f1, err1 := flms.Create(nil)

	assert.NotEqual(t, ``, id, `id should be a non empty string`)
	assert.NotNil(t, f1, `f1 should not be nil`)
	assert.Equal(t, 0, f1.getVersion(), `f1's version should be 0`)
	assert.Nil(t, err1, `err1 should be nil`)

	f2, err2 := flms.Read(nil, id)

	assert.NotNil(t, f2, `f2 should not be nil`)
	assert.Equal(t, f1, f2, `f2 should be f1`)
	assert.Nil(t, err2, `err2 should be nil`)
}

func Test_Read_EntityDoesNotExist_failure(t *testing.T){
	flms := newFooLocalMemoryStore()

	f, err := flms.Read(nil, ``)

	assert.Nil(t, f, `f should not be nil`)
	assert.Equal(t, EntityDoesNotExist, err, `err should be EntityDoesNotExist`)
}

func Test_Update_success(t *testing.T){
	flms := newFooLocalMemoryStore()
	id, f, err := flms.Create(nil)

	err = flms.Update(nil, id, f)

	assert.Equal(t, 1, f.getVersion(), `f's version should be 1`)
	assert.Nil(t, err, `err should be nil`)
}

func Test_Update_EntityDoesNotExist_failure(t *testing.T){
	flms := newFooLocalMemoryStore()
	_, f, _ := flms.Create(nil)

	err := flms.Update(nil, ``, f)

	assert.Equal(t, EntityDoesNotExist, err, `err should be EntityDoesNotExist`)
}

func Test_Delete_success(t *testing.T){
	flms := newFooLocalMemoryStore()
	id, f, err := flms.Create(nil)

	err = flms.Delete(nil, id)

	assert.Nil(t, err, `err should be nil`)

	f, err = flms.Read(nil, id)

	assert.Nil(t, f, `f should be nil`)
	assert.Equal(t, EntityDoesNotExist, err, `err should be EntityDoesNotExist`)
}

func Test_Update_NonsequentialUpdate_failure(t *testing.T){
	flms := newFooLocalMemoryStore()
	id, f1, _ := flms.Create(nil)
	_, f2, _ := flms.Create(nil)
	f1.incrementVersion()

	err := flms.Update(nil, id, f2)

	assert.Equal(t, NonsequentialUpdate, err, `err should be NonsequentialUpdate`)
}

type foo struct{
	Versionable
}

func newFooLocalMemoryStore() *fooLocalMemoryStore {
	idSrc := 0
	return &fooLocalMemoryStore{
		inner: NewLocalMemoryStore(
			func() string {
				idSrc++
				return fmt.Sprintf(`%d`, idSrc)
			},
			func() Versionable {
				return &foo{NewVersionable()}
			},
		),
	}
}

type fooLocalMemoryStore struct {
	inner VersionableStore
}

func (flms *fooLocalMemoryStore) Create(ctx context.Context) (id string, f *foo, err error) {
	id, v, err := flms.inner.Create(ctx)
	if v != nil {
		f = v.(*foo)
	}
	return
}

func (flms *fooLocalMemoryStore) Read(ctx context.Context, id string) (f *foo, err error) {
	v, err := flms.inner.Read(ctx, id)
	if v != nil {
		f = v.(*foo)
	}
	return
}

func (flms *fooLocalMemoryStore) Update(ctx context.Context, id string, f *foo) (err error) {
	return flms.inner.Update(ctx, id, f)
}

func (flms *fooLocalMemoryStore) Delete(ctx context.Context, id string) (err error) {
	return flms.inner.Delete(ctx, id)
}