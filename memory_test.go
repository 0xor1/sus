package sus

import(
	`fmt`
	`testing`
	`github.com/stretchr/testify/assert`
)

func Test_MemoryStore_Create(t *testing.T){
	fms := newFooMemoryStore()

	id1, f1, err1 := fms.Create()

	assert.NotEqual(t, ``, id1, `id1 should be a non empty string`)
	assert.NotNil(t, f1, `f1 should not be nil`)
	assert.Equal(t, 0, f1.getVersion(), `f1's version should be 0`)
	assert.Nil(t, err1, `err1 should be nil`)

	id2, f2, err2 := fms.Create()

	assert.NotEqual(t, ``, id2, `id2 should be a non empty string`)
	assert.NotEqual(t, id1, id2, `id2 should not be id1`)
	assert.NotNil(t, f2, `f2 should not be nil`)
	assert.Equal(t, 0, f2.getVersion(), `f2's version should be 0`)
	assert.True(t, f2 != f1, `f2 should not be f1`)
	assert.Nil(t, err2, `err2 should be nil`)
}

func Test_MemoryStore_CreateMulti_with_zero_count(t *testing.T){
	fms := newFooMemoryStore()

	ids, fs, err := fms.CreateMulti(0)

	assert.Nil(t, ids, `ids should be nil`)
	assert.Nil(t, fs, `fs should be nil`)
	assert.Nil(t, err, `err should be nil`)
}

func Test_MemoryStore_Read_success(t *testing.T){
	fms := newFooMemoryStore()

	id, f1, err1 := fms.Create()

	assert.NotEqual(t, ``, id, `id should be a non empty string`)
	assert.NotNil(t, f1, `f1 should not be nil`)
	assert.Equal(t, 0, f1.getVersion(), `f1's version should be 0`)
	assert.Nil(t, err1, `err1 should be nil`)

	f2, err2 := fms.Read(id)

	assert.NotNil(t, f2, `f2 should not be nil`)
	assert.Equal(t, f1, f2, `f2 should be f1`)
	assert.Nil(t, err2, `err2 should be nil`)
}

func Test_MemoryStore_ReadMulti_with_zero_count(t *testing.T){
	fms := newFooMemoryStore()

	f, err := fms.ReadMulti([]string{})

	assert.Nil(t, f, `f should be nil`)
	assert.Nil(t, err, `err1 should be nil`)
}

func Test_MemoryStore_Read_EntityDoesNotExist_failure(t *testing.T){
	fms := newFooMemoryStore()

	f, err := fms.Read(``)

	assert.Nil(t, f, `f should be nil`)
	assert.Equal(t, EntityDoesNotExist, err, `err should be EntityDoesNotExist`)
}

func Test_MemoryStore_Update_success(t *testing.T){
	fms := newFooMemoryStore()
	id, f, err := fms.Create()

	err = fms.Update(id, f)

	assert.Equal(t, 1, f.getVersion(), `f's version should be 1`)
	assert.Nil(t, err, `err should be nil`)
}

func Test_MemoryStore_Update_EntityDoesNotExist_failure(t *testing.T){
	fms := newFooMemoryStore()
	_, f, _ := fms.Create()

	err := fms.Update(``, f)

	assert.Equal(t, EntityDoesNotExist, err, `err should be EntityDoesNotExist`)
}

func Test_MemoryStore_Update_NonsequentialUpdate_failure(t *testing.T){
	fms := newFooMemoryStore()
	id, f, _ := fms.Create()
	f.incrementVersion()

	err := fms.Update(id, f)

	assert.Equal(t, NonsequentialUpdate, err, `err should be NonsequentialUpdate`)
}

func Test_MemoryStore_UpdateMulti_LenIdsNotEqualToLenVs_failure(t *testing.T){
	fms := newFooMemoryStore()

	err := fms.UpdateMulti([]string{``}, []*foo{})

	assert.Equal(t, LenIdsNotEqualToLenVs, err, `err should be LenIdsNotEqualToLenVs`)
}

func Test_MemoryStore_UpdateMulti_with_zero_count(t *testing.T){
	fms := newFooMemoryStore()

	err := fms.UpdateMulti([]string{}, []*foo{})

	assert.Nil(t, err, `err should be nil`)
}

func Test_MemoryStore_Delete_success(t *testing.T){
	fms := newFooMemoryStore()
	id, f, err := fms.Create()

	err = fms.Delete(id)

	assert.Nil(t, err, `err should be nil`)

	f, err = fms.Read(id)

	assert.Nil(t, f, `f should be nil`)
	assert.Equal(t, EntityDoesNotExist, err, `err should be EntityDoesNotExist`)
}

func Test_MemoryStore_DeleteMulti_with_zero_ids(t *testing.T){
	fms := newFooMemoryStore()
	ids := []string{}

	err := fms.DeleteMulti(ids)

	assert.Nil(t, err, `err should be nil`)
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

func (fms *fooMemoryStore) Create() (id string, f *foo, err error) {
	id, v, err := fms.inner.Create(nil)
	if v != nil {
		f = v.(*foo)
	}
	return
}

func (fms *fooMemoryStore) CreateMulti(count uint) (ids []string, fs []*foo, err error) {
	ids, vs, err := fms.inner.CreateMulti(nil, count)
	if vs != nil {
		count := len(vs)
		fs = make([]*foo, count, count)
		for i := 0; i < count; i++ {
			fs[i] = vs[i].(*foo)
		}
	}
	return
}

func (fms *fooMemoryStore) Read(id string) (f *foo, err error) {
	v, err := fms.inner.Read(nil, id)
	if v != nil {
		f = v.(*foo)
	}
	return
}

func (fms *fooMemoryStore) ReadMulti(ids []string) (fs []*foo, err error) {
	vs, err := fms.inner.ReadMulti(nil, ids)
	if vs != nil {
		count := len(vs)
		fs = make([]*foo, count, count)
		for i := 0; i < count; i++ {
			fs[i] = vs[i].(*foo)
		}
	}
	return
}

func (fms *fooMemoryStore) Update(id string, f *foo) (err error) {
	return fms.inner.Update(nil, id, f)

}

func (fms *fooMemoryStore) UpdateMulti(ids []string, fs []*foo) (err error) {
	if fs != nil {
		count := len(fs)
		vs := make([]Version, count, count)
		for i := 0; i < count; i++ {
			vs[i] = Version(fs[i])
		}
		err = fms.inner.UpdateMulti(nil, ids, vs)
	}
	return
}

func (fms *fooMemoryStore) Delete(id string) (err error) {
	return fms.inner.Delete(nil, id)
}

func (fms *fooMemoryStore) DeleteMulti(ids []string) (err error) {
	return fms.inner.DeleteMulti(nil, ids)
}