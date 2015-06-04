package sus

import(
	`errors`
	`encoding/json`
	`golang.org/x/net/context`
)

var(
	EntityDoesNotExist = errors.New(`entity does not exist`)
	NonsequentialUpdate = errors.New(`nonsequential update`)
	LenIdsNotEqualToLenVs = errors.New(`len(ids) not equal to len(vs)`)
)

type Version interface{
	getVersion() int
	incrementVersion()
}

func NewVersion() Version {
	vi := version(0)
	return &vi
}

type version int

func (vi *version) getVersion() int{
	return int(*vi)
}

func (vi *version) incrementVersion() {
	*vi += 1
}

type VersionStore interface{
	Create(ctx context.Context) (id string, v Version, err error)
	CreateMulti(ctx context.Context, count uint) (ids []string, vs []Version, err error)
	Read(ctx context.Context, id string) (v Version, err error)
	ReadMulti(ctx context.Context, ids []string) (vs []Version, err error)
	Update(ctx context.Context, id string, v Version) error
	UpdateMulti(ctx context.Context, ids []string, vs []Version) error
	Delete(ctx context.Context, id string) error
	DeleteMulti(ctx context.Context, ids []string) error
}

type IdFactory func() string
type VersionFactory func() Version
type RunInTransaction func(ctx context.Context, tran Transaction) error
type Transaction func(ctx context.Context) error
type GetMulti func(ctx context.Context, ids []string) ([]Version, error)
type PutMulti func(ctx context.Context, ids []string, vs []Version) error
type DeleteMulti func(ctx context.Context, ids []string) error

func NewVersionStore(gm GetMulti, pm PutMulti, dm DeleteMulti, idf IdFactory, vf VersionFactory, rit RunInTransaction) VersionStore {
	return &versionStore{gm, pm, dm, idf, vf, rit}
}

type versionStore struct{
	getMulti			GetMulti
	putMulti			PutMulti
	deleteMulti			DeleteMulti
	idFactory 			IdFactory
	versionFactory 		VersionFactory
	runInTransaction	RunInTransaction
}

func (s *versionStore) Create(ctx context.Context) (id string, v Version, err error) {
	ids, vs, err := s.CreateMulti(ctx, 1)
	if len(ids) == 1 && len(vs) == 1 {
		id = ids[0]
		v = vs[0]
	}
	return
}

func (s *versionStore) CreateMulti(ctx context.Context, count uint) (ids []string, vs []Version, err error) {
	if count == 0 {
		return
	}
	ucount := int(count)
	err = s.runInTransaction(ctx, func(ctx context.Context) error {
		ids = make([]string, count, count)
		vs = make([]Version, count, count)
		for i := 0; i < ucount; i++ {
			ids[i] = s.idFactory()
			vs[i] = s.versionFactory()
		}
		return s.putMulti(ctx, ids, vs)
	})
	return
}

func (s *versionStore) Read(ctx context.Context, id string) (v Version, err error) {
	vs, err := s.ReadMulti(ctx, []string{id})
	if len(vs) == 1 {
		v = vs[0]
	}
	return
}

func (s *versionStore) ReadMulti(ctx context.Context, ids []string) (vs []Version, err error) {
	if len(ids) == 0 {
		return
	}
	err = s.runInTransaction(ctx, func(ctx context.Context) error {
		vs, err = s.getMulti(ctx, ids)
		return err
	})
	return
}

func (s *versionStore) Update(ctx context.Context, id string, v Version) (err error) {
	err = s.UpdateMulti(ctx, []string{id}, []Version{v})
	return
}

func (s *versionStore) UpdateMulti(ctx context.Context, ids []string, vs []Version) (err error) {
	count := len(ids)
	if count != len(vs) {
		err = LenIdsNotEqualToLenVs
		return
	}
	if count == 0 {
		return
	}
	err = s.runInTransaction(ctx, func(ctx context.Context) error {
		oldVs, err := s.getMulti(ctx, ids)
		if err == nil {
			for i := 0; i < count; i++ {
				if oldVs[i].getVersion() != vs[i].getVersion() {
					return NonsequentialUpdate
				}
			}
			for i := 0; i < count; i++ {
				vs[i].incrementVersion()
			}
			return s.putMulti(ctx, ids, vs)
		}
		return err
	})
	return
}

func (s *versionStore) Delete(ctx context.Context, id string) error {
	return s.DeleteMulti(ctx, []string{id})
}

func (s *versionStore) DeleteMulti(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	return s.runInTransaction(ctx, func(ctx context.Context) error {
		return s.deleteMulti(ctx, ids)
	})
}

func jsonMarshaler(v Version)([]byte, error){
	return json.Marshal(v)
}

func jsonUnmarshaler(d []byte, v Version) error{
	return json.Unmarshal(d, v)
}