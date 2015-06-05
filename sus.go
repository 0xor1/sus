/*
Package sus provides data storage for entities that require sequential updates.
Any type of datastore can be created in the same manner as those available by default
in sus, Memory/File/AppEngine.
 */
package sus

import(
	`errors`
	`encoding/json`
	`golang.org/x/net/context`
)

var(
	// Returned by Read calls when the entity does not exist.
	EntityDoesNotExist = errors.New(`entity does not exist`)
	// Returned by Update calls when the entity has not been updated sequentially.
	NonsequentialUpdate = errors.New(`nonsequential update`)
	// Returned by UpdateMulti when the number of id's is not equal to the number of entities provided.
	LenIdsNotEqualToLenVs = errors.New(`len(ids) not equal to len(vs)`)
)

// The interface that struct entities must include as anonymous fields in order to be used with sus stores.
type Version interface{
	getVersion() int
	incrementVersion()
}

// The constructor to initialise the anonymous Version fields in struct entities.
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

// The core sus interface.
type Store interface{
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

// Create and configure a core store.
func NewStore(gm GetMulti, pm PutMulti, dm DeleteMulti, idf IdFactory, vf VersionFactory, rit RunInTransaction) Store {
	return &store{gm, pm, dm, idf, vf, rit}
}

type store struct{
	getMulti			GetMulti
	putMulti			PutMulti
	deleteMulti			DeleteMulti
	idFactory 			IdFactory
	versionFactory 		VersionFactory
	runInTransaction	RunInTransaction
}

// Creates a new versioned entity.
func (s *store) Create(ctx context.Context) (id string, v Version, err error) {
	ids, vs, err := s.CreateMulti(ctx, 1)
	if len(ids) == 1 && len(vs) == 1 {
		id = ids[0]
		v = vs[0]
	}
	return
}

// Creates a set of new versioned entities.
func (s *store) CreateMulti(ctx context.Context, count uint) (ids []string, vs []Version, err error) {
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

// Fetches the versioned entity with id.
func (s *store) Read(ctx context.Context, id string) (v Version, err error) {
	vs, err := s.ReadMulti(ctx, []string{id})
	if len(vs) == 1 {
		v = vs[0]
	}
	return
}

// Fetches the versioned entities with id's.
func (s *store) ReadMulti(ctx context.Context, ids []string) (vs []Version, err error) {
	if len(ids) == 0 {
		return
	}
	err = s.runInTransaction(ctx, func(ctx context.Context) error {
		vs, err = s.getMulti(ctx, ids)
		return err
	})
	return
}

// Updates the versioned entity with id.
func (s *store) Update(ctx context.Context, id string, v Version) (err error) {
	err = s.UpdateMulti(ctx, []string{id}, []Version{v})
	return
}

// Updates the versioned entities with id's.
func (s *store) UpdateMulti(ctx context.Context, ids []string, vs []Version) (err error) {
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

// Deletes the versioned entity with id.
func (s *store) Delete(ctx context.Context, id string) error {
	return s.DeleteMulti(ctx, []string{id})
}

// Deletes the versioned entities with id's.
func (s *store) DeleteMulti(ctx context.Context, ids []string) error {
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