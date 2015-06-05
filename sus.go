/*
Package sus provides data storage for entities that require sequential updates.
Any type of datastore can be created in the same manner as those available by default
in sus, Memory/File/AppEngine.
 */
package sus

import(
	`errors`
)

var(
	EntityDoesNotExist = errors.New(`entity does not exist`)
	NonsequentialUpdate = errors.New(`nonsequential update`)
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
	Create() (id string, v Version, err error)
	CreateMulti(count uint) (ids []string, vs []Version, err error)
	Read(id string) (v Version, err error)
	ReadMulti(ids []string) (vs []Version, err error)
	Update(id string, v Version) error
	UpdateMulti(ids []string, vs []Version) error
	Delete(id string) error
	DeleteMulti(ids []string) error
}

type IdFactory func() string
type VersionFactory func() Version
type RunInTransaction func(tran Transaction) error
type Transaction func() error
type GetMulti func(ids []string) ([]Version, error)
type PutMulti func(ids []string, vs []Version) error
type DeleteMulti func(ids []string) error

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
func (s *store) Create() (id string, v Version, err error) {
	ids, vs, err := s.CreateMulti(1)
	if len(ids) == 1 && len(vs) == 1 {
		id = ids[0]
		v = vs[0]
	}
	return
}

// Creates a set of new versioned entities.
func (s *store) CreateMulti(count uint) (ids []string, vs []Version, err error) {
	if count == 0 {
		return
	}
	ucount := int(count)
	err = s.runInTransaction(func() error {
		ids = make([]string, count, count)
		vs = make([]Version, count, count)
		for i := 0; i < ucount; i++ {
			ids[i] = s.idFactory()
			vs[i] = s.versionFactory()
		}
		return s.putMulti(ids, vs)
	})
	return
}

// Fetches the versioned entity with id.
func (s *store) Read(id string) (v Version, err error) {
	vs, err := s.ReadMulti([]string{id})
	if len(vs) == 1 {
		v = vs[0]
	}
	return
}

// Fetches the versioned entities with id's.
func (s *store) ReadMulti(ids []string) (vs []Version, err error) {
	if len(ids) == 0 {
		return
	}
	err = s.runInTransaction(func() error {
		vs, err = s.getMulti(ids)
		return err
	})
	return
}

// Updates the versioned entity with id.
func (s *store) Update(id string, v Version) (err error) {
	err = s.UpdateMulti([]string{id}, []Version{v})
	return
}

// Updates the versioned entities with id's.
func (s *store) UpdateMulti(ids []string, vs []Version) (err error) {
	count := len(ids)
	if count != len(vs) {
		err = LenIdsNotEqualToLenVs
		return
	}
	if count == 0 {
		return
	}
	err = s.runInTransaction(func() error {
		oldVs, err := s.getMulti(ids)
		if err == nil {
			for i := 0; i < count; i++ {
				if oldVs[i].getVersion() != vs[i].getVersion() {
					return NonsequentialUpdate
				}
			}
			for i := 0; i < count; i++ {
				vs[i].incrementVersion()
			}
			return s.putMulti(ids, vs)
		}
		return err
	})
	return
}

// Deletes the versioned entity with id.
func (s *store) Delete(id string) error {
	return s.DeleteMulti([]string{id})
}

// Deletes the versioned entities with id's.
func (s *store) DeleteMulti(ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	return s.runInTransaction(func() error {
		return s.deleteMulti(ids)
	})
}