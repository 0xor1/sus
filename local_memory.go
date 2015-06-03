package sus

import(
	`sync`
	`golang.org/x/net/context`
)

func NewLocalMemoryStore(idf IdFactory, vf VersionableFactory) VersionableStore {
	return &localMemoryStore{
		store: map[string]Versionable{},
		idf: idf,
		vf: vf,
	}
}

type localMemoryStore struct {
	store   map[string]Versionable
	vf		VersionableFactory
	idf     IdFactory
	mtx     sync.Mutex
}

func (lms *localMemoryStore) Create(ctx context.Context) (id string, v Versionable, err error) {
	id = lms.idf()
	v = lms.vf()
	lms.mtx.Lock()
	defer lms.mtx.Unlock()
	lms.store[id] = v
	return
}

func (lms *localMemoryStore) Read(ctx context.Context, id string) (v Versionable, err error) {
	lms.mtx.Lock()
	defer lms.mtx.Unlock()
	v, exists := lms.store[id]
	if !exists {
		err = EntityDoesNotExist
	}
	return
}

func (lms *localMemoryStore) Update(ctx context.Context, id string, v Versionable) (err error) {
	lms.mtx.Lock()
	defer lms.mtx.Unlock()
	oldV, exists := lms.store[id]
	if !exists {
		err = EntityDoesNotExist
	} else {
		if oldV.getVersion() != v.getVersion() {
			err = NonsequentialUpdate
		}
		if err == nil {
			v.incrementVersion()
			lms.store[id] = v
		}
	}
	return
}

func (lms *localMemoryStore) Delete(ctx context.Context, id string) (err error) {
	lms.mtx.Lock()
	defer lms.mtx.Unlock()
	delete(lms.store, id)
	return
}