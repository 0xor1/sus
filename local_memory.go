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
	lms.store[id] = v
	lms.mtx.Unlock()
	return
}

func (lms *localMemoryStore) Read(ctx context.Context, id string) (v Versionable, err error) {
	lms.mtx.Lock()
	v, exists := lms.store[id]
	lms.mtx.Unlock()
	if !exists {
		err = EntityDoesNotExist
	}
	return
}

func (lms *localMemoryStore) Update(ctx context.Context, id string, v Versionable) (err error) {
	lms.mtx.Lock()
	oldV, exists := lms.store[id]
	if !exists {
		err = EntityDoesNotExist
	}
	if oldV.getVersion() != v.getVersion() {
		err = NonsequentialUpdate
	}
	if err == nil {
		v.incrementVersion()
		lms.store[id] = v
	}
	lms.mtx.Unlock()
	return
}

func (lms *localMemoryStore) Delete(ctx context.Context, id string) (err error) {
	lms.mtx.Lock()
	delete(lms.store, id)
	lms.mtx.Unlock()
	return
}