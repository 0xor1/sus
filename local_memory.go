package sus

import(
	`sync`
	`golang.org/x/net/context`
)

func NewLocalMemoryStore(m Marshaler, um Unmarshaler, idf IdFactory, vf VersionFactory) VersionStore {
	return &localMemoryStore{
		store: map[string][]byte{},
		m: m,
		um: um,
		idf: idf,
		vf: vf,
	}
}

type localMemoryStore struct {
	store   map[string][]byte
	m		Marshaler
	um		Unmarshaler
	vf 		VersionFactory
	idf     IdFactory
	mtx     sync.Mutex
}

func (lms *localMemoryStore) Create(ctx context.Context) (id string, v Version, err error) {
	id = lms.idf()
	v = lms.vf()
	d, err := lms.m(v)
	if err == nil {
		lms.store[id] = d
	}
	lms.mtx.Lock()
	defer lms.mtx.Unlock()
	return
}

func (lms *localMemoryStore) Read(ctx context.Context, id string) (v Version, err error) {
	lms.mtx.Lock()
	defer lms.mtx.Unlock()
	d, exists := lms.store[id]
	if !exists {
		err = EntityDoesNotExist
	} else {
		v = lms.vf()
		err = lms.um(d, v)
	}
	return
}

func (lms *localMemoryStore) Update(ctx context.Context, id string, v Version) (err error) {
	lms.mtx.Lock()
	defer lms.mtx.Unlock()
	oldD, exists := lms.store[id]
	if !exists {
		err = EntityDoesNotExist
	} else {
		oldV := lms.vf()
		err = lms.um(oldD, oldV)
		if oldV.getVersion() != v.getVersion() {
			err = NonsequentialUpdate
		}
		if err == nil {
			v.incrementVersion()
			var d []byte
			d, err = lms.m(v)
			if err == nil {
				lms.store[id] = d
			}
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