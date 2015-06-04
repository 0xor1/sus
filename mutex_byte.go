package sus

import(
	`sync`
	`golang.org/x/net/context`
)

type Marshaler func(src Version) ([]byte, error)
type Unmarshaler func(data []byte, dst Version) error
type ByteGetter func(ctx context.Context, id string) ([]byte, error)
type BytePutter func(ctx context.Context, id string, d []byte) error
type Deleter func(ctx context.Context, id string) error

func NewMutexByteStore(bg ByteGetter, bp BytePutter, d Deleter, m Marshaler, un Unmarshaler, idf IdFactory, vf VersionFactory) VersionStore {
	return &mutexByteStore{bg, bp, d, m, un, idf, vf, sync.Mutex{}}
}

type mutexByteStore struct {
	get				ByteGetter
	put				BytePutter
	delete			Deleter
	marshal			Marshaler
	unmarshal 		Unmarshaler
	idFactory		IdFactory
	versionFactory 	VersionFactory
	mtx     		sync.Mutex
}

func (mbs *mutexByteStore) Create(ctx context.Context) (id string, v Version, err error) {
	mbs.mtx.Lock()
	defer mbs.mtx.Unlock()
	id = mbs.idFactory()
	v = mbs.versionFactory()
	d, err := mbs.marshal(v)
	if err == nil {
		err = mbs.put(ctx, id, d)
	}
	return
}

func (mbs *mutexByteStore) Read(ctx context.Context, id string) (v Version, err error) {
	mbs.mtx.Lock()
	defer mbs.mtx.Unlock()
	d, err := mbs.get(ctx, id)
	if err == nil {
		v = mbs.versionFactory()
		err = mbs.unmarshal(d, v)
	}
	return
}

func (mbs *mutexByteStore) Update(ctx context.Context, id string, v Version) (err error) {
	mbs.mtx.Lock()
	defer mbs.mtx.Unlock()
	oldD, err := mbs.get(ctx, id)
	if err == nil {
		oldV := mbs.versionFactory()
		err = mbs.unmarshal(oldD, oldV)
		if oldV.getVersion() != v.getVersion() {
			err = NonsequentialUpdate
		}
		if err == nil {
			v.incrementVersion()
			var d []byte
			d, err = mbs.marshal(v)
			if err == nil {
				err = mbs.put(ctx, id, d)
			}
		}
	}
	return
}

func (mbs *mutexByteStore) Delete(ctx context.Context, id string) (err error) {
	mbs.mtx.Lock()
	defer mbs.mtx.Unlock()
	err = mbs.delete(ctx, id)
	return
}
