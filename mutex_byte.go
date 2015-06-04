package sus

import(
	`sync`
	`golang.org/x/net/context`
)

type Marshaler func(src Version) ([]byte, error)
type Unmarshaler func(data []byte, dst Version) error
type ByteGetter func(ctx context.Context, id string) ([]byte, error)
type BytePutter func(ctx context.Context, id string, d []byte) error

func NewMutexByteStore(bg ByteGetter, bp BytePutter, d Delete, m Marshaler, un Unmarshaler, idf IdFactory, vf VersionFactory) VersionStore {
	mtx := sync.Mutex{}

	get := func(ctx context.Context, id string) (v Version, err error) {
		d, err := bg(ctx, id)
		if err == nil {
			v = vf()
			err = un(d, v)
		}
		return
	}

	put := func(ctx context.Context, id string, v Version) error {
		d, err := m(v)
		if err == nil {
			err = bp(ctx, id, d)
		}
		return err
	}

	del := func(ctx context.Context, id string) error {
		return d(ctx, id)
	}

	rit := func(ctx context.Context, tran Transaction) error {
		mtx.Lock()
		defer mtx.Unlock()
		return tran(ctx)
	}

	return NewVersionStore(get, put, del, idf, vf, rit)
}