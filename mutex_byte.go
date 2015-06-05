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

// Creates and configures a store that stores entities by converting them to and from []byte and ensures versioning correctness with mutex locks.
func NewMutexByteStore(bg ByteGetter, bp BytePutter, d Deleter, m Marshaler, un Unmarshaler, idf IdFactory, vf VersionFactory) Store {
	mtx := sync.Mutex{}

	getMulti := func(ctx context.Context, ids []string) ([]Version, error) {
		var err error
		var d []byte
		count := len(ids)
		vs := make([]Version, count, count)
		for i := 0; i < count; i++{
			d, err = bg(ctx, ids[i])
			if err != nil {
				break
			}
			vs[i] = vf()
			err = un(d, vs[i])
			if err != nil {
				break
			}
		}
		if err != nil {
			vs = nil
		}
		return vs, err
	}

	putMulti := func(ctx context.Context, ids []string, vs []Version) error {
		var err error
		var d []byte
		count := len(ids)
		for i := 0; i < count; i++{
			d, err = m(vs[i])
			if err != nil {
				break
			}
			err = bp(ctx, ids[i], d)
		}
		return err
	}

	delMulti := func(ctx context.Context, ids []string) (err error) {
		count := len(ids)
		for i := 0; i < count; i++ {
			err = d(ctx, ids[i])
			if err != nil {
				break
			}
		}
		return
	}

	rit := func(ctx context.Context, tran Transaction) error {
		mtx.Lock()
		defer mtx.Unlock()
		return tran(ctx)
	}

	return NewStore(getMulti, putMulti, delMulti, idf, vf, rit)
}