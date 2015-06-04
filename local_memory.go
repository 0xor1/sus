package sus

import(
	`golang.org/x/net/context`
)

func NewLocalMemoryStore(m Marshaler, un Unmarshaler, idf IdFactory, vf VersionFactory) VersionStore {
	store := map[string][]byte{}
	get := func(ctx context.Context, id string) ([]byte, error) {
		var err error
		d, exists := store[id]
		if !exists {
			err = EntityDoesNotExist
		}
		return d, err
	}
	put := func(ctx context.Context, id string, d []byte) error {
		store[id] = d
		return nil
	}
	del := func(ctx context.Context, id string) error {
		delete(store, id)
		return nil
	}
	return NewMutexByteStore(get, put, del, m, un, idf, vf)
}