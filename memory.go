package sus

import(
	`golang.org/x/net/context`
)

// Creates and configures a store that stores entities by converting them to and from json []byte data and keeps them in the local system memory.
func NewJsonMemoryStore(idf IdFactory, vf VersionFactory) Store {
	return NewMemoryStore(jsonMarshaler, jsonUnmarshaler, idf, vf)
}

// Creates and configures a store that stores entities by converting them to and from []byte and keeps them in the local system memory.
func NewMemoryStore(m Marshaler, un Unmarshaler, idf IdFactory, vf VersionFactory) Store {
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