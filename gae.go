package sus

import(
	`github.com/qedus/nds`
	`golang.org/x/net/context`
	`google.golang.org/appengine/datastore`
)

func NewGaeStore(kind string, idf IdFactory, vf VersionFactory) VersionStore {
	getKey := func(ctx context.Context, id string) *datastore.Key {
		return datastore.NewKey(ctx, kind, id, 0, nil)
	}

	get := func(ctx context.Context, id string) (v Version, err error) {
		v = vf()
		key := getKey(ctx, id)
		err = nds.Get(ctx, key, v)
		return
	}

	put := func(ctx context.Context, id string, v Version) (err error) {
		key := getKey(ctx, id)
		_, err = nds.Put(ctx, key, v)
		return
	}

	del := func(ctx context.Context, id string) error {
		key := getKey(ctx, id)
		return nds.Delete(ctx, key)
	}

	rit := func(ctx context.Context, tran Transaction) error {
		return nds.RunInTransaction(ctx, tran, &datastore.TransactionOptions{XG:true})
	}

	return NewVersionStore(get, put, del, idf, vf, rit)
}