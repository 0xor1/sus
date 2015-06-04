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

	getMulti := func(ctx context.Context, ids []string) (vs []Version, err error) {
		count := len(ids)
		vs = make([]Version, count, count)
		ks := make([]*datastore.Key, count, count)
		for i := 0; i < count; i++ {
			vs[i] = vf()
			ks[i] = getKey(ctx, ids[i])
		}
		err = nds.GetMulti(ctx, ks, vs)
		return
	}

	putMulti := func(ctx context.Context, ids []string, vs []Version) (err error) {
		count := len(ids)
		ks := make([]*datastore.Key, count, count)
		for i := 0; i < count; i++ {
			ks[i] = getKey(ctx, ids[i])
		}
		_, err = nds.PutMulti(ctx, ks, vs)
		return
	}

	delMulti := func(ctx context.Context, ids []string) error {
		count := len(ids)
		ks := make([]*datastore.Key, count, count)
		for i := 0; i < count; i++ {
			ks[i] = getKey(ctx, ids[i])
		}
		return nds.DeleteMulti(ctx, ks)
	}

	rit := func(ctx context.Context, tran Transaction) error {
		return nds.RunInTransaction(ctx, tran, &datastore.TransactionOptions{XG:true})
	}

	return NewVersionStore(getMulti, putMulti, delMulti, idf, vf, rit)
}