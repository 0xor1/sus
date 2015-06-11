package sus

import(
	`sync`
	`github.com/qedus/nds`
	`golang.org/x/net/context`
	`google.golang.org/appengine/datastore`
)

// Creates and configures a store that stores entities in Google AppEngines memcache and datastore.
// github.com/qedus/nds is used for strongly consistent automatic caching.
func NewGaeStore(kind string, idf IdFactory, vf VersionFactory) Store {
	var tranCtx context.Context
	var mtx sync.Mutex

	getKey := func(ctx context.Context, id string) *datastore.Key {
		return datastore.NewKey(ctx, kind, id, 0, nil)
	}

	getMulti := func(ids []string) (vs []Version, err error) {
		count := len(ids)
		vs = make([]Version, count, count)
		ks := make([]*datastore.Key, count, count)
		for i := 0; i < count; i++ {
			vs[i] = vf()
			ks[i] = getKey(tranCtx, ids[i])
		}
		err = nds.GetMulti(tranCtx, ks, vs)
		return
	}

	putMulti := func(ids []string, vs []Version) (err error) {
		count := len(ids)
		ks := make([]*datastore.Key, count, count)
		for i := 0; i < count; i++ {
			ks[i] = getKey(tranCtx, ids[i])
		}
		_, err = nds.PutMulti(tranCtx, ks, vs)
		return
	}

	delMulti := func(ids []string) error {
		count := len(ids)
		ks := make([]*datastore.Key, count, count)
		for i := 0; i < count; i++ {
			ks[i] = getKey(tranCtx, ids[i])
		}
		return nds.DeleteMulti(tranCtx, ks)
	}

	isNonExtantError := func(err error) bool {
		return err == datastore.ErrNoSuchEntity
	}

	rit := func(tran Transaction) error {
		return nds.RunInTransaction(context.Background(), func(ctx context.Context)error{
			mtx.Lock()
			defer mtx.Unlock()
			tranCtx = ctx
			err := tran()
			tranCtx = nil
			return err
		}, &datastore.TransactionOptions{XG:true})
	}

	return NewStore(getMulti, putMulti, delMulti, idf, vf, isNonExtantError,rit)
}