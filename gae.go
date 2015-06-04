package sus

import(
	`github.com/qedus/nds`
	`golang.org/x/net/context`
	`google.golang.org/appengine/datastore`
)

func NewGaeStore(kind string, idf IdFactory, vf VersionFactory) VersionStore {
	return &gaeStore{
		kind: kind,
		idf: idf,
		vf: vf,
	}
}

type gaeStore struct {
	kind	string
	vf 		VersionFactory
	idf     IdFactory
}

func (gs *gaeStore) Create(ctx context.Context) (id string, v Version, err error) {
	id = gs.idf()
	v = gs.vf()
	key := datastore.NewKey(ctx, gs.kind, id, 0, nil)
	_, err = nds.Put(ctx, key, v)
	return
}

func (gs *gaeStore) Read(ctx context.Context, id string) (v Version, err error) {
	v = gs.vf()
	key := datastore.NewKey(ctx, gs.kind, id, 0, nil)
	err = nds.Get(ctx, key, v)
	return
}

func (gs *gaeStore) Update(ctx context.Context, id string, v Version) (err error) {
	err = nds.RunInTransaction(ctx, func(ctx context.Context) (err error) {
		oldV := gs.vf()
		key := datastore.NewKey(ctx, gs.kind, id, 0, nil)
		err = nds.Get(ctx, key, oldV)
		if err == nil {
			if oldV.getVersion() == v.getVersion() {
				err = NonsequentialUpdate
			}
			if err == nil {
				v.incrementVersion()
				_, err = datastore.Put(ctx, key, v)
			}
		}
		return
	}, &datastore.TransactionOptions{XG:false})
	return
}

func (gs *gaeStore) Delete(ctx context.Context, id string) (err error) {
	key := datastore.NewKey(ctx, gs.kind, id, 0, nil)
	err = nds.Delete(ctx, key)
	return
}