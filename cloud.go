package sus

import(
	`github.com/qedus/nds`
	`golang.org/x/net/context`
	`google.golang.org/appengine/datastore`
)

func NewCloudStore(kind string, idf IdFactory, vf VersionableFactory) VersionableStore {
	return &cloudStore{
		kind: kind,
		idf: idf,
		vf: vf,
	}
}

type cloudStore struct {
	kind	string
	vf		VersionableFactory
	idf     IdFactory
}

func (cs *cloudStore) Create(ctx context.Context) (id string, v Versionable, err error) {
	id = cs.idf()
	v = cs.vf()
	key := datastore.NewKey(ctx, cs.kind, id, 0, nil)
	_, err = nds.Put(ctx, key, v)
	return
}

func (cs *cloudStore) Read(ctx context.Context, id string) (v Versionable, err error) {
	v = cs.vf()
	key := datastore.NewKey(ctx, cs.kind, id, 0, nil)
	err = nds.Get(ctx, key, v)
	return
}

func (cs *cloudStore) Update(ctx context.Context, id string, v Versionable) (err error) {
	err = nds.RunInTransaction(ctx, func(ctx context.Context) (err error) {
		oldV := cs.vf()
		key := datastore.NewKey(ctx, cs.kind, id, 0, nil)
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

func (cs *cloudStore) Delete(ctx context.Context, id string) (err error) {
	key := datastore.NewKey(ctx, cs.kind, id, 0, nil)
	err = nds.Delete(ctx, key)
	return
}