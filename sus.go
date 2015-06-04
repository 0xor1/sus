package sus

import(
	`errors`
	`encoding/json`
	`golang.org/x/net/context`
)

var(
	EntityDoesNotExist = errors.New(`entity does not exist`)
	NonsequentialUpdate = errors.New(`nonsequential update`)
)

type Version interface{
	getVersion() int
	incrementVersion()
}

func NewVersion() Version {
	vi := version(0)
	return &vi
}

type version int

func (vi *version) getVersion() int{
	return int(*vi)
}

func (vi *version) incrementVersion() {
	*vi = *vi + 1
}

type VersionStore interface{
	Create(ctx context.Context) (id string, v Version, err error)
	Read(ctx context.Context, id string) (v Version, err error)
	Update(ctx context.Context, id string, v Version) error
	Delete(ctx context.Context, id string) error
}

type IdFactory func() string
type VersionFactory func() Version
type RunInTransaction func(ctx context.Context, tran Transaction) error
type Transaction func(ctx context.Context) error
type Get func(ctx context.Context, id string) (Version, error)
type Put func(ctx context.Context, id string, v Version) error
type Delete func(ctx context.Context, id string) error

func NewVersionStore(g Get, p Put, d Delete, idf IdFactory, vf VersionFactory, rit RunInTransaction) VersionStore {
	return &versionStore{g, p, d, idf, vf, rit}
}

type versionStore struct{
	get 				Get
	put 				Put
	delete 				Delete
	idFactory 			IdFactory
	versionFactory 		VersionFactory
	runInTransaction	RunInTransaction
}

func (vs *versionStore) Create(ctx context.Context) (id string, v Version, err error) {
	err = vs.runInTransaction(ctx, func(ctx context.Context) error {
		id = vs.idFactory()
		v = vs.versionFactory()
		return vs.put(ctx, id, v)
	})
	return
}

func (vs *versionStore) Read(ctx context.Context, id string) (v Version, err error) {
	err = vs.runInTransaction(ctx, func(ctx context.Context) error {
		v, err = vs.get(ctx, id)
		return err
	})
	return
}

func (vs *versionStore) Update(ctx context.Context, id string, v Version) (err error) {
	err = vs.runInTransaction(ctx, func(ctx context.Context) error {
		oldV, err := vs.get(ctx, id)
		if err == nil {
			if oldV.getVersion() != v.getVersion() {
				err = NonsequentialUpdate
			} else {
				v.incrementVersion()
				err = vs.put(ctx, id, v)
			}
		}
		return err
	})
	return
}

func (vs *versionStore) Delete(ctx context.Context, id string) (err error) {
	err = vs.runInTransaction(ctx, func(ctx context.Context) error {
		return vs.delete(ctx, id)
	})
	return
}

func jsonMarshaler(v Version)([]byte, error){
	return json.Marshal(v)
}

func jsonUnmarshaler(d []byte, v Version) error{
	return json.Unmarshal(d, v)
}