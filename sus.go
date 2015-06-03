package sus

import(
	`errors`
	`golang.org/x/net/context`
)

var(
	EntityDoesNotExist = errors.New(`entity does not exist`)
	NonsequentialUpdate = errors.New(`nonsequential update`)
)

type Versionable interface{
	getVersion() int
	incrementVersion()
}

func NewVersionable() Versionable {
	return &versionableImpl{}
}

type VersionableFactory func() Versionable

type IdFactory func() string

type VersionableStore interface{
	Create(ctx context.Context) (id string, v Versionable, err error)
	Read(ctx context.Context, id string) (v Versionable, err error)
	Update(ctx context.Context, id string, v Versionable) error
	Delete(ctx context.Context, id string) error
}

type versionableImpl struct {
	Version int	`datastore:",noindex" json:"version"`
}

func (vi *versionableImpl) getVersion() int{
	return vi.Version
}

func (vi *versionableImpl) incrementVersion() {
	vi.Version++
}

