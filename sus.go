package sus

import(
	`errors`
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
	vi := versionImpl(0)
	return &vi
}

type VersionFactory func() Version

type IdFactory func() string

type VersionStore interface{
	Create(ctx context.Context) (id string, v Version, err error)
	Read(ctx context.Context, id string) (v Version, err error)
	Update(ctx context.Context, id string, v Version) error
	Delete(ctx context.Context, id string) error
}

type versionImpl int

func (vi *versionImpl) getVersion() int{
	return int(*vi)
}

func (vi *versionImpl) incrementVersion() {
	*vi = *vi + 1
}

