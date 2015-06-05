package sus

import(
	`errors`
	`testing`
	`github.com/stretchr/testify/assert`
	`golang.org/x/net/context`
)

func Test_MutexByteStore_Delete_with_error(t *testing.T){
	deleteError := errors.New(`delete error`)
	mbs := NewMutexByteStore(nil, nil, func(ctx context.Context, id string)error{return deleteError}, nil, nil, nil, nil)

	err := mbs.Delete(nil, ``)

	assert.Equal(t, deleteError, err, `err should be deleteError`)
}