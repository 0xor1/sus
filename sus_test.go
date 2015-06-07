package sus

import(
	`testing`
	`github.com/stretchr/testify/assert`
)

func Test_versionImpl(t *testing.T){
	v := NewVersion()
	assert.Equal(t, 0, v.GetVersion(), `version should init to 0`)
	v.incrementVersion()
	assert.Equal(t, 1, v.GetVersion(), `increment should increment by 1`)
	v.incrementVersion()
	assert.Equal(t, 2, v.GetVersion(), `increment should increment by 1`)
}