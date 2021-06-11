package feeds_test

import (
	"context"
	"testing"

	"github.com/smartcontractkit/chainlink/core/services/feeds"
	"github.com/smartcontractkit/chainlink/core/services/feeds/mocks"
	"github.com/smartcontractkit/chainlink/core/services/keystore/keys/csakey"
	ksmocks "github.com/smartcontractkit/chainlink/core/services/keystore/mocks"
	"github.com/smartcontractkit/chainlink/core/utils/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestService struct {
	feeds.Service
	orm *mocks.ORM
	ks  *ksmocks.CSAKeystoreInterface
}

func Test_Service_RegisterManager(t *testing.T) {
	t.Parallel()

	var (
		id     = int32(1)
		ms     = feeds.FeedsManager{}
		pubkey = crypto.PublicKey("pubkey")
		key    = csakey.Key{
			PublicKey: pubkey,
		}
	)
	svc := setupTestService(t)

	svc.orm.On("CountManagers").Return(int64(0), nil)
	svc.orm.On("CreateManager", context.Background(), &ms).
		Return(id, nil)
	svc.ks.On("ListCSAKeys").Return([]csakey.Key{key}, nil)
	svc.ks.On("Unsafe_GetUnlockedPrivateKey", pubkey).Return([]byte("privkey"), nil)
	svc.orm.On("ListManagers", context.Background()).Return([]feeds.FeedsManager{ms}, nil)

	actual, err := svc.RegisterManager(&ms)
	require.NoError(t, err)

	assert.Equal(t, actual, id)
}

func Test_Service_ListManagers(t *testing.T) {
	t.Parallel()

	var (
		ms  = feeds.FeedsManager{}
		mss = []feeds.FeedsManager{ms}
	)
	svc := setupTestService(t)

	svc.orm.On("ListManagers", context.Background()).
		Return(mss, nil)

	actual, err := svc.ListManagers()
	require.NoError(t, err)

	assert.Equal(t, actual, mss)
}

func Test_Service_GetManagers(t *testing.T) {
	t.Parallel()

	var (
		id = int32(1)
		ms = feeds.FeedsManager{ID: id}
	)
	svc := setupTestService(t)

	svc.orm.On("GetManager", context.Background(), id).
		Return(&ms, nil)

	actual, err := svc.GetManager(id)
	require.NoError(t, err)

	assert.Equal(t, actual, &ms)
}

func Test_Service_CreateJobProposal(t *testing.T) {
	t.Parallel()

	var (
		id = uint(1)
		jp = feeds.JobProposal{}
	)
	svc := setupTestService(t)

	svc.orm.On("CreateJobProposal", context.Background(), &jp).
		Return(id, nil)

	actual, err := svc.CreateJobProposal(&jp)
	require.NoError(t, err)

	assert.Equal(t, actual, id)
}

func setupTestService(t *testing.T) *TestService {
	orm := &mocks.ORM{}
	ks := &ksmocks.CSAKeystoreInterface{}

	t.Cleanup(func() {
		orm.AssertExpectations(t)
	})

	return &TestService{
		Service: feeds.NewService(orm, ks),
		orm:     orm,
		ks:      ks,
	}
}
