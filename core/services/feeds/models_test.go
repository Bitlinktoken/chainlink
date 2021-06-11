package feeds_test

import (
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/smartcontractkit/chainlink/core/services/feeds"
	"github.com/smartcontractkit/chainlink/core/utils/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_JobProposalStatus(t *testing.T) {
	testCases := []struct {
		name      string
		status    feeds.JobProposalStatus
		statusStr string
	}{
		{
			name:      "unknown",
			status:    feeds.JobProposalStatusUnknown,
			statusStr: "unknown",
		},
		{
			name:      "pending",
			status:    feeds.JobProposalStatusPending,
			statusStr: "pending",
		},
		{
			name:      "approved",
			status:    feeds.JobProposalStatusApproved,
			statusStr: "approved",
		},
		{
			name:      "rejected",
			status:    feeds.JobProposalStatusRejected,
			statusStr: "rejected",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Test String()
			assert.Equal(t, tc.statusStr, tc.status.String())

			// Test JobProposalStatusString()
			s, err := feeds.JobProposalStatusString(tc.statusStr)
			require.NoError(t, err)
			assert.Equal(t, tc.status, s)

			// Test Value()
			dv, err := tc.status.Value()
			require.NoError(t, err)
			assert.Equal(t, tc.statusStr, dv)

			// Test Scan()
			var st feeds.JobProposalStatus
			st.Scan(tc.statusStr)
			assert.Equal(t, tc.status, st)
		})
	}
}

func Test_PublicKey_String(t *testing.T) {
	t.Parallel()

	pubKey, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)

	pk := crypto.PublicKey(pubKey)
	expected := hex.EncodeToString(pubKey)

	assert.Equal(t, expected, pk.String())
}

func Test_PublicKey_MarshalJSON(t *testing.T) {
	t.Parallel()

	pubKey, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	hexKey := hex.EncodeToString(pubKey)

	pk := crypto.PublicKey(pubKey)
	actual, err := pk.MarshalJSON()
	require.NoError(t, err)

	assert.Equal(t, fmt.Sprintf(`"%s"`, hexKey), string(actual))
}

func Test_PublicKey_UnmarshalJSON(t *testing.T) {
	t.Parallel()

	pubKey, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	hexKey := hex.EncodeToString(pubKey)

	actual := &crypto.PublicKey{}
	err = actual.UnmarshalJSON([]byte(fmt.Sprintf(`"%s"`, hexKey)))
	require.NoError(t, err)

	assert.Equal(t, crypto.PublicKey(pubKey), *actual)
}

func Test_PublicKey_Scan(t *testing.T) {
	pubKey, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)

	actual := &crypto.PublicKey{}

	// Error if not bytes
	err = actual.Scan("not bytes")
	assert.Error(t, err)

	// Nil
	err = actual.Scan(nil)
	require.NoError(t, err)
	nilPk := crypto.PublicKey(nil)
	assert.Equal(t, &nilPk, actual)

	// Bytes
	err = actual.Scan([]byte(pubKey))
	require.NoError(t, err)
	assert.Equal(t, crypto.PublicKey(pubKey), *actual)
}

func Test_PublicKey_Value(t *testing.T) {
	pubKey, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)

	pk := crypto.PublicKey(pubKey)
	dv, err := pk.Value()
	require.NoError(t, err)
	assert.Equal(t, []byte(pubKey), dv)
}
