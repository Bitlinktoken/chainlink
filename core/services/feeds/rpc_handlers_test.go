package feeds_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/smartcontractkit/chainlink/core/services/feeds"
	pb "github.com/smartcontractkit/chainlink/core/services/feeds/proto"
	"github.com/stretchr/testify/require"
)

func Test_rpcHandlers_ProposeJob(t *testing.T) {
	svc := setupTestService(t)

	var (
		jobID          = uuid.New()
		spec           = `some spec`
		feedsManagerID = int32(1)
	)
	h := feeds.NewRPCHandlers(svc, feedsManagerID)

	svc.orm.
		On("CreateJobProposal", context.Background(), &feeds.JobProposal{
			Spec:           spec,
			Status:         feeds.JobProposalStatusPending,
			JobID:          jobID,
			FeedsManagerID: feedsManagerID,
		}).
		Return(uint(1), nil)

	_, err := h.ProposeJob(context.Background(), &pb.ProposeJobRequest{
		Id:   jobID.String(),
		Spec: spec,
	})
	require.NoError(t, err)
}
