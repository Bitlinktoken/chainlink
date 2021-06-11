package feeds

import (
	"context"

	"github.com/google/uuid"
	pb "github.com/smartcontractkit/chainlink/core/services/feeds/proto"
)

// RPCHandlers define handlers for RPC method calls from the Feeds Manager
type RPCHandlers struct {
	svc            Service
	feedsManagerID int32
}

func NewRPCHandlers(svc Service, feedsManagerID int32) *RPCHandlers {
	return &RPCHandlers{
		svc:            svc,
		feedsManagerID: feedsManagerID,
	}
}

// ProposeJob creates a new job proposal record for the feeds manager
func (h *RPCHandlers) ProposeJob(ctx context.Context, req *pb.ProposeJobRequest) (*pb.ProposeJobResponse, error) {
	jobID, err := uuid.Parse(req.Id)
	if err != nil {
		return nil, err
	}

	jp := &JobProposal{
		Spec:           req.Spec,
		Status:         JobProposalStatusPending,
		JobID:          jobID,
		FeedsManagerID: h.feedsManagerID,
	}

	_, err = h.svc.CreateJobProposal(jp)
	if err != nil {
		return nil, err
	}

	return &pb.ProposeJobResponse{}, nil
}
