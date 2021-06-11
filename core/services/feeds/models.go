package feeds

import (
	"database/sql/driver"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/smartcontractkit/chainlink/core/utils/crypto"
)

// We only support OCR and FM for the feeds manager
const (
	JobTypeFluxMonitor       = "fluxmonitor"
	JobTypeOffchainReporting = "offchainreporting"
)

type FeedsManager struct {
	ID        int32
	Name      string
	URI       string
	PublicKey crypto.PublicKey
	JobTypes  pq.StringArray `gorm:"type:text[]"`
	Network   string
	CreatedAt time.Time
	UpdatedAt time.Time
}

func (FeedsManager) TableName() string {
	return "feeds_managers"
}

// JobProposalStatus are the status codes that define the stage of a proposal
type JobProposalStatus uint

const (
	JobProposalStatusUnknown JobProposalStatus = iota
	JobProposalStatusPending
	JobProposalStatusApproved
	JobProposalStatusRejected
)

var _JobProposalStatusValues = [...]string{"unknown", "pending", "approved", "rejected"}

func (s JobProposalStatus) String() string {
	return _JobProposalStatusValues[s]
}

// JobProposalStatusString retrieves an enum value from the enum constants
// string name. Throws an error if the param is not part of the enum.
func JobProposalStatusString(s string) (JobProposalStatus, error) {
	for i, val := range _JobProposalStatusValues {
		if s == val {
			return JobProposalStatus(i), nil
		}
	}

	return 0, fmt.Errorf("%s does not belong to JobProposalStatus values", s)
}

func (s JobProposalStatus) Value() (driver.Value, error) {
	return s.String(), nil
}

func (s *JobProposalStatus) Scan(value interface{}) error {
	if value == nil {
		return nil
	}

	str, ok := value.(string)
	if !ok {
		bytes, ok := value.([]byte)
		if !ok {
			return fmt.Errorf("value is not a byte slice")
		}

		str = string(bytes[:])
	}

	val, err := JobProposalStatusString(str)
	if err != nil {
		return err
	}

	*s = val
	return nil
}

type JobProposal struct {
	ID             uint
	Spec           string
	Status         JobProposalStatus
	JobID          uuid.UUID
	FeedsManagerID int32
	CreatedAt      time.Time
	UpdatedAt      time.Time
}
