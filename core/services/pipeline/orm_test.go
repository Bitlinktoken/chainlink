package pipeline_test

import (
	"fmt"
	"testing"
	"time"

	uuid "github.com/satori/go.uuid"
	"github.com/smartcontractkit/chainlink/core/internal/cltest"
	"github.com/smartcontractkit/chainlink/core/services/pipeline"
	"github.com/stretchr/testify/require"
)

func Test_PipelineORM_FindRun(t *testing.T) {
	store, cleanup := cltest.NewStore(t)
	defer cleanup()
	db := store.DB

	orm := pipeline.NewORM(db, store.Config)

	require.NoError(t, db.Exec(`SET CONSTRAINTS pipeline_runs_pipeline_spec_id_fkey DEFERRED`).Error)
	expected := cltest.MustInsertPipelineRun(t, db)

	run, err := orm.FindRun(expected.ID)
	require.NoError(t, err)

	require.Equal(t, expected.ID, run.ID)
}

func Test_PipelineORM_StoreRun(t *testing.T) {
	store, cleanup := cltest.NewStore(t)
	defer cleanup()
	db := store.DB

	orm := pipeline.NewORM(db, store.Config)

	run := &pipeline.Run{
		CreatedAt: time.Now(),
		Errors:    nil,
		Outputs:   pipeline.JSONSerializable{Null: true},
	}

	// allow inserting without a spec
	require.NoError(t, db.Exec(`SET CONSTRAINTS pipeline_runs_pipeline_spec_id_fkey DEFERRED`).Error)

	err := orm.CreateRun(db, run)
	require.NoError(t, err)

	s := fmt.Sprintf(`
ds1 [type=bridge async=true name="example-bridge" timeout=0 requestData=<{"data": {"coin": "BTC", "market": "USD"}}>]
ds1_parse [type=jsonparse lax=false  path="data,result"]
ds1_multiply [type=multiply times=1000000000000000000]

ds1->ds1_parse->ds1_multiply->answer1;

answer1 [type=median index=0];
answer2 [type=bridge name=election_winner index=1];
`)
	p, err := pipeline.Parse(s)
	require.NoError(t, err)

	// spec := pipeline.Spec{DotDagSource: s}

	now := time.Now()

	err = orm.StoreRun(db, run, []pipeline.TaskRunResult{
		// pending task
		pipeline.TaskRunResult{
			ID:         uuid.NewV4(),
			Task:       p.ByDotID("ds1"),
			Result:     pipeline.Result{},
			CreatedAt:  now,
			FinishedAt: nil,
		},
		// finished task
		pipeline.TaskRunResult{
			ID:         uuid.NewV4(),
			Task:       p.ByDotID("answer2"),
			Result:     pipeline.Result{Value: 1},
			CreatedAt:  now,
			FinishedAt: &now,
		},
	}, true)
	require.NoError(t, err)

	r, err := orm.FindRun(run.ID)
	require.NoError(t, err)
	run = &r
	require.Equal(t, 2, len(run.PipelineTaskRuns))
}
