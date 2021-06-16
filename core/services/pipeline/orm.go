package pipeline

import (
	"context"
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/smartcontractkit/chainlink/core/services/postgres"
	"github.com/smartcontractkit/chainlink/core/store/models"
	"gorm.io/gorm"
)

// CamelToSnakeASCII converts camel case strings to snake case. For performance
// reasons it only works with ASCII strings.
func CamelToSnakeASCII(s string) string {
	buf := []byte(s)
	out := make([]byte, 0, len(buf)+3)

	l := len(buf)
	for i := 0; i < l; i++ {
		if !(allowedBindRune(buf[i]) || buf[i] == '_') {
			panic(fmt.Sprint("not allowed name ", s))
		}

		b := rune(buf[i])

		if unicode.IsUpper(b) {
			if i > 0 && buf[i-1] != '_' && (unicode.IsLower(rune(buf[i-1])) || (i+1 < l && unicode.IsLower(rune(buf[i+1])))) {
				out = append(out, '_')
			}
			b = unicode.ToLower(b)
		}

		out = append(out, byte(b))
	}

	return string(out)
}

func allowedBindRune(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9')
}

var (
	ErrNoSuchBridge = errors.New("no such bridge exists")
)

//go:generate mockery --name ORM --output ./mocks/ --case=underscore

type ORM interface {
	CreateSpec(ctx context.Context, tx *gorm.DB, pipeline Pipeline, maxTaskTimeout models.Interval) (int32, error)
	CreateRun(db *gorm.DB, run *Run) (err error)
	StoreRun(db *gorm.DB, run *Run, trrs []TaskRunResult, saveSuccessfulTaskRuns bool) (err error)
	InsertFinishedRun(db *gorm.DB, run Run, trrs []TaskRunResult, saveSuccessfulTaskRuns bool) (runID int64, err error)
	DeleteRunsOlderThan(threshold time.Duration) error
	FindBridge(name models.TaskType) (models.BridgeType, error)
	FindRun(id int64) (Run, error)
	GetAllRuns() ([]Run, error)
	DB() *gorm.DB
}

type orm struct {
	db     *gorm.DB
	config Config
}

var _ ORM = (*orm)(nil)

var (
	promPipelineRunErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pipeline_run_errors",
		Help: "Number of errors for each pipeline spec",
	},
		[]string{"job_id", "job_name"},
	)
	promPipelineRunTotalTimeToCompletion = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pipeline_run_total_time_to_completion",
		Help: "How long each pipeline run took to finish (from the moment it was created)",
	},
		[]string{"job_id", "job_name"},
	)
	promPipelineTasksTotalFinished = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pipeline_tasks_total_finished",
		Help: "The total number of pipeline tasks which have finished",
	},
		[]string{"job_id", "job_name", "task_type", "status"},
	)
)

func NewORM(db *gorm.DB, config Config) *orm {
	return &orm{db, config}
}

// The tx argument must be an already started transaction.
func (o *orm) CreateSpec(ctx context.Context, tx *gorm.DB, pipeline Pipeline, maxTaskDuration models.Interval) (int32, error) {
	spec := Spec{
		DotDagSource:    pipeline.Source,
		MaxTaskDuration: maxTaskDuration,
	}
	err := tx.Create(&spec).Error
	if err != nil {
		return 0, err
	}
	return spec.ID, errors.WithStack(err)
}

// InsertRun
func (o *orm) CreateRun(db *gorm.DB, run *Run) (err error) {
	if run.CreatedAt.IsZero() {
		return errors.New("run.CreatedAt must be set")
	}
	if err = db.Create(run).Error; err != nil {
		return errors.Wrap(err, "error inserting finished pipeline_run")
	}
	return err
}

// StoreRun
func (o *orm) StoreRun(db *gorm.DB, run *Run, trrs []TaskRunResult, saveSuccessfulTaskRuns bool) (err error) {
	finished := run.FinishedAt != nil
	return postgres.GormTransactionWithoutContext(db, func(tx *gorm.DB) error {
		// wrapper to sqlx
		raw_db, err := tx.DB()
		if err != nil {
			return err
		}
		db := sqlx.NewDb(raw_db, "postgres")
		db.MapperFunc(CamelToSnakeASCII)

		if !finished {
			// Lock the current run. This prevents races with /v2/resume
			sql := `SELECT id FROM pipeline_runs WHERE id = $1 FOR UPDATE;`
			if _, err := db.Exec(sql, run.ID); err != nil {
				return err
			}

			// Reload task runs, we want to check for any changes while the run was ongoing
			rows, err := db.Queryx(`SELECT * FROM pipeline_task_runs WHERE pipeline_run_id = $1`, run.ID)
			if err != nil {
				return err
			}
			taskRuns := []TaskRun{}
			if err := sqlx.StructScan(rows, &taskRuns); err != nil {
				return err
			}

			// diff with current state, if updated, swap run.PipelineTaskRuns and early return with restart = true
			var updated bool
			for _, trr := range trrs {
				if !trr.IsPending() {
					continue
				}

				// find the equivalent in taskRuns
			}

			if updated {
				run.PipelineTaskRuns = taskRuns
				// TODO: return restart flag
				return nil
			}
		} else {
			// simply finish the run, no need to do any sort of locking
			if run.Outputs.Val == nil || len(run.Errors) == 0 {
				return errors.Errorf("run must have both Outputs and Errors, got Outputs: %#v, Errors: %#v", run.Outputs.Val, run.Errors)
			}

			// TODO: this won't work if CreateRun() hasn't executed before, needs to be an upsert?
			if _, err := db.NamedExec(`UPDATE pipeline_runs SET finished_at = :finished_at, errors= :errors, outputs = :outputs WHERE id = :id`, run); err != nil {
				return err
			}
		}

		if !saveSuccessfulTaskRuns && !run.HasErrors() && finished {
			return nil
		}

		sql := `
		INSERT INTO pipeline_task_runs (pipeline_run_id, run_id, type, index, output, error, dot_id, created_at, finished_at)
		VALUES (:pipeline_run_id, :run_id, :type, :index, :output, :error, :dot_id, :created_at, :finished_at)
		ON CONFLICT (run_id) DO UPDATE SET
		output = EXCLUDED.output, error = EXCLUDED.error, finished_at = EXCLUDED.finished_at
		RETURNING *;
		`
		values := []TaskRun{}
		for _, trr := range trrs {
			output := trr.Result.OutputDB()
			values = append(values, TaskRun{
				PipelineRunID: run.ID,
				RunID:         trr.ID,
				Type:          trr.Task.Type(),
				Index:         trr.Task.OutputIndex(),
				Output:        &output,
				Error:         trr.Result.ErrorDB(),
				DotID:         trr.Task.DotID(),
				CreatedAt:     trr.CreatedAt,
				FinishedAt:    trr.FinishedAt,
			})
		}

		rows, err := db.NamedQuery(sql, values)
		if err != nil {
			return err
		}
		taskRuns := []TaskRun{}
		if err := sqlx.StructScan(rows, &taskRuns); err != nil {
			return err
		}
		// replace with new task run data
		run.PipelineTaskRuns = taskRuns
		return nil
	})
}

// If saveSuccessfulTaskRuns = false, we only save errored runs.
// That way if the job is run frequently (such as OCR) we avoid saving a large number of successful task runs
// which do not provide much value.
func (o *orm) InsertFinishedRun(db *gorm.DB, run Run, trrs []TaskRunResult, saveSuccessfulTaskRuns bool) (runID int64, err error) {
	if run.CreatedAt.IsZero() {
		return 0, errors.New("run.CreatedAt must be set")
	}
	if run.FinishedAt.IsZero() {
		return 0, errors.New("run.FinishedAt must be set")
	}
	if run.Outputs.Val == nil || len(run.Errors) == 0 {
		return 0, errors.Errorf("run must have both Outputs and Errors, got Outputs: %#v, Errors: %#v", run.Outputs.Val, run.Errors)
	}
	if len(trrs) == 0 && (saveSuccessfulTaskRuns || run.HasErrors()) {
		return 0, errors.New("must provide task run results")
	}

	err = postgres.GormTransactionWithoutContext(db, func(tx *gorm.DB) error {
		if err = tx.Create(&run).Error; err != nil {
			return errors.Wrap(err, "error inserting finished pipeline_run")
		}

		if !saveSuccessfulTaskRuns && !run.HasErrors() {
			return nil
		}

		sql := `
		INSERT INTO pipeline_task_runs (pipeline_run_id, type, index, output, error, dot_id, created_at, finished_at)
		VALUES %s
		`
		valueStrings := []string{}
		valueArgs := []interface{}{}
		for _, trr := range trrs {
			valueStrings = append(valueStrings, "(?,?,?,?,?,?,?,?)")
			valueArgs = append(valueArgs, run.ID, trr.Task.Type(), trr.Task.OutputIndex(), trr.Result.OutputDB(), trr.Result.ErrorDB(), trr.Task.DotID(), trr.CreatedAt, trr.FinishedAt)
		}

		/* #nosec G201 */
		stmt := fmt.Sprintf(sql, strings.Join(valueStrings, ","))
		return tx.Exec(stmt, valueArgs...).Error
	})
	return run.ID, err
}

func (o *orm) DeleteRunsOlderThan(threshold time.Duration) error {
	err := o.db.Exec(`DELETE FROM pipeline_runs WHERE finished_at < ?`, time.Now().Add(-threshold)).Error
	if err != nil {
		return err
	}
	return nil
}

func (o *orm) FindBridge(name models.TaskType) (models.BridgeType, error) {
	return FindBridge(o.db, name)
}

func (o *orm) FindRun(id int64) (Run, error) {
	var run = Run{ID: id}
	err := o.db.
		Preload("PipelineSpec").
		Preload("PipelineTaskRuns", func(db *gorm.DB) *gorm.DB {
			return db.
				Order("created_at ASC, id ASC")
		}).First(&run).Error
	return run, err
}

func (o *orm) GetAllRuns() ([]Run, error) {
	var runs []Run
	err := o.db.
		Preload("PipelineSpec").
		Preload("PipelineTaskRuns", func(db *gorm.DB) *gorm.DB {
			return db.
				Order("created_at ASC, id ASC")
		}).Find(&runs).Error
	return runs, err
}

// FindBridge find a bridge using the given database
func FindBridge(db *gorm.DB, name models.TaskType) (models.BridgeType, error) {
	var bt models.BridgeType
	return bt, errors.Wrapf(db.First(&bt, "name = ?", name.String()).Error, "could not find bridge with name '%s'", name)
}

func (o *orm) DB() *gorm.DB {
	return o.db
}
