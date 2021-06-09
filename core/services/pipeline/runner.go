package pipeline

import (
	"context"
	"fmt"
	"runtime/debug"
	"sort"
	"time"

	"github.com/smartcontractkit/chainlink/core/service"
	"github.com/smartcontractkit/chainlink/core/store/models"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/smartcontractkit/chainlink/core/logger"
	"github.com/smartcontractkit/chainlink/core/utils"
	"gorm.io/gorm"
)

//go:generate mockery --name Runner --output ./mocks/ --case=underscore

type Runner interface {
	service.Service

	// We expect spec.JobID and spec.JobName to be set for logging/prometheus.
	// ExecuteRun executes a new run in-memory according to a spec and returns the results.
	ExecuteRun(ctx context.Context, spec Spec, pipelineInput interface{}, meta JSONSerializable, l logger.Logger) (run Run, trrs TaskRunResults, err error)
	// InsertFinishedRun saves the run results in the database.
	InsertFinishedRun(db *gorm.DB, run Run, trrs TaskRunResults, saveSuccessfulTaskRuns bool) (int64, error)

	// ExecuteAndInsertNewRun executes a new run in-memory according to a spec, persists and saves the results.
	// It is a combination of ExecuteRun and InsertFinishedRun.
	// Note that the spec MUST have a DOT graph for this to work.
	ExecuteAndInsertFinishedRun(ctx context.Context, spec Spec, pipelineInput interface{}, meta JSONSerializable, l logger.Logger, saveSuccessfulTaskRuns bool) (runID int64, finalResult FinalResult, err error)
}

type runner struct {
	orm             ORM
	config          Config
	runReaperWorker utils.SleeperTask

	utils.StartStopOnce
	chStop chan struct{}
	chDone chan struct{}
}

var (
	promPipelineTaskExecutionTime = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pipeline_task_execution_time",
		Help: "How long each pipeline task took to execute",
	},
		[]string{"job_id", "job_name", "task_type"},
	)
)

func NewRunner(orm ORM, config Config) *runner {
	r := &runner{
		orm:    orm,
		config: config,
		chStop: make(chan struct{}),
		chDone: make(chan struct{}),
	}
	r.runReaperWorker = utils.NewSleeperTask(
		utils.SleeperTaskFuncWorker(r.runReaper),
	)
	return r
}

func (r *runner) Start() error {
	return r.StartOnce("PipelineRunner", func() error {
		go r.runReaperLoop()
		return nil
	})
}

func (r *runner) Close() error {
	return r.StopOnce("PipelineRunner", func() error {
		close(r.chStop)
		<-r.chDone
		return nil
	})
}

func (r *runner) destroy() {
	err := r.runReaperWorker.Stop()
	if err != nil {
		logger.Error(err)
	}
}

func (r *runner) runReaperLoop() {
	defer close(r.chDone)
	defer r.destroy()

	runReaperTicker := time.NewTicker(r.config.JobPipelineReaperInterval())
	defer runReaperTicker.Stop()
	for {
		select {
		case <-r.chStop:
			return
		case <-runReaperTicker.C:
			r.runReaperWorker.WakeUp()
		}
	}
}

type memoryTaskRun struct {
	task   Task
	inputs []input
	vars   Vars
}

// Returns the results sorted by index. It is not thread-safe.
func (m *memoryTaskRun) inputsSorted() (a []Result) {
	inputs := make([]input, len(m.inputs))
	copy(inputs, m.inputs)
	sort.Slice(inputs, func(i, j int) bool {
		return inputs[i].index < inputs[j].index
	})
	a = make([]Result, len(inputs))
	for i, input := range inputs {
		a[i] = input.result
	}

	return
}

type input struct {
	result Result
	index  int32
}

// When a task panics, we catch the panic and wrap it in an error for reporting to the scheduler.
type ErrRunPanicked struct {
	v interface{}
}

func (err ErrRunPanicked) Error() string {
	return fmt.Sprintf("goroutine panicked when executing run: %v", err.v)
}

func (r *runner) ExecuteRun(
	ctx context.Context,
	spec Spec,
	pipelineInput interface{},
	meta JSONSerializable,
	l logger.Logger,
) (Run, TaskRunResults, error) {
	l.Debugw("Initiating tasks for pipeline run of spec", "job ID", spec.JobID, "job name", spec.JobName)

	var (
		startRun = time.Now()
		run      = Run{
			PipelineSpec:   spec,
			PipelineSpecID: spec.ID,
			Inputs:         JSONSerializable{Val: pipelineInput, Null: false},
			CreatedAt:      startRun,
		}
	)

	scheduler, err := r.run(ctx, &run, pipelineInput, meta, l)
	if err != nil {
		return run, nil, err
	}

	var taskRunResults TaskRunResults
	for _, result := range scheduler.results {
		taskRunResults = append(taskRunResults, result)
	}

	finalResult := taskRunResults.FinalResult()
	if finalResult.HasErrors() {
		promPipelineRunErrors.WithLabelValues(fmt.Sprintf("%d", spec.JobID), spec.JobName).Inc()
	}
	run.Errors = finalResult.ErrorsDB()
	run.Outputs = finalResult.OutputsDB()

	now := time.Now()
	run.FinishedAt = &now
	runTime := run.FinishedAt.Sub(run.CreatedAt)
	l.Debugw("Finished all tasks for pipeline run", "specID", spec.ID, "runTime", runTime)
	promPipelineRunTotalTimeToCompletion.WithLabelValues(fmt.Sprintf("%d", spec.JobID), spec.JobName).Set(float64(runTime))

	return run, taskRunResults, nil
}

func (r *runner) run(
	ctx context.Context,
	run *Run,
	pipelineInput interface{},
	meta JSONSerializable,
	l logger.Logger,
) (*scheduler, error) {
	l.Debugw("Initiating tasks for pipeline run of spec", "job ID", run.PipelineSpec.JobID, "job name", run.PipelineSpec.JobName)

	pipeline, err := Parse(run.PipelineSpec.DotDagSource)
	if err != nil {
		return nil, err
	}

	// initialize certain task params
	for _, task := range pipeline.Tasks {
		if task.Type() == TaskTypeHTTP {
			task.(*HTTPTask).config = r.config
		} else if task.Type() == TaskTypeBridge {
			task.(*BridgeTask).config = r.config
			task.(*BridgeTask).tx = r.orm.DB()
		}
	}

	// avoid an extra db write if there is no async tasks present
	// or if this is called from ResumeRun
	if pipeline.HasAsync() && run.ID == 0 {
		if err := r.orm.CreateRun(r.orm.DB(), run); err != nil {
			return nil, err
		}

		run.Async = true
	}

	todo := context.TODO()
	scheduler := newScheduler(todo, pipeline, run, pipelineInput)
	go scheduler.Run()

	for taskRun := range scheduler.taskCh {
		// execute
		go func(taskRun *memoryTaskRun) {
			defer func() {
				if err := recover(); err != nil {
					logger.Default.Errorw("goroutine panicked executing run", "panic", err, "stacktrace", string(debug.Stack()))

					t := time.Now()
					scheduler.report(todo, TaskRunResult{
						Task:       taskRun.task,
						Result:     Result{Error: ErrRunPanicked{err}},
						FinishedAt: t,
						CreatedAt:  t, // TODO: more accurate start time
					})
				}
			}()
			result := r.executeTaskRun(ctx, run.PipelineSpec, taskRun, meta, l)

			logTaskRunToPrometheus(result, run.PipelineSpec)

			scheduler.report(todo, result)
		}(taskRun)
	}

	// if the run is suspended, awaiting resumption
	run.Pending = scheduler.pending

	return scheduler, err
}

func (r *runner) executeTaskRun(ctx context.Context, spec Spec, taskRun *memoryTaskRun, meta JSONSerializable, l logger.Logger) TaskRunResult {
	start := time.Now()
	loggerFields := []interface{}{
		"taskName", taskRun.task.DotID(),
	}

	// Order of precedence for task timeout:
	// - Specific task timeout (task.TaskTimeout)
	// - Job level task timeout (spec.MaxTaskDuration)
	// - Passed in context
	taskTimeout, isSet := taskRun.task.TaskTimeout()
	if isSet {
		var cancel context.CancelFunc
		ctx, cancel = utils.CombinedContext(r.chStop, taskTimeout)
		defer cancel()
	} else if spec.MaxTaskDuration != models.Interval(time.Duration(0)) {
		var cancel context.CancelFunc
		ctx, cancel = utils.CombinedContext(r.chStop, time.Duration(spec.MaxTaskDuration))
		defer cancel()
	}

	result := taskRun.task.Run(ctx, taskRun.vars, meta, taskRun.inputsSorted())
	loggerFields = append(loggerFields, "result value", result.Value)
	loggerFields = append(loggerFields, "result error", result.Error)
	switch v := result.Value.(type) {
	case []byte:
		loggerFields = append(loggerFields, "resultString", fmt.Sprintf("%q", v))
		loggerFields = append(loggerFields, "resultHex", fmt.Sprintf("%x", v))
	}
	l.Debugw("Pipeline task completed", loggerFields...)

	return TaskRunResult{
		Task:       taskRun.task,
		Result:     result,
		CreatedAt:  start,
		FinishedAt: time.Now(),
	}
}

func logTaskRunToPrometheus(trr TaskRunResult, spec Spec) {
	elapsed := trr.FinishedAt.Sub(trr.CreatedAt)

	promPipelineTaskExecutionTime.WithLabelValues(fmt.Sprintf("%d", spec.JobID), spec.JobName, string(trr.Task.Type())).Set(float64(elapsed))
	var status string
	if trr.Result.Error != nil {
		status = "error"
	} else {
		status = "completed"
	}
	promPipelineTasksTotalFinished.WithLabelValues(fmt.Sprintf("%d", spec.JobID), spec.JobName, string(trr.Task.Type()), status).Inc()
}

// ExecuteAndInsertFinishedRun executes a run in memory then inserts the finished run/task run records, returning the final result
func (r *runner) ExecuteAndInsertFinishedRun(ctx context.Context, spec Spec, pipelineInput interface{}, meta JSONSerializable, l logger.Logger, saveSuccessfulTaskRuns bool) (runID int64, finalResult FinalResult, err error) {
	run, trrs, err := r.ExecuteRun(ctx, spec, pipelineInput, meta, l)
	if err != nil {
		return 0, finalResult, errors.Wrapf(err, "error executing run for spec ID %v", spec.ID)
	}

	if run.Async {
		if run.Pending {
			// store the suspended run in the database, await resumption

			// TODO: handle instant continue
			if err = r.orm.StoreSuspendedRun(r.orm.DB(), run.ID, trrs); err != nil {
				return run.ID, finalResult, errors.Wrapf(err, "error inserting suspended run for spec ID %v", spec.ID)
			}
		} else {
			finalResult = trrs.FinalResult()
			// if async, we need to update an existing db row
			if err = r.orm.FinishRun(r.orm.DB(), &run, trrs, saveSuccessfulTaskRuns); err != nil {
				return run.ID, finalResult, errors.Wrapf(err, "error inserting finished results for spec ID %v", spec.ID)
			}
		}
		return run.ID, finalResult, nil
	} else {
		finalResult = trrs.FinalResult()
		// else we create a new row
		if runID, err = r.orm.InsertFinishedRun(r.orm.DB(), run, trrs, saveSuccessfulTaskRuns); err != nil {
			return runID, finalResult, errors.Wrapf(err, "error inserting finished results for spec ID %v", spec.ID)
		}
		return runID, finalResult, nil
	}

}

func (r *runner) Resume(ctx context.Context, runID int64, l logger.Logger) (run Run, incomplete bool, err error) {
	run, err = r.orm.FindRun(runID)

	if err != nil {
		return Run{}, false, err
	}
	// construct a scheduler with certain results already set
	// recalculate unfinished dependencies for each task
	// make sure it doesn't emit from roots, but from tasks that have 0 deps now and aren't in result set
	// call run

	// TODO: consider how to handle resume where we got back one result but two bridges are pending

	// TODO: meta has to come from somewhere
	s, err := r.run(ctx, &run, run.Inputs.Val, JSONSerializable{}, l)

	return run, s.pending, err
}

func (r *runner) InsertFinishedRun(db *gorm.DB, run Run, trrs TaskRunResults, saveSuccessfulTaskRuns bool) (int64, error) {
	return r.orm.InsertFinishedRun(db, run, trrs, saveSuccessfulTaskRuns)
}

func (r *runner) runReaper() {
	err := r.orm.DeleteRunsOlderThan(r.config.JobPipelineReaperThreshold())
	if err != nil {
		logger.Errorw("Pipeline run reaper failed", "error", err)
	}
}
