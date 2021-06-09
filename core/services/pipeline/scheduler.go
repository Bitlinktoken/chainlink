package pipeline

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/smartcontractkit/chainlink/core/logger"
)

func (s *scheduler) newMemoryTaskRun(task Task) *memoryTaskRun {
	run := &memoryTaskRun{task: task, vars: s.vars.Copy()}

	// fill in the inputs
	if len(task.Inputs()) == 0 {
		// roots receive pipeline inputs
		run.inputs = append(run.inputs, input{index: 0, result: Result{Value: s.input}})
	} else {
		for _, i := range task.Inputs() {
			run.inputs = append(run.inputs, input{index: int32(i.OutputIndex()), result: s.results[i.ID()].Result})
		}
	}

	return run
}

type scheduler struct {
	ctx          context.Context
	pipeline     *Pipeline
	run          *Run
	dependencies map[int]uint
	input        interface{}
	waiting      uint
	results      map[int]TaskRunResult
	vars         Vars

	pending bool

	taskCh   chan *memoryTaskRun
	resultCh chan TaskRunResult
}

func newScheduler(ctx context.Context, p *Pipeline, run *Run, pipelineInput interface{}) *scheduler {
	dependencies := make(map[int]uint, len(p.Tasks))

	for id, task := range p.Tasks {
		len := len(task.Inputs())
		dependencies[id] = uint(len)
	}

	s := &scheduler{
		ctx:          ctx,
		pipeline:     p,
		run:          run,
		dependencies: dependencies,
		input:        pipelineInput,
		results:      make(map[int]TaskRunResult, len(p.Tasks)),
		vars:         NewVarsFrom(map[string]interface{}{"input": pipelineInput}),

		// taskCh should never block
		taskCh:   make(chan *memoryTaskRun, len(dependencies)),
		resultCh: make(chan TaskRunResult),
	}

	// if there's results already present on Run, then this is a resumption. Loop over them and fill results table
	for _, r := range run.PipelineTaskRuns {
		result := Result{}

		task := p.ByDotID(r.DotID)

		if task == nil {
			panic("can't find task by dot id")
		}

		// TODO: properly detect pending mark
		// if r.FinishedAt is not set, but CreatedAt is, then the task is pending
		if r.Error.String == "pending" {
			continue
		}

		if r.Error.Valid {
			result.Error = errors.New(r.Error.String)
		}

		if r.Output.Null == false {
			result.Value = r.Output.Val
		}

		s.results[task.ID()] = TaskRunResult{
			Task:   task,
			Result: result,
		}

		// store the result in vars
		if result.Error != nil {
			s.vars.Set(task.DotID(), result.Error)
		} else {
			s.vars.Set(task.DotID(), result.Value)
		}

		// mark all outputs as complete
		for _, output := range task.Outputs() {
			id := output.ID()
			s.dependencies[id]--
		}
	}

	// immediately schedule all doable tasks
	for id, task := range p.Tasks {
		// skip tasks that are not ready
		if s.dependencies[id] != 0 {
			continue
		}

		// skip finished tasks
		if _, ok := s.results[id]; ok {
			continue
		}

		run := s.newMemoryTaskRun(task)

		s.taskCh <- run
		s.waiting++
	}

	return s
}

func (s *scheduler) Run() {
Loop:
	for s.waiting > 0 {
		// we don't "for result in resultCh" because it would stall if the
		// pipeline is completely empty

		var result TaskRunResult
		select {
		case result = <-s.resultCh:
		case <-s.ctx.Done():
			now := time.Now()
			// mark remaining jobs as timeout
			for _, task := range s.pipeline.Tasks {
				if _, ok := s.results[task.ID()]; !ok {
					s.results[task.ID()] = TaskRunResult{
						Task:       task,
						Result:     Result{Error: ErrTimeout},
						CreatedAt:  now, // TODO: more accurate start time
						FinishedAt: now,
					}
				}
			}

			break Loop
		}

		s.waiting--

		// mark job as complete
		s.results[result.Task.ID()] = result

		// store the result in vars
		if result.Result.Error != nil {
			s.vars.Set(result.Task.DotID(), result.Result.Error)
		} else {
			s.vars.Set(result.Task.DotID(), result.Result.Value)
		}

		// TODO:
		// catch the pending state, we will keep the pipeline running until no more progress is made
		if result.Result.Error != nil && (result.Result.Error.Error() == "pending") {
			s.pending = true

			// skip output wrangling because this task isn't actually complete yet
			continue
		}

		for _, output := range result.Task.Outputs() {
			id := output.ID()
			s.dependencies[id]--

			// if all dependencies are done, schedule task run
			if s.dependencies[id] == 0 {
				task := s.pipeline.Tasks[id]
				run := s.newMemoryTaskRun(task)

				s.taskCh <- run
				s.waiting++
			}
		}

	}

	close(s.taskCh)
}

func (s *scheduler) report(ctx context.Context, result TaskRunResult) {
	select {
	case s.resultCh <- result:
	case <-ctx.Done():
		logger.Errorw("pipeline.scheduler: timed out reporting result", "result", result)
	}
}
