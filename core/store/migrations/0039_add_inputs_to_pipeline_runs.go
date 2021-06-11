package migrations

import (
	"gorm.io/gorm"
)

// TODO: do we need to modify the constraint
const up39 = `
	ALTER TABLE pipeline_runs ADD COLUMN inputs jsonb;
`
const down39 = `
	ALTER TABLE pipeline_runs DROP COLUMN inputs;
`

func init() {
	Migrations = append(Migrations, &Migration{
		ID: "0039_add_inputs_to_pipeline_runs",
		Migrate: func(db *gorm.DB) error {
			return db.Exec(up39).Error
		},
		Rollback: func(db *gorm.DB) error {
			return db.Exec(down39).Error
		},
	})
}
