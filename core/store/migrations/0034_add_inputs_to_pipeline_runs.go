package migrations

import (
	"gorm.io/gorm"
)

// TODO: do we need to modify the constraint
const up34 = `
	ALTER TABLE pipeline_runs ADD COLUMN inputs jsonb;
`
const down34 = `
	ALTER TABLE pipeline_runs DROP COLUMN inputs;
`

func init() {
	Migrations = append(Migrations, &Migration{
		ID: "0034_add_inputs_to_pipeline_runs",
		Migrate: func(db *gorm.DB) error {
			return db.Exec(up34).Error
		},
		Rollback: func(db *gorm.DB) error {
			return db.Exec(down34).Error
		},
	})
}
