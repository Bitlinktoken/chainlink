package migrations

import (
	"gorm.io/gorm"
)

const up38 = `
	CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
	ALTER TABLE pipeline_runs ADD COLUMN run_id uuid NOT NULL UNIQUE DEFAULT uuid_generate_v4 ();
`
const down38 = `
	DROP CONSTRAINT IF EXISTS pipeline_runs_run_id_key;
	ALTER TABLE pipeline_runs DROP COLUMN run_id;
`

func init() {
	Migrations = append(Migrations, &Migration{
		ID: "0038_add_run_id_to_pipeline_runs",
		Migrate: func(db *gorm.DB) error {
			return db.Exec(up38).Error
		},
		Rollback: func(db *gorm.DB) error {
			return db.Exec(down38).Error
		},
	})
}
