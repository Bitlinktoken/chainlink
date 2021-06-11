package migrations

import (
	"gorm.io/gorm"
)

const up39 = `
CREATE TABLE job_proposals (
    id BIGSERIAL PRIMARY KEY,
	spec TEXT NOT NULL,
	status VARCHAR (50) NOT NULL,
	job_id uuid NOT NULL,
	feeds_manager_id int NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
	CONSTRAINT fk_feeds_manager FOREIGN KEY(feeds_manager_id) REFERENCES feeds_managers(id)
);
CREATE INDEX idx_job_proposals_job_id on job_proposals (job_id);
CREATE INDEX idx_job_proposals_feeds_manager_id on job_proposals (feeds_manager_id);
`

const down39 = `
	DROP TABLE job_proposals
`

func init() {
	Migrations = append(Migrations, &Migration{
		ID: "0039_create_job_proposals",
		Migrate: func(db *gorm.DB) error {
			return db.Exec(up39).Error
		},
		Rollback: func(db *gorm.DB) error {
			return db.Exec(down39).Error
		},
	})
}
