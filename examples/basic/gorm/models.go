package main

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

type Series struct {
	ID          string    `gorm:"column:series_id;primarykey;not null"`
	Title       string    `gorm:"column:title"`
	Info        string    `gorm:"column:series_info"`
	Comment     string    `gorm:"column:comment"`
	ReleaseDate time.Time `gorm:"column:release_date"`

	Seasons []Season
}

func (s *Series) BeforeCreate(_ *gorm.DB) (err error) {
	id, err := uuid.NewUUID()
	if err != nil {
		return err
	}
	s.ID = id.String()
	for i := range s.Seasons {
		s.Seasons[i].SeriesID = s.ID
	}
	return
}

type Season struct {
	ID         string    `gorm:"column:season_id;primarykey;not null"`
	SeriesID   string    `gorm:"column:series_id;index"`
	Title      string    `gorm:"column:title"`
	FirstAired time.Time `gorm:"column:first_aired"`
	LastAired  time.Time `gorm:"column:last_aired"`

	Episodes []Episode
}

func (s *Season) BeforeCreate(_ *gorm.DB) (err error) {
	id, err := uuid.NewUUID()
	if err != nil {
		return err
	}
	s.ID = id.String()
	for i := range s.Episodes {
		s.Episodes[i].SeasonID = s.ID
	}
	return
}

type Episode struct {
	ID       string    `gorm:"column:episode_id;primarykey;not null"`
	SeasonID string    `gorm:"column:season_id;index"`
	Title    string    `gorm:"column:title"`
	AirDate  time.Time `gorm:"column:air_date"`
}

func (e *Episode) BeforeCreate(_ *gorm.DB) (err error) {
	id, err := uuid.NewUUID()
	if err != nil {
		return err
	}
	e.ID = id.String()
	return
}
