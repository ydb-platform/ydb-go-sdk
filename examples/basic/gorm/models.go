package main

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

type Series struct {
	ID          string    `gorm:"column:series_id;primarykey;not null"`
	Title       string    `gorm:"column:title;not null"`
	Info        string    `gorm:"column:series_info"`
	Comment     string    `gorm:"column:comment"`
	ReleaseDate time.Time `gorm:"column:release_date;not null"`

	Seasons []Season
}

func (s *Series) BeforeCreate(_ *gorm.DB) (err error) {
	id, err := uuid.NewUUID()
	if err != nil {
		return err
	}
	s.ID = id.String()
	for _, season := range s.Seasons {
		season.SeriesID = s.ID
	}
	return
}

type Season struct {
	ID         string    `gorm:"column:season_id;primarykey"`
	SeriesID   string    `gorm:"column:series_id;index"`
	Title      string    `gorm:"column:title;not null"`
	FirstAired time.Time `gorm:"column:first_aired;not null"`
	LastAired  time.Time `gorm:"column:last_aired;not null"`

	Episodes []Episode
}

func (s *Season) BeforeCreate(_ *gorm.DB) (err error) {
	id, err := uuid.NewUUID()
	if err != nil {
		return err
	}
	s.ID = id.String()
	for _, episode := range s.Episodes {
		episode.SeasonID = s.ID
	}
	return
}

type Episode struct {
	ID       string    `gorm:"column:episode_id;primarykey"`
	SeasonID string    `gorm:"column:season_id;index;not null"`
	Title    string    `gorm:"column:title;not null"`
	AirDate  time.Time `gorm:"column:air_date;not null"`
}

func (e *Episode) BeforeCreate(_ *gorm.DB) (err error) {
	id, err := uuid.NewUUID()
	if err != nil {
		return err
	}
	e.ID = id.String()
	return
}
