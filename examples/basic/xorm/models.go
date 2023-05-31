package main

import (
	"time"
)

type Series struct {
	ID          string    `xorm:"pk 'series_id'"`
	Title       string    `xorm:"'title' index(index_series_title)"`
	Info        string    `xorm:"'series_info'"`
	Comment     string    `xorm:"'comment'"`
	ReleaseDate time.Time `xorm:"'release_date'"`
}

func (*Series) TableName() string {
	return "Series"
}

type Seasons struct {
	ID         string    `xorm:"pk 'season_id'"`
	SeriesID   string    `xorm:"'series_id' index(index_series)"`
	Title      string    `xorm:"'title'"`
	FirstAired time.Time `xorm:"'first_aired'"`
	LastAired  time.Time `xorm:"'last_aired'"`
}

func (*Seasons) TableName() string {
	return "Seasons"
}

type Episodes struct {
	ID       string    `xorm:"pk 'episode_id'"`
	SeasonID string    `xorm:"'season_id' index(index_seasons)"`
	Title    string    `xorm:"'title'"`
	AirDate  time.Time `xorm:"'air_date'"`
	Views    uint64    `xorm:"'views'"`
}

func (*Episodes) TableName() string {
	return "Episodes"
}
