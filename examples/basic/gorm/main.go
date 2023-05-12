package main

import (
	"context"
	"log"
	"os"
	"time"

	ydb "github.com/ydb-platform/gorm-driver"
	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// connect
	dsn, exists := os.LookupEnv("YDB_CONNECTION_STRING")
	if !exists {
		panic("YDB_CONNECTION_STRING environment variable not defined")
	}

	db, err := gorm.Open(
		ydb.Open(
			dsn,
			ydb.WithTablePathPrefix("gorm"),
			ydb.With(
				environ.WithEnvironCredentials(ctx),
			),
		),
		&gorm.Config{
			Logger: logger.Default.LogMode(logger.Error),
		},
	)
	if err != nil {
		panic(err)
	}

	// prepare scheme and migrations
	if err = prepareScheme(db); err != nil {
		panic(err)
	}

	// fill data
	if err = fillData(db); err != nil {
		panic(err)
	}

	// read all data
	if err = readAll(db); err != nil {
		panic(err)
	}

	// find by condition
	if err = findEpisodesByTitle(db, "Bad"); err != nil {
		panic(err)
	}
}

func prepareScheme(db *gorm.DB) error {
	if err := db.Migrator().DropTable(
		&Series{},
		&Season{},
		&Episode{},
	); err != nil {
		return err
	}
	return db.AutoMigrate(
		&Series{},
		&Season{},
		&Episode{},
	)
}

func fillData(db *gorm.DB) error {
	return db.Create(data).Error
}

func readAll(db *gorm.DB) error {
	// get all series
	var series []Series
	if err := db.Preload("Seasons.Episodes").Find(&series).Error; err != nil {
		return err
	}
	log.Println("all known series:")
	for _, s := range series {
		log.Printf(
			"  > [%s]     %s (%s)\n",
			s.ID, s.Title, s.ReleaseDate.Format("2006"),
		)
		for _, ss := range s.Seasons {
			log.Printf(
				"    > [%s]   %s\n",
				ss.ID, ss.Title,
			)
			for _, e := range ss.Episodes {
				log.Printf(
					"      > [%s] [%s] %s\n",
					e.ID, e.AirDate.Format(dateISO8601), e.Title,
				)
			}
		}
	}
	return nil
}

func findEpisodesByTitle(db *gorm.DB, fragment string) error {
	var episodes []Episode
	if err := db.Find(&episodes, clause.Like{
		Column: "title",
		Value:  "%" + fragment + "%",
	}).Error; err != nil {
		return err
	}
	log.Println("all episodes with title with word 'bad':")
	for _, e := range episodes {
		ss := Season{
			ID: e.SeasonID,
		}
		if err := db.Take(&ss).Error; err != nil {
			return err
		}
		s := Series{
			ID: ss.SeriesID,
		}
		if err := db.Take(&s).Error; err != nil {
			return err
		}
		log.Printf(
			"  > [%s]     %s (%s)\n",
			s.ID, s.Title, s.ReleaseDate.Format("2006"),
		)
		log.Printf(
			"    > [%s]   %s\n",
			ss.ID, ss.Title,
		)
		log.Printf(
			"      > [%s] [%s] %s\n",
			e.ID, e.AirDate.Format(dateISO8601), e.Title,
		)
	}
	return nil
}
