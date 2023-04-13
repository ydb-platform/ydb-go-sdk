package main

import (
	"errors"
	"log"
	"os"

	ydb "github.com/ydb-platform/gorm-driver"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

//nolint:lll
func initDB(cfg *gorm.Config) (*gorm.DB, error) {
	// docker run -it postgres psql -h 127.0.0.1 -p 5432 -U postgres -d postgres
	// POSTGRES_CONNECTION_STRING="user=postgres password=mysecretpassword dbname=postgres host=127.0.0.1 port=5432 sslmode=disable"
	if dsn, has := os.LookupEnv("POSTGRES_CONNECTION_STRING"); has {
		return gorm.Open(postgres.Open(dsn), cfg)
	}
	// SQLITE_CONNECTION_STRING=./test.db
	if dsn, has := os.LookupEnv("SQLITE_CONNECTION_STRING"); has {
		return gorm.Open(sqlite.Open(dsn), cfg)
	}
	if dsn, has := os.LookupEnv("YDB_CONNECTION_STRING"); has {
		return gorm.Open(ydb.Open(dsn, ydb.WithTablePathPrefix("gorm")), cfg)
	}
	return nil, errors.New("cannot initialize DB")
}

func main() {
	// connect
	db, err := initDB(&gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
	})
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
