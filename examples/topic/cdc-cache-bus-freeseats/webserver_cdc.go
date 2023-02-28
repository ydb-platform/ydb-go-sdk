package main

import (
	"context"
	"log"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicsugar"
)

func (s *server) cdcLoop() {
	ctx := context.Background()
	consumer := consumerName(s.id)
	reader, err := s.db.Topic().StartReader(consumer, topicoptions.ReadSelectors{
		{
			Path:     "bus/updates",
			ReadFrom: time.Now(),
		},
	},
	)
	if err != nil {
		log.Fatalf("failed to start reader: %+v", err)
	}

	log.Printf("Start cdc listen for server: %v", s.id)
	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Fatalf("failed to read message: %+v", err)
		}

		var cdcEvent struct {
			Key    []string
			Update struct {
				FreeSeats int64
			}
			Erase *struct{}
		}

		err = topicsugar.JSONUnmarshal(msg, &cdcEvent)
		if err != nil {
			log.Fatalf("failed to unmarshal message: %+v", err)
		}

		busID := cdcEvent.Key[0]
		// s.dropFromCache(busID) // used for clean cache and force database request
		if cdcEvent.Erase == nil {
			s.cache.Set(busID, cdcEvent.Update.FreeSeats) // used for direct update cache from cdc without database request
			log.Println("server-id:", s.id, "Update record: ", busID, "set freeseats:", cdcEvent.Update.FreeSeats)
		} else {
			log.Println("server-id:", s.id, "Remove record from cache: ", busID)
			s.cache.Delete(busID)
		}
		err = reader.Commit(ctx, msg)
		if err != nil {
			log.Printf("failed to commit message: %+v", err)
		}
	}
}
