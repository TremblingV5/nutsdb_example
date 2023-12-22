package main

import (
	"log"
	"os"
	"strconv"
	"time"

	"github.com/nutsdb/nutsdb"
	"github.com/panjf2000/ants/v2"
)

var DB *nutsdb.DB

func init() {
	db, err := nutsdb.Open(
		nutsdb.DefaultOptions,
		nutsdb.WithDir("./nutsdb"),
	)
	if err != nil {
		panic(err)
	}
	DB = db

	logFile, err := os.OpenFile("./nutsdb.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		panic(err)
	}
	log.SetOutput(logFile)
}

func main() {
	defer DB.Close()
	defer ants.Release()

	limit := 12500
	loop := 1000

	for i := 0; i < limit; i++ {
		key := "test:" + strconv.FormatUint(uint64(i), 10)
		score := time.Now().UnixMilli()

		for j := 0; j < loop; j++ {
			mid := strconv.FormatUint(uint64(j), 10)
			err := DB.Update(func(tx *nutsdb.Tx) error {
				return tx.ZAdd("test", []byte(key), float64(score), []byte(mid))
			})
			if err != nil {
				log.Fatal(err)
			}
		}

		log.Printf("finised limit: %d", i)
	}
}
