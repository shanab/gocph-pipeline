package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

const (
	tripsPath = "data/trips.json"
	queueURL  = "https://sqs.us-east-1.amazonaws.com/691610436071/gocph-pipeline-trips"
	charset   = "abcdefghijklmnopqrstuvwxyz" +
		"ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
		"0123456789"
	sqsMsgIDLength = 80
	sleepDur       = 1 * time.Second
)

var seededRand = rand.New(
	rand.NewSource(time.Now().UnixNano()))

type Trip struct {
	DropOffLocationID    string `json:"dolocationid"`
	Extra                string `json:"extra"`
	FareAmount           string `json:"fare_amount"`
	ImprovementSurcharge string `json:"improvement_surcharge"`
	MTATax               string `json:"mta_tax"`
	PassengerCount       string `json:"passenger_count"`
	PaymentType          string `json:"payment_type"`
	PickUpLocationID     string `json:"pulocationid"`
	RateCodeID           string `json:"ratecodeid"`
	TipAmount            string `json:"tip_amount"`
	TollsAmount          string `json:"tolls_amount"`
	TotalAmount          string `json:"total_amount"`
	DropOffTime          string `json:"tpep_dropoff_datetime"`
	PickUpTime           string `json:"tpep_pickup_datetime"`
	TripDistance         string `json:"trip_distance"`
	VendorID             string `json:"vendorid"`
}

func main() {
	cfg, err := external.LoadDefaultAWSConfig()
	if err != nil {
		log.Fatalf("unable to load SDK config: %v", err.Error())
	}
	trips, err := parseData()
	if err != nil {
		log.Fatalf("unable to parse trips data: %v", err.Error())
	}
	sqsClient := sqs.New(cfg)
	i := 0
	for {
		j := i + 5
		log.Print("Sending 5 trips to SQS...")
		if err := sendTrips(sqsClient, trips[i:j]); err != nil {
			log.Fatalf("unable to send trips: %v", err.Error())
		}
		i = j

		if i+5 >= len(trips) {
			i = 0
		}
		time.Sleep(sleepDur)
	}
}

func parseData() ([]Trip, error) {
	f, err := os.Open(tripsPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var trips []Trip
	if err := json.NewDecoder(f).Decode(&trips); err != nil {
		return nil, err
	}
	return trips, nil
}

func randomString(length int) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

func sendTrips(sqsClient *sqs.SQS, trips []Trip) error {
	var entries []sqs.SendMessageBatchRequestEntry
	for _, t := range trips {
		b, _ := json.Marshal(t)
		entries = append(entries, sqs.SendMessageBatchRequestEntry{
			Id:          aws.String(randomString(sqsMsgIDLength)),
			MessageBody: aws.String(string(b)),
		})
	}
	req := sqsClient.SendMessageBatchRequest(&sqs.SendMessageBatchInput{
		Entries:  entries,
		QueueUrl: aws.String(queueURL),
	})
	_, err := req.Send()
	if err != nil {
		return err
	}
	return nil
}
