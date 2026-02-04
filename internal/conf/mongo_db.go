package conf

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/bluenviron/mediamtx/internal/conf/env"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// MongoConnection holds MongoDB connection parameters.
type MongoConnection struct {
	Host       string `json:"host" env:"MONGO_HOST"`
	Port       string `json:"port" env:"MONGO_PORT"`
	Database   string `json:"database" env:"MONGO_DATABASE"`
	Username   string `json:"username" env:"MONGO_USERNAME"`
	Password   string `json:"password" env:"MONGO_PASSWORD"`
	Collection string `json:"collection" env:"MONGO_COLLECTION"`
}

// mongoSourceDoc represents a document in MongoDB sources collection.
type mongoSourceDoc struct {
	Name       string `bson:"Name"`
	Source     string `bson:"Source"`
	IsDisabled bool   `bson:"IsDisabled"`
}

// MongoChange represents a single change event from MongoDB change stream.
type MongoChange struct {
	OperationType string // insert, update, delete, replace
	Name          string
	Source        string
	IsDisabled    bool
}

// Global state
var (
	MongoClient   *mongo.Client
	MongoColl     *mongo.Collection
	ResumeToken   bson.Raw
	ResumeTokenMu sync.RWMutex
)

// InitMongo initializes MongoDB client from environment variables.
// Panics if configuration is missing or connection fails.
func InitMongo() {
	log.Println("Loading MongoDB configuration from environment variables")

	var conn MongoConnection
	if err := env.Load("MONGO", &conn); err != nil {
		log.Fatalf("Missing MongoDB configuration: %v", err)
	}

	uri := constructMongoURI(&conn)
	if conn.Collection == "" {
		conn.Collection = "VideoSource"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var err error
	MongoClient, err = mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		log.Fatalf("Failed to connect to MongoDB: %v", err)
	}

	if err = MongoClient.Ping(ctx, readpref.Primary()); err != nil {
		log.Fatalf("Failed to ping MongoDB: %v", err)
	}

	if conn.Database == "" {
		conn.Database = "mediamtx"
	}

	MongoColl = MongoClient.Database(conn.Database).Collection(conn.Collection)
	log.Printf("MongoDB initialized successfully (DB: %s, Collection: %s)",
		conn.Database, conn.Collection)
}

func constructMongoURI(conn *MongoConnection) string {
	uri := "mongodb://127.0.0.1:27017/?replicaSet=rs0&directConnection=true" //uri := "mongodb://"
	//if conn.Username != "" && conn.Password != "" {
	//	uri += conn.Username + ":" + conn.Password + "@"
	//}
	//uri += conn.Host
	//if conn.Port != "" {
	//	uri += ":" + conn.Port
	//}
	//uri += "/" + conn.Database
	return uri
}

// FetchSourcesFromMongo retrieves all active sources from MongoDB.
// Only returns documents where Source is not empty and IsDisabled is false.
// Uses cursor with batch size for efficient handling of up to 100k records.
// Returns nil if MongoDB is not initialized.
func FetchSourcesFromMongo() map[string]string {
	if MongoColl == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	start := time.Now()

	filter := primitive.M{
		"Source":     primitive.M{"$ne": ""},
		"IsDisabled": false,
	}

	opts := options.Find().
		SetBatchSize(1000).
		SetProjection(primitive.M{"Name": 1, "Source": 1})

	cursor, err := MongoColl.Find(ctx, filter, opts)
	if err != nil {
		log.Fatalf("MongoDB Find failed: %v", err)
	}
	defer cursor.Close(ctx)

	result := make(map[string]string)
	var doc mongoSourceDoc

	for cursor.Next(ctx) {
		if err := cursor.Decode(&doc); err != nil {
			log.Printf("Failed to decode document: %v", err)
			continue
		}

		result[doc.Name] = doc.Source
	}

	if err := cursor.Err(); err != nil {
		log.Fatalf("Cursor iteration error: %v", err)
	}

	elapsed := time.Since(start)
	log.Printf("Retrieved %d active sources from MongoDB in %v", len(result), elapsed)
	return result
}

// WatchMongoChanges subscribes to MongoDB Change Streams and calls onChange callback
// when active sources configuration changes. Handles reconnection with resume token.
func WatchMongoChanges(ctx context.Context, onChange func(MongoChange)) (<-chan struct{}, error) {
	done := make(chan struct{})

	pipeline := mongo.Pipeline{
		primitive.D{{"$match", primitive.M{
			"operationType": primitive.M{"$in": []string{"insert", "update", "delete", "replace"}},
		}}},
	}

	opts := options.ChangeStream().
		SetFullDocument(options.UpdateLookup)

	resumeToken := getResumeToken()
	if len(resumeToken) > 0 {
		opts.SetResumeAfter(resumeToken)
	}

	go func() {
		defer close(done)

		const maxRetries = 5
		retryCount := 0

		for {
			stream, err := MongoColl.Watch(ctx, pipeline, opts)
			if err != nil {
				if ctx.Err() != nil {
					return
				}

				log.Printf("Failed to create change stream (attempt %d): %v", retryCount+1, err)

				retryCount++
				if retryCount >= maxRetries {
					log.Printf("Max retries exceeded, giving up")
					return
				}

				waitTime := time.Duration(1<<uint(retryCount)) * time.Second
				select {
				case <-time.After(waitTime):
				case <-ctx.Done():
					return
				}
				continue
			}

			retryCount = 0
			log.Println("MongoDB Change Stream connected successfully")

			processChangeStream(ctx, stream, onChange)
		}
	}()

	return done, nil
}

func processChangeStream(ctx context.Context, stream *mongo.ChangeStream, onChange func(MongoChange)) {
	for stream.Next(ctx) {
		var change struct {
			OperationType string `bson:"operationType"`
			DocumentKey   struct {
				ID primitive.ObjectID `bson:"_id"`
			} `bson:"documentKey"`
			FullDocument *mongoSourceDoc `bson:"fullDocument"`
			UpdateDesc   struct {
				UpdatedFields map[string]interface{} `bson:"updatedFields"`
			} `bson:"updateDescription"`
		}

		if err := stream.Decode(&change); err != nil {
			log.Printf("Failed to decode change event: %v", err)
			continue
		}

		setResumeToken(stream.ResumeToken())

		mongoChange := MongoChange{
			OperationType: change.OperationType,
			Name:          "",
			Source:        "",
			IsDisabled:    false,
		}

		switch change.OperationType {
		case "insert", "replace":
			if change.FullDocument != nil {
				mongoChange.Name = change.FullDocument.Name
				mongoChange.Source = change.FullDocument.Source
				mongoChange.IsDisabled = change.FullDocument.IsDisabled
			}

		case "update":
			if change.FullDocument != nil {
				mongoChange.Name = change.FullDocument.Name
				mongoChange.Source = change.FullDocument.Source
				mongoChange.IsDisabled = change.FullDocument.IsDisabled
			}

		case "delete":
			docID := change.DocumentKey.ID.Hex()
			log.Printf("Document deleted with ID: %s", docID)
			mongoChange.Name = docID
		}

		onChange(mongoChange)
	}

	if err := stream.Err(); err != nil {
		log.Printf("Change stream error: %v", err)
	}
}

func getResumeToken() bson.Raw {
	ResumeTokenMu.RLock()
	defer ResumeTokenMu.RUnlock()
	return ResumeToken
}

func setResumeToken(token bson.Raw) {
	ResumeTokenMu.Lock()
	defer ResumeTokenMu.Unlock()
	ResumeToken = token
}
