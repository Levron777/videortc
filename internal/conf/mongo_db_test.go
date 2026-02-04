package conf

import (
	"context"
	"testing"
	"time"

	"github.com/bluenviron/mediamtx/internal/conf/env"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestInitMongo(t *testing.T) {
	t.Skip("Requires MongoDB connection")

	InitMongo()
	require.NotNil(t, MongoClient)
	require.NotNil(t, MongoColl)
}

func TestFetchSourcesFromMongo(t *testing.T) {
	t.Skip("Requires MongoDB connection")

	ctx := context.Background()

	testDocs := []interface{}{
		mongoSourceDoc{Name: "test1", Source: "rtsp://example.com/stream1", IsDisabled: false},
		mongoSourceDoc{Name: "test2", Source: "rtsp://example.com/stream2", IsDisabled: false},
		mongoSourceDoc{Name: "test3", Source: "", IsDisabled: false},
		mongoSourceDoc{Name: "test4", Source: "rtsp://example.com/stream4", IsDisabled: true},
	}

	_, err := MongoColl.InsertMany(ctx, testDocs)
	require.NoError(t, err)
	defer MongoColl.DeleteMany(ctx, primitive.M{})

	sources := FetchSourcesFromMongo()

	require.Equal(t, 2, len(sources))
	require.Equal(t, "rtsp://example.com/stream1", sources["test1"])
	require.Equal(t, "rtsp://example.com/stream2", sources["test2"])
	require.NotContains(t, sources, "test3")
	require.NotContains(t, sources, "test4")
}

func TestWatchMongoChanges(t *testing.T) {
	t.Skip("Requires MongoDB connection")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	changeCount := 0
	done, err := WatchMongoChanges(ctx, func(sources map[string]string) {
		changeCount++
	})
	require.NoError(t, err)

	_, err = MongoColl.InsertOne(ctx, mongoSourceDoc{
		Name: "newtest", Source: "rtsp://test.com", IsDisabled: false,
	})
	require.NoError(t, err)

	<-done
	require.Greater(t, changeCount, 0)
}

func TestEnvLoad(t *testing.T) {
	conn := MongoConnection{}
	err := env.Load("MONGO", &conn)
	require.NoError(t, err)
}

func TestResumeToken(t *testing.T) {
	testToken := bson.Raw{0x00, 0x01, 0x02}
	setResumeToken(testToken)
	retrievedToken := getResumeToken()
	require.Equal(t, testToken, retrievedToken)
}
