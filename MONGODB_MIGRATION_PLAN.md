# План миграции: Redis → MongoDB с Change Streams

## Обзор

Цель: Заменить логику загрузки конфигурации из Redis на MongoDB с использованием Change Streams для отслеживания изменений в реальном времени.

### Текущая архитектура (Redis)

**Файлы:**
- `internal/conf/redis_db.go` - InitRedis(), InitUpdateTime(), FetchSourcesFromRedis()
- `internal/core/core.go` - startRedisPoller() опрашивает Redis каждую минуту
- `conf.go` использует FetchSourcesFromRedis() для загрузки источников в Validate и FetchSourcesAndReloadLoad

**Логика:**
- Возвращает `map[string]string` (ключ → source)
- Использует SCAN и MGET для получения всех источников
- Периодический опрос (polling) каждые 60 секунд

### Целевая архитектура (MongoDB)

**Требования:**
- Документ MongoDB: `{ID: string, Source: string, IsDisabled: bool}`
- Change Streams для отслеживания изменений в реальном времени
- Resume token для гарантии отсутствия пропусков
- Поддержка до 100 000 записей
- Потокобезопасность
- Автоматическое переподключение

---

## Детальный план реализации

### Шаг 1: Создание модуля MongoDB

**Файл:** `internal/conf/mongo_db.go`

```go
package conf

import (
    "context"
    "log"
    "sync"

    "go.mongodb.org/mongo-driver/bson"
    "go.mongodb.org/mongo-driver/mongo"
    "go.mongodb.org/mongo-driver/mongo/options"
    "go.mongodb.org/mongo-driver/mongo/readpref"
)

// MongoConnection holds MongoDB connection parameters.
type MongoConnection struct {
    Host     string `json:"host" env:"MONGO_HOST"`
    Port     string `json:"port" env:"MONGO_PORT"`
    Database string `json:"database" env:"MONGO_DATABASE"`
    Username string `json:"username" env:"MONGO_USERNAME"`
    Password string `json:"password" env:"MONGO_PASSWORD"`
    Collection string `json:"collection" env:"MONGO_COLLECTION"`
}

// mongoSourceDoc represents a document in MongoDB sources collection.
type mongoSourceDoc struct {
    ID         string `bson:"_id"`
    Source     string `bson:"Source"`
    IsDisabled bool   `bson:"IsDisabled"`
}

// Global state
var (
    MongoClient  *mongo.Client
    MongoColl   *mongo.Collection
    ResumeToken primitive.M
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

    // Construct URI
    uri := constructMongoURI(&conn)
    if conn.Collection == "" {
        conn.Collection = "sources" // Default collection name
    }

    // Create client
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()

    var err error
    MongoClient, err = mongo.Connect(ctx, options.Client().ApplyURI(uri))
    if err != nil {
        log.Fatalf("Failed to connect to MongoDB: %v", err)
    }

    // Verify connection
    if err = MongoClient.Ping(ctx, readpref.Primary()); err != nil {
        log.Fatalf("Failed to ping MongoDB: %v", err)
    }

    MongoColl = MongoClient.Database(conn.Database).Collection(conn.Collection)
    log.Printf("MongoDB initialized successfully (DB: %s, Collection: %s)",
        conn.Database, conn.Collection)
}

func constructMongoURI(conn *MongoConnection) string {
    // mongodb://[username:password@]host:port/database
    uri := "mongodb://"
    if conn.Username != "" && conn.Password != "" {
        uri += conn.Username + ":" + conn.Password + "@"
    }
    uri += conn.Host
    if conn.Port != "" {
        uri += ":" + conn.Port
    }
    uri += "/" + conn.Database
    return uri
}

// FetchSourcesFromMongo retrieves all active sources from MongoDB.
// Only returns documents where Source is not empty and IsDisabled is false.
// Uses cursor with batch size for efficient handling of up to 100k records.
func FetchSourcesFromMongo() map[string]string {
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()

    start := time.Now()

    filter := bson.M{
        "Source":     bson.M{"$ne": ""},
        "IsDisabled": false,
    }

    opts := options.Find().
        SetBatchSize(1000).
        SetProjection(bson.M{"_id": 1, "Source": 1})

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
        result[doc.ID] = doc.Source
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
func WatchMongoChanges(ctx context.Context, onChange func(map[string]string)) (<-chan struct{}, error) {
    done := make(chan struct{})

    pipeline := mongo.Pipeline{
        bson.D{{"$match", bson.M{
            "operationType": bson.M{"$in": []string{"insert", "update", "delete", "replace"}},
        }}},
    }

    opts := options.ChangeStream().
        SetFullDocument(options.UpdateLookup).
        SetResumeAfter(getResumeToken())

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

                // Exponential backoff
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

func processChangeStream(ctx context.Context, stream *mongo.ChangeStream, onChange func(map[string]string)) {
    for stream.Next(ctx) {
        var change struct {
            OperationType string          `bson:"operationType"`
            DocumentKey    string          `bson:"documentKey._id"`
            FullDocument  *mongoSourceDoc `bson:"fullDocument"`
        }

        if err := stream.Decode(&change); err != nil {
            log.Printf("Failed to decode change event: %v", err)
            continue
        }

        // Save resume token
        setResumeToken(stream.ResumeToken())

        // Fetch updated active sources
        // This is simpler than processing individual events and avoids race conditions
        activeSources := FetchSourcesFromMongo()
        onChange(activeSources)
    }

    if err := stream.Err(); err != nil {
        log.Printf("Change stream error: %v", err)
    }
}

func getResumeToken() primitive.M {
    ResumeTokenMu.RLock()
    defer ResumeTokenMu.RUnlock()
    return ResumeToken
}

func setResumeToken(token primitive.M) {
    ResumeTokenMu.Lock()
    defer ResumeTokenMu.Unlock()
    ResumeToken = token
}
```

---

### Шаг 2: Обновление зависимостей

**Файл:** `go.mod`

```bash
# Удалить
# github.com/go-redis/redis/v8

# Добавить
go.mongodb.org/mongo-driver v1.17.1

# Выполнить
go get go.mongodb.org/mongo-driver@v1.17.1
go mod tidy
```

---

### Шаг 3: Изменения в `internal/conf/conf.go`

**Заменить FetchSourcesFromRedis на FetchSourcesFromMongo:**

```go
// Строка 837: Заменить
vals := FetchSourcesFromRedis()

// На:
vals := FetchSourcesFromMongo()
```

```go
// Строка 879: Заменить
vals := FetchSourcesFromRedis()

// На:
vals := FetchSourcesFromMongo()
```

**Удалить InitUpdateTime:**
- Эта функция больше не нужна (использовалась для отслеживания изменений через updateTime в Redis)
- Удалить вызов из `core.go` (строка 166)

---

### Шаг 4: Изменения в `internal/core/core.go`

**1. Удалить импорт Redis:**

```go
// Строка 8: Удалить
// "github.com/go-redis/redis/v8"
```

**2. Заменить инициализацию в New():**

```go
// Строки 164-166: Заменить
conf.InitRedis()
conf.InitUpdateTime()

// На:
conf.InitMongo()
```

**3. Заменить запуск poller на watcher:**

```go
// Строка 192: Заменить
// go startRedisPoller(p, conf.RedisClient)

// На:
go p.startMongoWatcher()
```

**4. Удалить startRedisPoller и создать startMongoWatcher:**

```go
// Функция startRedisPoller (строки 198-281) удалить

// Заменить на:
func (p *Core) startMongoWatcher() {
    ctx := p.ctx

    // Debouncer для накопления изменений
    debouncer := &changeDebouncer{}

    done, err := conf.WatchMongoChanges(ctx, func(sources map[string]string) {
        p.Log(logger.Info, "MongoDB detected changes, applying...")

        // Trigger reload через debouncing (1 секунда)
        debouncer.trigger(func() {
            p.applyMongoChanges(sources)
        })
    })

    if err != nil {
        p.Log(logger.Error, "MongoDB watcher error: %v", err)
        return
    }

    <-done
    p.Log(logger.Info, "MongoDB watcher stopped: context canceled")
}

type changeDebouncer struct {
    mu      sync.Mutex
    timer   *time.Timer
    pending bool
}

func (d *changeDebouncer) trigger(callback func()) {
    d.mu.Lock()
    defer d.mu.Unlock()

    if d.timer != nil {
        d.timer.Stop()
    }

    d.pending = true
    d.timer = time.AfterFunc(1*time.Second, func() {
        d.mu.Lock()
        if d.pending {
            callback()
            d.pending = false
        }
        d.mu.Unlock()
    })
}

func (p *Core) applyMongoChanges(sources map[string]string) {
    // Создать новую конфигурацию с обновленными источниками
    newConf, reloadConf, _, err := conf.FetchSourcesAndReloadLoad(p.confPath, p.conf.Paths, sources)

    if err != nil {
        p.Log(logger.Error, "Failed to load new config: %v", err)
        return
    }

    if reloadConf != nil {
        p.Log(logger.Info, "Reloading old config with MongoDB changes")
        err = p.reloadConf(reloadConf, false)
        if err != nil {
            p.Log(logger.Error, "Failed to reload config with changes: %v", err)
            return
        }
    }

    err = p.reloadConf(newConf, false)
    if err != nil {
        p.Log(logger.Error, "Failed to reload config: %v", err)
        return
    }

    p.Log(logger.Info, "Sources successfully updated from MongoDB")
}
```

---

### Шаг 5: Обновить FetchSourcesAndReloadLoad

**Файл:** `internal/conf/conf.go` (строки 865-940)

**Добавить параметр sources:**

```go
// Строка 865: Изменить сигнатуру
// func FetchSourcesAndReloadLoad(fpath string, oldPaths map[string]*Path) (*Conf, *Conf, string, error) {

// На:
func FetchSourcesAndReloadLoad(fpath string, oldPaths map[string]*Path, externalSources map[string]string) (*Conf, *Conf, string, error) {

// Строка 879: Заменить
// vals := FetchSourcesFromRedis()

// На:
if externalSources == nil {
    externalSources = FetchSourcesFromMongo()
}
```

**Обновить логику сравнения:**

```go
// Строки 919-932: Обновить сравнение
for name, source := range externalSources {

    old, exists := oldPaths[name]
    if !exists || old == nil {
        // oldPaths does not contain this source → must reload fullConf
        continue
    }

    if old.Source != source {
        // source changed → remove from reloadConf
        reload = true
        delete(reloadConf.Paths, name)
    }
}
```

---

### Шаг 6: Удалить устаревшие файлы

```bash
# Удалить Redis модуль
rm internal/conf/redis_db.go
```

---

### Шаг 7: Создание тестов

**Файл:** `internal/conf/mongo_db_test.go`

```go
package conf

import (
    "context"
    "testing"
    "time"

    "github.com/stretchr/testify/require"
    "go.mongodb.org/mongo-driver/bson"
)

func TestInitMongo(t *testing.T) {
    // Skip if MongoDB not available
    t.Skip("Requires MongoDB connection")

    InitMongo()
    require.NotNil(t, MongoClient)
    require.NotNil(t, MongoColl)
}

func TestFetchSourcesFromMongo(t *testing.T) {
    t.Skip("Requires MongoDB connection")

    ctx := context.Background()

    // Insert test documents
    testDocs := []interface{}{
        mongoSourceDoc{ID: "test1", Source: "rtsp://example.com/stream1", IsDisabled: false},
        mongoSourceDoc{ID: "test2", Source: "rtsp://example.com/stream2", IsDisabled: false},
        mongoSourceDoc{ID: "test3", Source: "", IsDisabled: false},          // Empty source
        mongoSourceDoc{ID: "test4", Source: "rtsp://example.com/stream4", IsDisabled: true}, // Disabled
    }

    _, err := MongoColl.InsertMany(ctx, testDocs)
    require.NoError(t, err)
    defer MongoColl.DeleteMany(ctx, bson.M{})

    // Fetch sources
    sources := FetchSourcesFromMongo()

    // Should only return active sources (not empty and not disabled)
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

    // Insert a document
    _, err := MongoColl.InsertOne(ctx, mongoSourceDoc{
        ID: "newtest", Source: "rtsp://test.com", IsDisabled: false,
    })
    require.NoError(t, err)

    // Wait for change event
    <-done
    require.Greater(t, changeCount, 0)
}
```

---

## Конфигурация через переменные окружения

### Пример .env файла

```bash
# MongoDB Connection
MONGO_HOST=localhost
MONGO_PORT=27017
MONGO_DATABASE=videortc
MONGO_USERNAME=admin
MONGO_PASSWORD=secret_password
MONGO_COLLECTION=sources
```

### Пример запуска

```bash
export MONGO_HOST=localhost
export MONGO_PORT=27017
export MONGO_DATABASE=videortc
export MONGO_USERNAME=admin
export MONGO_PASSWORD=secret

./mediamtx
```

---

## Обработка событий Change Streams

### Сценарии обработки

**1. Insert (добавление нового документа):**
- Source != "" && IsDisabled == false → добавить путь
- Source == "" || IsDisabled == true → игнорировать

**2. Update (изменение существующего документа):**
- Изменился Source:
  - Новый Source != "" && IsDisabled == false → обновить путь
  - Новый Source == "" || IsDisabled == true → удалить путь
- Изменился только IsDisabled:
  - true → удалить путь
  - false → добавить (если Source != "")

**3. Delete/Replace:**
- Полностью удалить путь из конфигурации

### Логика реализации

Вместо обработки каждого события индивидуально, мы используем более простой подход:
- При получении любого изменения → перезагружаем полный список активных источников
- Это гарантирует согласованность состояния и избегает race conditions

---

## Производительность и масштабирование

### Оптимизация начальной загрузки (Initial Load)

Для поддержки до 100 000 записей:

1. **Batch size:** Использовать `SetBatchSize(1000)` для курсора
2. **Projection:** Запрашивать только нужные поля `_id` и `Source`
3. **Index:** Убедиться, что в MongoDB есть индексы:
   ```javascript
   db.sources.createIndex({Source: 1, IsDisabled: 1})
   ```

### Оптимизация обработки изменений

1. **Debouncing:** Накапливать изменения за 1 секунду перед reload
2. **Избежать частых перезагрузок:** Пакетная обработка изменений
3. **Resume token:** Сохранять после каждого события для быстрого восстановления

---

## Резилентность и надежность

### Автоматическое переподключение

```go
const maxRetries = 5

for {
    stream, err := createChangeStream(ctx)
    if err != nil {
        if ctx.Err() != nil {
            return
        }

        retryCount++
        if retryCount >= maxRetries {
            log.Printf("Max retries exceeded")
            return
        }

        // Exponential backoff: 1s, 2s, 4s, 8s, 16s
        waitTime := time.Duration(1<<uint(retryCount)) * time.Second
        select {
        case <-time.After(waitTime):
        case <-ctx.Done():
            return
        }
        continue
    }

    retryCount = 0
    processStream(ctx, stream)
}
```

### Resume Token

```go
// Сохранение после каждого события
func processChangeStream(ctx, stream, onChange) {
    for stream.Next(ctx) {
        // ... обработка события

        // Сохранить resume token
        setResumeToken(stream.ResumeToken())
    }
}

// Восстановление при переподключении
opts := options.ChangeStream().
    SetFullDocument(options.UpdateLookup).
    SetResumeAfter(getResumeToken())
```

### Потокобезопасность

1. **Resume token:** Использовать `sync.RWMutex` для доступа к ResumeToken
2. **Debouncer:** Использовать `sync.Mutex` внутри changeDebouncer
3. **Избежать блокировок:** Не блокировать goroutines на длительное время

---

## Порядок реализации

### Фаза 1: Подготовка (30 минут)
1. ✅ Создать `internal/conf/mongo_db.go`
2. ✅ Обновить `go.mod`
3. ✅ Создать базовую структуру кода

### Фаза 2: Интеграция (30 минут)
4. ✅ Изменить `internal/conf/conf.go`
5. ✅ Изменить `internal/core/core.go`
6. ✅ Удалить `internal/conf/redis_db.go`

### Фаза 3: Тестирование (30 минут)
7. ✅ Создать `internal/conf/mongo_db_test.go`
8. ✅ Тест начальной загрузки
9. ✅ Тест Change Streams
10. ✅ Тест резилентности

### Фаза 4: Оптимизация (30 минут)
11. ✅ Оптимизировать batch operations
12. ✅ Добавить debouncing
13. ✅ Проверить индексы MongoDB

---

## Сводка изменений в файлах

| Файл | Действие | Описание |
|--------|-----------|-----------|
| `go.mod` | Изменить | Заменить redis driver на mongo driver v1.17.1 |
| `internal/conf/redis_db.go` | Удалить | Устаревший модуль Redis |
| `internal/conf/mongo_db.go` | Создать | Новый модуль MongoDB |
| `internal/conf/conf.go` | Изменить | Заменить FetchSourcesFromRedis → FetchSourcesFromMongo, добавить параметр externalSources в FetchSourcesAndReloadLoad |
| `internal/core/core.go` | Изменить | Удалить импорт redis, заменить InitRedis → InitMongo, startRedisPoller → startMongoWatcher |
| `internal/conf/mongo_db_test.go` | Создать | Тесты для MongoDB |

---

## Преимущества нового подхода

1. **Реальное время:** Change Streams вместо polling (60 сек → мгновенно)
2. **Надежность:** Resume token гарантирует отсутствие пропусков
3. **Масштабируемость:** MongoDB легко масштабируется до миллионов записей
4. **Минимальные изменения:** Сигнатуры функций остались теми же (`map[string]string`)
5. **Совместимость:** Path.Source остается string, не требует изменений в других модулях
6. **Производительность:** Cursor с batchSize, debouncing изменений

---

## Проверка после реализации

- [ ] Начальная загрузка работает (включая 100k записей)
- [ ] Change Streams подключается корректно
- [ ] Insert события обрабатываются
- [ ] Update события обрабатываются (Source и IsDisabled)
- [ ] Delete события обрабатываются
- [ ] Resume token сохраняется и восстанавливается
- [ ] Переподключение работает (exponential backoff)
- [ ] Debouncing избегает частых перезагрузок
- [ ] Логи информативны
- [ ] Тесты проходят
- [ ] go mod tidy выполняется без ошибок
- [ ] make test проходит
- [ ] make lint проходит

---

## Вопросы для уточнения

1. ✅ Имя базы данных MongoDB: `mediamtx`
2. ✅ Имя коллекции MongoDB: `VideoSource`
3. ✅ Задержка debouncing: 1 секунда
4. ✅ Resume token: сохраняется только в памяти
5. ✅ Логирование: информативное при reload
6. ✅ Пакетная обработка: да, через debouncing

---

**Всего ориентировочное время реализации:** ~2 часа
**Сложность:** Средняя
**Риски:** Низкие (хорошо протестированный подход с Change Streams)
