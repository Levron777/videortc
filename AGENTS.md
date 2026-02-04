# AGENTS.md

This document provides build commands, testing procedures, and code style guidelines for agents working on the MediaMTX project.

## Build, Test, and Lint Commands

### Build
```bash
go build .                      # Build local executable (mediamtx.exe on Windows)
make binaries                   # Build for all supported platforms
```

### Lint
```bash
make lint                       # Run all linters (Go, mod, docs, API docs)
make format                     # Format code with gofumpt
```

### Test
```bash
make test                       # Run all tests in Docker (includes race detector on 64-bit)
make test-32                    # Run tests on 32-bit system
make test-e2e                   # Run end-to-end tests

# Run single test (outside Docker)
go test -v ./internal/package                                    # All tests in package
go test -v ./internal/package -run TestFunctionName              # Specific test
go test -v ./internal/package -run "TestPrefix"                  # Tests matching pattern
go test -v -race ./internal/package                              # With race detector
go test -v -coverprofile=coverage.txt ./internal/package          # With coverage
```

### Before Committing
After making changes, run:
```bash
make format                      # Format code
make test                       # Run tests
make lint                       # Run linters
go mod tidy                     # Clean up go.mod
```

## Code Style Guidelines

### Project Structure
- **Package organization**: Each `internal/*` directory is a separate Go package
- **Entry point**: `main.go` (minimal, delegates to `internal/core`)
- **Configuration**: YAML-based with extensive comments in `mediamtx.yml`
- **Core logic**: Located in `internal/core`
- **Package names**: Match directory names, lowercase, single word preferred

### Imports
- **Order**: Standard library, third-party, internal packages (grouped by blank lines)
- **No unused imports**: Enforced by linter
- **Qualified imports**: Use qualified imports for external packages when helpful
- Example:
```go
import (
    "encoding/json"
    "net/http"

    "github.com/bluenviron/gortsplib/v5"
    "github.com/bluenviron/mediamtx/internal/auth"
    "github.com/bluenviron/mediamtx/internal/conf"
)
```

### Naming Conventions
- **Exported types/functions/constants**: PascalCase (`MyType`, `MyFunction`, `MaxConnections`)
- **Unexported types/functions/constants**: camelCase (`myVar`, `myFunc`, `maxRetry`)
- **Interface names**: Simple nouns or verb phrases ending in -er if possible (`Reader`, `Manager`)
- **Acronyms**: Keep uppercase (`HTTP`, `API`, `URL`, not `Http`, `Api`, `Url`)
- **Constants**: PascalCase (`DefaultTimeout`, not `defaultTimeout`)
- **Boolean constants**: Prefix with verbs (`isEnabled`, `hasValue`)

### Types and Structs
- **Minimal interfaces**: Prefer small, focused interfaces
- **Error types**: Custom error types implement `error` interface
- **Embedding**: Use type embedding for composition, not inheritance
- **Field order**: Group related fields together
- **JSON tags**: Include for all exported struct fields
- Example:
```go
type Manager struct {
    Field1 string `json:"field1"`
    Field2 int    `json:"field2"`
    internalField *string
}
```

### Error Handling
- **Never ignore errors**: Linter enforces checking error returns
- **Error wrapping**: Use `fmt.Errorf("context: %w", err)` for wrapping
- **Sentinel errors**: Use `errors.New()` for package-level errors
- **Custom error types**: Implement `error` interface with `Error()` method
- **Error messages**: Be specific, include context, start lowercase
- Example:
```go
if err != nil {
    return fmt.Errorf("failed to load config: %w", err)
}
```

### Functions and Methods
- **Early returns**: Return early on errors, reduce nesting
- **Receiver names**: Use 1-2 letter abbreviations or first letter of type
- **Pointer receivers**: Use for methods that modify receiver or to avoid copying
- **Defer cleanup**: Always defer cleanup (Close, Remove, etc.)
- Example:
```go
func (m *Manager) Process() error {
    m.mutex.Lock()
    defer m.mutex.Unlock()
    // ...
}
```

### Testing
- **Test package**: Use package name (not `_test` suffix) unless needed for circular imports
- **Framework**: Use `github.com/stretchr/testify/require` for assertions
- **Test functions**: `TestName(t *testing.T)` format, descriptive names
- **Sub-tests**: Use `t.Run()` for related test cases
- **Setup/teardown**: Use `defer` for cleanup
- **Helpers**: Create test helper functions for common patterns
- Example:
```go
func TestMyFunction(t *testing.T) {
    t.Run("case1", func(t *testing.T) {
        require.NoError(t, err)
    })
}
```

### Formatting
- **Tool**: Use `make format` (runs gofumpt)
- **Indentation**: Tabs, not spaces
- **Line length**: Check linter settings in `.golangci.yml`
- **Blank lines**: One blank line between top-level declarations
- **No trailing whitespace**: Enforced by linter

### Comments
- **Exported items**: Include godoc comments
- **Package comments**: Start with "Package X ..."
- **Implementation details**: Use `//` comments, keep them brief
- **TODO/FIXME**: Use sparingly, add context

### Concurrency
- **Mutexes**: Use `sync.RWMutex` for read-write scenarios, `sync.Mutex` otherwise
- **Lock ordering**: Be consistent to avoid deadlocks
- **Channels**: Prefer channels for goroutine communication
- **Defer unlock**: Always defer `mutex.Unlock()` after `Lock()` or `RLock()`
- Example:
```go
m.mutex.RLock()
defer m.mutex.RUnlock()
```

### Generics (Go 1.25+)
- **Use sparingly**: Only when abstraction is genuinely useful across types
- **Type parameters**: Single letter uppercase names preferred
- Example:
```go
func ptrOf[T any](v T) *T {
    return &v
}
```

### Dependencies
- **Vendor**: Not used; use go.mod and go.sum
- **Go version**: 1.25.0
- **Third-party**: Prefer well-maintained libraries
- **Updates**: Keep dependencies updated, run `go mod tidy` regularly

### Security
- **No hardcoded secrets**: Never commit credentials or API keys
- **Validation**: Validate all external inputs
- **TLS**: Use proper TLS configurations for network connections
- **Crypto**: Use standard library crypto functions

### Linter Configuration
See `.golangci.yml` for full configuration. Key linters:
- `gofumpt`, `goimports`: Formatting
- `errcheck`: Error checking
- `gocritic`, `revive`: Code quality
- `nilerr`: Check for nil returns

### Special Patterns
- **Configuration**: Use struct tags (`json:"fieldName"`) for YAML/JSON parsing
- **Logging**: Use `internal/logger` package, structured logging
- **Time handling**: Use `time.Duration` type with multiplications (`2 * time.Second`)
- **File paths**: Use `filepath` package for cross-platform path handling

This project enforces strict code quality through comprehensive linting. Ensure all checks pass before submitting changes.
