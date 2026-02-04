# MediaMTX Project (`mediamtx`)

This document provides a brief overview of the MediaMTX project, its structure, and key commands for development.

## Project Overview

MediaMTX is a ready-to-use and zero-dependency real-time media server and media proxy that allows publishing, reading, proxying, recording, and playback of video and audio streams. It is written in Go and is designed to be a single, self-contained executable.

The core application logic is located in the `internal/core` directory. The main entry point of the application is in `main.go`. Configuration is handled through a YAML file, `mediamtx.yml`, which is extensively commented.

### Key Directories

-   `api/`: Contains the OpenAPI specification.
-   `docker/`: Dockerfiles for building various images.
-   `docs/`: Project documentation.
-   `internal/`: All the internal Go packages.
-   `scripts/`: Makefiles for various build/test tasks.

## Building and Running

The project uses a `Makefile` to simplify the build and test process.

### Building

To build the `mediamtx` executable, you can use the `make binaries` command. This will build the binaries for all supported platforms and place them in the `binaries` directory.

```sh
make binaries
```

For a simple local build, you can also use the standard `go build` command:

```sh
go build .
```

This will create a `videortc.exe` (or `videortc` on Linux/macOS) executable in the root directory.

### Running

To run the server, execute the compiled binary. It will automatically load the `mediamtx.yml` configuration file from the current directory.

```sh
./mediamtx
```

(On Windows, use `mediamtx.exe`)

You can also run the project directly using `go run`:

```sh
go run .
```

## Development Conventions

### Testing

To run the test suite, use the following command:

```sh
make test
```

This will run all the unit tests in a Docker container.

### Linting

To check the code against the linter, run:

```sh
make lint
```

### Formatting

To format the Go code, run:

```sh
make format
```
