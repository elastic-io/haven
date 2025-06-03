PROJECT_NAME := haven

VERSION := $(shell cat VERSION)
GIT_COMMIT := $(shell git rev-parse HEAD)

GO := go
GOOS := linux
GOARCH := amd64

OUTPUT_DIR := bin
OUTPUT_BIN := $(OUTPUT_DIR)/$(PROJECT_NAME)

SRC_DIR := .

all: build

build:
	@echo "Building $(PROJECT_NAME) for macOS..."
	@mkdir -p $(OUTPUT_DIR)
	@$(GO) build -ldflags "-X main.version=${VERSION} -X main.gitCommit=${GIT_COMMIT}" -o $(OUTPUT_BIN) $(SRC_DIR)
	@echo "Build completed: $(OUTPUT_BIN) for macOS"

build-amd:
	@echo "Building $(PROJECT_NAME) for $(GOOS)/$(GOARCH)..."
	@mkdir -p $(OUTPUT_DIR)
	@GOOS=$(GOOS) GOARCH=$(GOARCH) $(GO) build -ldflags "-X main.version=${VERSION} -X main.gitCommit=${GIT_COMMIT}" -o $(OUTPUT_BIN)-amd $(SRC_DIR)
	@echo "Build completed: $(OUTPUT_BIN)-amd"

build-arm:
	@echo "Building $(PROJECT_NAME) for $(GOOS)/arm..."
	@mkdir -p $(OUTPUT_DIR)
	@GOOS=$(GOOS) GOARCH=arm $(GO) build -ldflags "-X main.version=${VERSION} -X main.gitCommit=${GIT_COMMIT}" -o $(OUTPUT_BIN)-arm $(SRC_DIR)
	@echo "Build completed: $(OUTPUT_BIN)-arm"

build-all: build build-amd build-arm

clean:
	@echo "Cleaning build artifacts..."
	@rm -rf $(OUTPUT_DIR)
	@echo "Clean completed."

run: build
	@echo "Running $(PROJECT_NAME)..."
	@$(OUTPUT_BIN)

test:
	@echo "Running tests..."
	@$(GO) test -v ./...

deps:
	@echo "Installing dependencies..."
	@$(GO) mod download

help:
	@echo "Available targets:"
	@echo "  all       - Build the project for macOS (default target)"
	@echo "  build     - Build the project for macOS"
	@echo "  build-amd - Build the project for x86"
	@echo "  build-arm - Build the project for ARM"
	@echo "  build-all - Build the project for both x86/ARM/macOS"
	@echo "  clean     - Clean build artifacts"
	@echo "  run       - Build and run the project (x86 version)"
	@echo "  test      - Run tests"
	@echo "  deps      - Install dependencies"
	@echo "  help      - Show this help message"

.PHONY: all build build-amd build-arm build-all clean run test deps help