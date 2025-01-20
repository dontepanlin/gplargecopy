
BINARY_NAME=gplargecopy
BUILD_DIR=build
RELEASE_DIR=release

build:
	mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/$(BINARY_NAME) .

release-linux-amd64:
	mkdir -p $(RELEASE_DIR)
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o $(RELEASE_DIR)/$(BINARY_NAME) .

all: build
	
clean:
	@rm -rf $(BUILD_DIR)
	@rm -rf $(RELEASE_DIR)

.PHONY: all build clean release-linux-amd64