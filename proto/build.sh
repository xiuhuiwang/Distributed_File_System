#!/usr/bin/env bash

# First, install Google Protocol Buffers.
#
# If "Permission denied", try run: 
#     chmod 777 build.sh
#
# If you don't have protoc-gen-go:
#     go install google.golang.org/protobuf/cmd/protoc-gen-go@latest

PATH="$PATH:${GOPATH}/bin:${HOME}/go/bin" protoc --go_out=../dfs/ ./dfs.proto
