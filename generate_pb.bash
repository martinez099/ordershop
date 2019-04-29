#!/bin/bash

python3 -m grpc_tools.protoc -Icommon/ --python_out=common/ --grpc_python_out=common/ common/event_store.proto
