#!/usr/bin/env bash

java -cp build/jar/libDeli-client.jar:conf:lib/hbase-binding-0.1.4.jar tthbase.client.Demo  $(pwd)/lib/libDeli-coproc.jar
