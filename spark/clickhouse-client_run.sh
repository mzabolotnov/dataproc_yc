#!/bin/bash


host_clickhouse=$(awk 'NR==1 {print; exit}' ./ch_host)
clickhouse-client --host "$host_clickhouse" \
                  --secure \
                  --user user1 \
                  --database db1 \
                  --port 9440 \
                  --ask-password
