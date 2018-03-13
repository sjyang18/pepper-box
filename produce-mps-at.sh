#!/usr/bin/env bash
java -cp target/pepper-box-1.0.jar:.  com.gslab.pepper.PepperBoxLoadGenerator --schema-file schema1000.txt --producer-config-file pblg.properties --topic-name mps.$1 --per-thread-topics YES  --throughput-per-producer $1 --test-duration 20 --num-producers 3  --starting-offset 0 &> produce_mps_at.$1.log

