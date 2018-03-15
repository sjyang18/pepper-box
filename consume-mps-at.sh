#!/bin/bash
java -cp target/pepper-box-1.0.jar:.  com.gslab.pepper.PepperBoxLoadConsumer --consumer-config-file pblg.properties --num-consumers 3  --topic-name mps.$1 --per-thread-topics YES --test-duration 20 --throughput-per-consumer $1 --starting-offset 0 &> consume_mps_at.$1.log
