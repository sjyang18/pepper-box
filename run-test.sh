#!/bin/bash

# TODO: consider https://gist.github.com/jzawodn/27452

echo "[$(date)] Starting test with [$1] topics"

CONSUMESCRIPT=$(sed "s/NUMOFCONSUMERS/$1/g" ./exec-consumer.sh)

echo "Starting consumer on loadtest02.ohio"
echo "$CONSUMESCRIPT" | ssh ec2-user@loadtest02.ohio 'bash -s' &

sleep 5

echo "Starting producer on loadtest01.ohio"
./produce-mps-at.sh $1 &

wait

echo "[$(date)] Completed test with [$1] topics."
