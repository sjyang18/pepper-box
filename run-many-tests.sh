#!/bin/bash
./run-test.sh 1 &&
sleep 240 &&
./run-test.sh 2 &&
sleep 240 &&
./run-test.sh 5 &&
sleep 240 &&
./run-test.sh 10 &&
sleep 240 &&
./run-test.sh 50 &&
sleep 240 &&
./run-test.sh 125 &&
sleep 240 &&
./run-test.sh 250 &&
sleep 240 &&
./run-test.sh 500 &&
sleep 240 &&
./run-test.sh 1000 &&
sleep 240 &&
./run-test.sh 2000
echo "Completed all tests"
