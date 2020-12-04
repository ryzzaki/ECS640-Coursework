#!/bin/bash
python ./scripts/part_b_job1.py -r hadoop hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/transactions hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/contracts > ./output/part_b_out.txt