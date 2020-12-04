#!/bin/bash
hadoop fs -rm -r -f output/partbj1 && python ./scripts/part_b_job1.py -r hadoop --output-dir output/partbj1 --no-cat-output hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/transactions
