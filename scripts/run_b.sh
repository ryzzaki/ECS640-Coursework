#!/bin/bash
hadoop fs -rm -r -f output/partb && python ./scripts/part_b.py -r hadoop --output-dir output/partb --no-cat-output hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/transactions hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/contracts
