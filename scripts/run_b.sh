#!/bin/bash
python ./scripts/part_b.py -r hadoop hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/transactions hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/contracts > ./output/part_b_out.txt
