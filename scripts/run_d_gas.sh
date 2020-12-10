#!/bin/bash
python ./scripts/part_d_gas.py -r hadoop hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/transactions hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/contracts > ./output/part_d_gas_out.txt
