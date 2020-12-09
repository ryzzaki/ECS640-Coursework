#!/bin/bash
python ./scripts/part_d_scams.py -r hadoop --file input/scams.json hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/transactions > ./output/part_d_scams_out.txt
