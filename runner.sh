#!/bin/bash

./tpcds_tpch_sf10_gac.sh
./tpcds_tpch_sf100_gac.sh
./tpcds_tpch_sf1000_gac.sh

./tpcds_tpch_sf10_json_vs_grpc.sh
./tpcds_tpch_sf100_json_vs_grpc.sh
./tpcds_tpch_sf1000_json_vs_grpc.sh