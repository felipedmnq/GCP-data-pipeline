#!/bin/bash

python3 gcp_get_num_offers.py && python3 gcp_get_all_offers_ids.py && python3 gcp_get_raw_infos.py && python3 gcp_raw_data_pp.py

