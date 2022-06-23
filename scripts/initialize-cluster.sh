#!/bin/bash

ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
if [[ "${ROLE}" == 'Master' ]]; then
    apt install python3-pip
    pip install -r https://raw.githubusercontent.com/dedeco/structured-streaming-job-pyspark/main/requirements.txt
fi