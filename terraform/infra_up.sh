#!/bin/bash
terraform apply -auto-approve
./set_dataproc_master.sh
