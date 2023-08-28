#!/bin/bash
terraform output
ip=$(yc compute instance list | grep dataproc-m | awk '{print $10}')
echo $ip "ch_raw_link="$(terraform output | awk '{print $3}' | awk -F/ '{print $3}' | awk -F: '{print $1}') > ../ansible/inventory
echo "master_node_dataproc_ext_ip = $ip" 
