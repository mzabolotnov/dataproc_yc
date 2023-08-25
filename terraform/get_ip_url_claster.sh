#!/bin/bash
terraform output
ip=$(yc compute instance list | grep dataproc-m | awk '{print $10}')
echo "master_node_dataproc_ext_ip = $ip" 
