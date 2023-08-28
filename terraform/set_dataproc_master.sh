#!/bin/bash
ip_dataproc=$(yc compute instance list | grep dataproc-m | awk '{print $10}')
fqdn_ch=$(terraform output | awk '{print $3}' | awk -F/ '{print $3}' | awk -F: '{print $1}')
echo $ip_dataproc "ch_raw_link="$fqdn_ch > ../ansible/inventory
cd ../ansible
ansible-playbook set_dataproc_m.yml