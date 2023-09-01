#!/bin/bash
yc storage bucket delete $(terraform output | awk 'NR==2 {print $3}' | sed 's/\"//g')
terraform destroy
