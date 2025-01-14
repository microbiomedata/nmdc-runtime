ls -dlt /global/cfs/projectdirs/m3408/nmdc-mongodumps/dump_nmdc-prod_* | head -n 1 | awk -F' ' '{print $NF}'

