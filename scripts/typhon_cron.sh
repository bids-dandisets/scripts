#!/bin/bash

source /data/dandi/bids-dandisets/.typhon_cron_secrets

source /data/dandi/s3-logs/conda/bin/activate bids_dandisets
cd /data/dandi/bids-dandisets/scripts
git pull
cd /data/dandi/bids-dandisets/nwb2bids
git checkout main
git pull
python /data/dandi/bids-dandisets/scripts/scripts/generate.py
git checkout alternative_sanitization
git pull
python /data/dandi/bids-dandisets/scripts/scripts/generate.py

# with `crontab -e`
# 0 18 * * * flock -n /data/dandi/bids-dandisets/work/cron.lock bash -lc '/data/dandi/bids-dandisets/scripts/typhon_cron.sh' > /data/dandi/bids-dandisets/work/cron.log 2>&1
