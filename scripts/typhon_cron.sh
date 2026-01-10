#!/bin/bash

source /data/dandi/bids-dandisets/.typhon_cron_secrets
source /data/dandi/s3-logs/conda/bin/activate bids_dandisets

git -C /data/dandi/bids-dandisets/nwb2bids pull
python /data/dandi/bids-dandisets/scripts/scripts/generate.py --branch draft
python /data/dandi/bids-dandisets/scripts/scripts/generate.py --branch basic_sanitization

# with `crontab -e`
# 0 18 * * * flock -n /data/dandi/bids-dandisets/work/cron.lock bash -lc  '/data/dandi/bids-dandisets/scripts/scripts/typhon_cron.sh' > /data/dandi/bids-dandisets/work/cron.log 2>&1
