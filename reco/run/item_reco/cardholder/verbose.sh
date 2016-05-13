#! /bin/bash
set +e

export PIG_HEAPSIZE=2096

MAIN_SCRIPT_PATH=/home/jli21/project/reco/src/production/cnp_item_main

source ./reco_membership_param.cfg

echo $ItemInfoSnapshot
echo $TMP_SUPPORT_DIR
echo $Weekly
echo $ItemInfoDump

