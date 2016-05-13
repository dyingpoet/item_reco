#! /bin/bash
set +e

export PIG_HEAPSIZE=2096

MAIN_SCRIPT_PATH=/home/jli21/project/reco/src/production/cnp_item_main

source ./reco_membership_param.cfg

#source ${MAIN_SCRIPT_PATH}/support_driver
#if [[ $? != 0 ]]; then echo "The support module failed."; exit 1; fi

#source ${MAIN_SCRIPT_PATH}/prep_driver
#if [[ $? != 0 ]]; then echo "The prep module failed."; exit 1; fi

#echo $Output
#echo $RecoStudyScaled
###### membership scoring
#bash ${MAIN_SCRIPT_PATH}/main_scoring_thread_membership ${MAIN_SCRIPT_PATH} 1>${LOG_DIR}/main_scoring_thread_membership.log 2>&1 &

###### cardholder scoring
#source ./reco_cardholder_param.cfg
#bash ${MAIN_SCRIPT_PATH}/main_scoring_thread_cardholder 1>${LOG_DIR}/main_scoring_thread_cardholder.log 2>&1 &




ANCHOR_BLACKLIST_STRING=`cat $ANCHOR_BLACKLIST | tr '\n' , | sed 's/,$//'`
RECO_BLACKLIST_STRING=`cat $RECO_BLACKLIST | tr '\n' , | sed 's/,$//'`
#echo "hive -hiveconf Database=$DATABASE -hiveconf seasonality_tbl=$SEASONALITY_TBL -hiveconf cobought_dt=$COB_DT -hiveconf cobought_tbl=$COBOUGHT_TBL -hiveconf reco_blacklist=$RECO_BLACKLIST_STRING -hiveconf anchor_blacklist=$ANCHOR_BLACKLIST_STRING -hiveconf seasonality_month=$SEASONALITY_DT -hiveconf trans_tbl=$TRANS_TBL -hiveconf trans_partition=$TRANS_PARTITION -hiveconf item_reco_landing_dir=$Output -hiveconf member_type=$MEMBER_TYPE -f ${SRC_CORE_DIR}/CoboughtModelItem.hql"
###${Output%/*}
#if [[ ! -e ${TOUCH3} ]]; then
    #if ! hadoop fs -test -d ${Output}; then hadoop fs -mkdir ${Output}; fi
    #hive -hiveconf Database=$DATABASE -hiveconf seasonality_tbl=$SEASONALITY_TBL -hiveconf cobought_dt=$COB_DT -hiveconf cobought_tbl=$COBOUGHT_TBL -hiveconf reco_blacklist=$RECO_BLACKLIST_STRING -hiveconf anchor_blacklist=$ANCHOR_BLACKLIST_STRING -hiveconf seasonality_month=$SEASONALITY_DT -hiveconf trans_tbl=$TRANS_TBL -hiveconf trans_partition=$TRANS_PARTITION -hiveconf item_reco_landing_dir=$Output -hiveconf member_type=$MEMBER_TYPE -f debug_item_reco.hql -v 1> debug_item_reco.log

echo "hive -hiveconf seasonality_tbl=$SEASONALITY_TBL -hiveconf cobought_dt=$COB_DT -hiveconf cobought_tbl=$COBOUGHT_TBL -hiveconf seasonality_month=$SEASONALITY_DT -hiveconf member_type=$MEMBER_TYPE -hiveconf trans_tbl=$TRANS_TBL -hiveconf trans_partition=$TRANS_PARTITION"

#hive -hiveconf seasonality_tbl=$SEASONALITY_TBL -hiveconf cobought_dt=$COB_DT -hiveconf cobought_tbl=$COBOUGHT_TBL -hiveconf seasonality_month=$SEASONALITY_DT -hiveconf member_type=$MEMBER_TYPE -f debug_item_reco.hql -v 1> debug_item_reco.log
#hive -hiveconf seasonality_tbl=$SEASONALITY_TBL -hiveconf cobought_dt=$COB_DT -hiveconf cobought_tbl=$COBOUGHT_TBL -hiveconf seasonality_month=$SEASONALITY_DT -hiveconf member_type=$MEMBER_TYPE -hiveconf reco_blacklist=$RECO_BLACKLIST_STRING -hiveconf anchor_blacklist=$ANCHOR_BLACKLIST_STRING -f debug_item_reco.hql -v 1> debug_item_reco.log
#hive -hiveconf seasonality_tbl=$SEASONALITY_TBL -hiveconf cobought_dt=$COB_DT -hiveconf cobought_tbl=$COBOUGHT_TBL -hiveconf seasonality_month=$SEASONALITY_DT -hiveconf member_type=$MEMBER_TYPE -hiveconf trans_tbl=$TRANS_TBL -hiveconf trans_partition=$TRANS_PARTITION -hiveconf reco_blacklist=$RECO_BLACKLIST_STRING -hiveconf anchor_blacklist=$ANCHOR_BLACKLIST_STRING -f debug_item_reco2.hql -v 1> debug_item_reco2.log
hive -hiveconf seasonality_tbl=$SEASONALITY_TBL -hiveconf cobought_dt=$COB_DT -hiveconf cobought_tbl=$COBOUGHT_TBL -hiveconf seasonality_month=$SEASONALITY_DT -hiveconf member_type=$MEMBER_TYPE -hiveconf trans_tbl=$TRANS_TBL -hiveconf trans_partition=$TRANS_PARTITION -hiveconf reco_blacklist=$RECO_BLACKLIST_STRING -hiveconf anchor_blacklist=$ANCHOR_BLACKLIST_STRING -f debug_sample.hql -v 1> debug_sample.log
#hive -f debug_item_reco.hql -v 1> debug_item_reco.log

#    if [[ $? -ne 0 ]]; then echo "Step 3: Core recommendation failed!"; exit 3; else echo "Step 3: Core recommendation succeeded!"; touch ${TOUCH3}; fi
#else
#    echo "Step 3: Core recommendation was successfully run before!" 
#fi



