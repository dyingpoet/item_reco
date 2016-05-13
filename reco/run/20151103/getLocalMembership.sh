
#sudo \rm -rf /home/app/scs/recommend/product_landing/membership_last/*

LOCAL_PATH=/home/app/scs/recommend/product_landing
#sudo \rm -rf ${LOCAL_PATH}/cardholder_last/*
#sudo \mv ${LOCAL_PATH}/cardholder/* ${LOCAL_PATH}/cardholder_last/
sudo \rm -rf ${LOCAL_PATH}/membership_last/*
sudo \mv ${LOCAL_PATH}/membership/* ${LOCAL_PATH}/membership_last/

export DS_RUN=2015-11-04
export COB_TYPE=cosine
export AGG_METHOD="Decay"
export OutputDir=/user/pythia/Workspaces/SamsMEP

TYPE=cnp_member
export RecoScore=$OutputDir/Recommend/Scoring/${TYPE}/${DS_RUN}/${AGG_METHOD}/recommend_score_cobought_no_smoothing_max_${COB_TYPE}_cobought_transaction_debug
export RecoMember=${RecoScore}"_cnp.gz"
echo ${RecoMember}
sudo hadoop fs -get ${RecoMember}/pa*  ${LOCAL_PATH}/membership/

#TYPE=cnp_memberCard
#export RecoScore=$OutputDir/Recommend/Scoring/${TYPE}/${DS_RUN}/${AGG_METHOD}/recommend_score_cobought_no_smoothing_max_${COB_TYPE}_cobought_transaction_debug
#export RecoCard=${RecoScore}"_cnp.gz"
#echo ${RecoCard}
#sudo hadoop fs -get ${RecoCard}/pa*  ${LOCAL_PATH}/cardholder/

