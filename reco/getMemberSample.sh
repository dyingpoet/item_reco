SCORE_INPUT_MEMBERSHIP=/user/pythia/Workspaces/SamsMEP/Recommend/Scoring/cnp/member/2014-11-06/Decay/recommend_score_cobought_no_smoothing_max_cosine_cobought_transaction_debug_cnp.gz
SCORE_SAMPLE_MEMBERSHIP=${SCORE_INPUT_MEMBERSHIP}.sample
SCORE_INPUT_CARDHOLDER=/user/pythia/Workspaces/SamsMEP/Recommend/Scoring/cnp/memberCard/2014-11-06/Decay/recommend_score_cobought_no_smoothing_max_cosine_cobought_transaction_debug_cnp.gz
SCORE_SAMPLE_CARDHOLDER=${SCORE_INPUT_CARDHOLDER}.sample

DT=`date +%Y-%m-%d`
pig -p ScoreInput=$SCORE_INPUT_MEMBERSHIP -p ScoreSample=$SCORE_SAMPLE_MEMBERSHIP -f getMemberSample.pig 1> get_membership_sample_${DT}.log 2>&1
pig -p ScoreInput=$SCORE_INPUT_CARDHOLDER -p ScoreSample=$SCORE_SAMPLE_CARDHOLDER -f getMemberSample.pig 1> get_cardholder_sample_${DT}.log 2>&1


