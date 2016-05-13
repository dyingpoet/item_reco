DT=2014-10-08
SCORE_INPUT_MEMBERSHIP=/user/jli21/pythia/Workspaces/CNP/reco/$DT/preference/item_membership_preference
SCORE_SAMPLE_MEMBERSHIP=${SCORE_INPUT_MEMBERSHIP}_sample
#SCORE_INPUT_CARDHOLDER=/user/pythia/Workspaces/SamsMEP/Recommend/Scoring/cnp/memberCard/20141008Decay/recommend_score_cobought_no_smoothing_max_cosine_cobought_transaction_debug_cnp_left.gz
#SCORE_SAMPLE_CARDHOLDER=${SCORE_INPUT_CARDHOLDER}.sample

pig -p ScoreInput=$SCORE_INPUT_MEMBERSHIP -p ScoreSample=$SCORE_SAMPLE_MEMBERSHIP -f getMemberItemPrefSample.pig


