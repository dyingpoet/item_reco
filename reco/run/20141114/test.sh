export DS_RUN=2014-11-17
#export DS_RUN=`date +%Y-%m-%d`
echo "DS_RUN=${DS_RUN}"
export DT=`date -d "1 day ago ${DS_RUN}" +%Y-%m-%d`
echo $DT
