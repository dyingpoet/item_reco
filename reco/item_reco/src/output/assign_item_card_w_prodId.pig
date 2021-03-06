--set default_parallel 100
set default_parallel 1000
set mapred.task.timeout 1800000
SET mapred.output.compress 'true';
SET mapred.output.compression.codec 'org.apache.hadoop.io.compress.GzipCodec';
--set output.compression.enabled true;
--set output.compression.codec org.apache.hadoop.io.compress.GzipCodec;


/* input: 1) reco output: <member|card|createDate|subcat|score> (here the card can be any card or pcard)	2) card_club <member,card,member_create_date,preferred_club,assigned_club,issuing_club>	3) output from sc2item.pig <club,subcat,item>
   output: <member|card|createDate|subcat|item|score> top30 subcats for each member  (next join with itemInfo to get customer_item_nbr and itemDesc)
*/



recoRaw = LOAD '$reco' USING PigStorage('\u0001') AS (
        membership_nbr: chararray
        , cardholder_nbr: chararray
        , membership_create_date: chararray
        --, cat_subcat_nbr: chararray
        , system_item_nbr: int
        , score: float
        -- , anchor_cat_subcat_nbr: chararray
);

prodIdMap = LOAD '$prodIdMap' USING PigStorage('\t') AS (system_item_nbr: int, product_id: chararray);

itemInfo = LOAD '$itemInfo' USING PigStorage('\u0001') AS (system_item_nbr:int,category_nbr:int,sub_category_nbr:int,customer_item_nbr:int,consumer_item_desc:chararray,product_desc:chararray);

/*
cardClub = LOAD '$cardClub' USING PigStorage('\u0001') AS (
        membership_nbr: chararray,
        cardholder_nbr: chararray,
        membership_create_date: chararray,
        preferred_club: chararray,
        assigned_club: chararray,
        issued_club: chararray
);

sc2item = LOAD '$clubSubcatItem' USING PigStorage('\u0001') AS (club:chararray, subcat:chararray, item:int);

--weeklySeasonality = LOAD '$' USING PigStorage('\u0001') AS ();
*/


recoRawGrp = GROUP recoRaw BY (membership_nbr, cardholder_nbr, membership_create_date);

reco = FOREACH recoRawGrp {
        recoRawOrder = ORDER recoRaw BY score DESC;
        recoRawLimit = LIMIT recoRawOrder 100;
        GENERATE FLATTEN(recoRawLimit) AS (
                        membership_nbr: chararray
                        , cardholder_nbr: chararray
                        , membership_create_date: chararray
                        , system_item_nbr: int
                        --, cat_subcat_nbr: chararray
                        , score: float
                        --, anchor_cat_subcat_nbr: chararray
        );
}

/*
recoClubJoin = JOIN reco BY (membership_nbr,cardholder_nbr,membership_create_date), cardClub BY (membership_nbr,cardholder_nbr,membership_create_date);

recoClub = FOREACH recoClubJoin GENERATE 
        reco::membership_nbr AS membership_nbr
        , reco::cardholder_nbr AS cardholder_nbr
        , reco::membership_create_date AS membership_create_date
        , reco::cat_subcat_nbr AS cat_subcat_nbr
        , reco::score AS score
        --, reco::anchor_cat_subcat_nbr AS anchor_cat_subcat_nbr
        , cardClub::preferred_club AS preferred_club
;

recoItemJoin = JOIN recoClub BY (preferred_club,cat_subcat_nbr), sc2item BY (club,subcat);

recoItem = FOREACH recoItemJoin GENERATE
        recoClub::membership_nbr AS membership_nbr
        , recoClub::cardholder_nbr AS cardholder_nbr
        , recoClub::membership_create_date AS membership_create_date
        , recoClub::cat_subcat_nbr AS cat_subcat_nbr
        , recoClub::score AS score
        --, recoClub::anchor_cat_subcat_nbr AS anchor_cat_subcat_nbr
        --, recoClub::preferred_club AS preferred_club
        , sc2item::item AS system_item_nbr
;

recoItemJoinInfo = JOIN recoItem BY system_item_nbr, itemInfo BY system_item_nbr;

recoItemAttach0 = FOREACH recoItemJoinInfo GENERATE
        recoItem::membership_nbr AS membership_nbr
        , recoItem::cardholder_nbr AS cardholder_nbr
        , recoItem::membership_create_date AS membership_create_date
        --, recoItem::cat_subcat_nbr AS cat_subcat_nbr
        , (int) SUBSTRING(recoItem::cat_subcat_nbr,0,2) AS category_nbr
        , (int) SUBSTRING(recoItem::cat_subcat_nbr,2,4) AS sub_category_nbr
        --, itemInfo::category_nbr AS category_nbr
        --, itemInfo::sub_category_nbr AS sub_category_nbr
        , recoItem::score AS score
        --, recoItem::anchor_cat_subcat_nbr AS anchor_cat_subcat_nbr
        --, recoItem::preferred_club AS preferred_club
        , recoItem::system_item_nbr AS mdsFamId
        , itemInfo::customer_item_nbr AS itemNbr
        , itemInfo::consumer_item_desc AS itemDesc
;
*/

recoItemJoin = JOIN reco BY system_item_nbr, itemInfo BY system_item_nbr;

recoItemAttach0 = FOREACH recoItemJoin GENERATE
        reco::membership_nbr AS membership_nbr
        , reco::cardholder_nbr AS cardholder_nbr
        , reco::membership_create_date AS membership_create_date
        --, reco::cat_subcat_nbr AS cat_subcat_nbr
        --, (int) SUBSTRING(reco::cat_subcat_nbr,0,2) AS category_nbr
        --, (int) SUBSTRING(reco::cat_subcat_nbr,2,4) AS sub_category_nbr
        , itemInfo::category_nbr AS category_nbr
        , itemInfo::sub_category_nbr AS sub_category_nbr
        , reco::score AS score
        --, reco::anchor_cat_subcat_nbr AS anchor_cat_subcat_nbr
        --, reco::preferred_club AS preferred_club
        , reco::system_item_nbr AS mdsFamId
        , itemInfo::customer_item_nbr AS itemNbr
        , itemInfo::consumer_item_desc AS itemDesc
;


recoItemAttach0Grp = GROUP recoItemAttach0 BY (membership_nbr, cardholder_nbr, membership_create_date, category_nbr);

recoItemAttach = FOREACH recoItemAttach0Grp {
        recoItemAttach0Order = ORDER recoItemAttach0 BY score DESC;
        recoItemAttach0Limit = LIMIT recoItemAttach0Order 3;
        GENERATE FLATTEN(recoItemAttach0Limit) AS (membership_nbr,cardholder_nbr,membership_create_date,category_nbr,sub_category_nbr,score,mdsFamId,itemNbr,itemDesc);
}

recoItemAttachGrp = GROUP recoItemAttach BY (membership_nbr, cardholder_nbr, membership_create_date);

grpRecoItemAttach = FOREACH recoItemAttachGrp GENERATE FLATTEN(recoItemAttach);

define appendDailyIndex `/usr/bin/python append_daily_index.py`
    input (stdin using PigStreaming('|'))
    output (stdout using PigStreaming('|'))
    cache('$dailyIndex#dailyIndex.txt')
    ship('$TaskDir/append_daily_index.py','$TaskDir/bag.py');


recoItemWeekly0 = STREAM recoItemAttach THROUGH appendDailyIndex AS (membership_nbr:chararray, cardholder_nbr:chararray, membership_create_date:chararray, categoryId:int, subCategoryId:int, relevanceScore:float, mdsFamId:int, itemNbr:int, itemDesc:chararray, weekRatio:tuple(Mon:float,Tue:float,Wed:float,Thu:float,Fri:float,Sat:float,Sun:float));
--weekRatio:bag{(Mon:float,Tue:float,Wed:float,Thu:float,Fri:float,Sat:float,Sun:float)}

recoItemWeekly0Join = JOIN recoItemWeekly0 BY mdsFamId LEFT, prodIdMap BY system_item_nbr Using 'replicated';

recoItemWeekly = FOREACH recoItemWeekly0Join GENERATE
        recoItemWeekly0::membership_nbr AS membership_nbr
        , recoItemWeekly0::cardholder_nbr AS cardholder_nbr
        , recoItemWeekly0::membership_create_date AS membership_create_date
        , recoItemWeekly0::categoryId AS categoryId
        , recoItemWeekly0::subCategoryId AS subCategoryId
        , recoItemWeekly0::relevanceScore AS relevanceScore
        , recoItemWeekly0::mdsFamId AS mdsFamId
        , recoItemWeekly0::itemNbr AS itemNbr
        , prodIdMap::product_id AS productId
        , recoItemWeekly0::itemDesc AS itemDesc
        , recoItemWeekly0::weekRatio AS weekRatio
;

recoGrp = GROUP recoItemWeekly BY (membership_nbr,cardholder_nbr,membership_create_date) PARALLEL 400;

recoTop = FOREACH recoGrp {
        recoOrder = ORDER recoItemWeekly BY relevanceScore DESC;
        recoLimit = Limit recoOrder 30;
        --GENERATE FLATTEN(group) AS (membership_nbr,cardholder_nbr,membership_create_date), recoLimit.(mdsFamId,itemDesc,categoryId,subCategoryId,itemNbr,relevanceScore,popupReason,weekRatio) AS recoItems;
        recoItems = FOREACH recoLimit GENERATE mdsFamId,itemDesc,categoryId,subCategoryId,itemNbr, productId, relevanceScore, 'Recommended' AS popupReason,weekRatio;
        --GENERATE group.membership_nbr AS membershipNumber, recoLimit.(mdsFamId,itemDesc,categoryId,subCategoryId,itemNbr,relevanceScore,popupReason,weekRatio) AS recoItems;
        GENERATE group.membership_nbr AS membershipNumber, group.cardholder_nbr AS cardHolder, recoItems;
}

recoMemGrp = GROUP recoTop BY membershipNumber PARALLEL 400;

recoOut = FOREACH recoMemGrp {
        cardholders = FOREACH recoTop GENERATE cardHolder, recoItems;
        GENERATE group AS membershipNumber, cardholders;
}

STORE recoOut INTO '$OUTPUT' USING JsonStorage();
/*
*/

