set default_parallel 1000
SET mapred.task.timeout 1800000
SET mapred.output.compress 'true';
SET mapred.output.compression.codec 'org.apache.hadoop.io.compress.GzipCodec';
--set output.compression.enabled true;
--set output.compression.codec org.apache.hadoop.io.compress.GzipCodec;


/* input: 1) reco output: <member|card|createDate|subcat|score> (here the card can be any card or pcard)	2) card_club <member,card,member_create_date,preferred_club,assigned_club,issuing_club>	3) output from sc2item.pig <club,subcat,item>
   output: <member|card|createDate|subcat|item|score> top30 subcats for each member  (next join with itemInfo to get customer_item_nbr and itemDesc)
*/



reco = LOAD '$reco' USING PigStorage('\t') AS (
        membership_nbr: chararray
        , cardholder_nbr: chararray
        , membership_create_date: chararray
        , cat_subcat_nbr: chararray
        , score: float
        -- , anchor_cat_subcat_nbr: chararray
);



cardClub = LOAD '$cardClub' USING PigStorage('\u0001') AS (
        membership_nbr: chararray,
        cardholder_nbr: chararray,
        membership_create_date: chararray,
        preferred_club: chararray,
        assigned_club: chararray,
        issued_club: chararray
);

--prodIdMap = LOAD '$prodIdMap' USING PigStorage('\t') AS (system_item_nbr: int, product_id: chararray);

--sc2item = LOAD '$clubSubcatItem' USING PigStorage('\u0001') AS (club:chararray, subcat:chararray, item:int);

--weeklySeasonality = LOAD '$' USING PigStorage('\u0001') AS ();

--itemInfo = LOAD '$itemInfo' USING PigStorage('\u0001') AS (system_item_nbr:int,category_nbr:int,sub_category_nbr:int,customer_item_nbr:int,consumer_item_desc:chararray,product_desc:chararray);



clubAvail = LOAD '$clubAvail' Using PigStorage('\u0001') AS (item:int,club:chararray);

--4876^A4920^A23811729^A7
itemClubPopularity = LOAD '$itemClubPopularity' USING PigStorage('\u0001') AS (
    club: chararray
    , subcat: chararray
    , system_item_nbr: int
    , order: float
);

popularityJoin = JOIN clubAvail BY (item,club), itemClubPopularity BY (system_item_nbr, club);

describe popularityJoin;

/*
popularAvail = FOREACH popularityJoin GENERATE 
    itemClubPopularity::club AS club
    , itemClubPopularity::subcat AS subcat
    , itemClubPopularity::system_item_nbr AS system_item_nbr
    , itemClubPopularity::order AS order;
*/

popularAvail = FOREACH popularityJoin GENERATE itemClubPopularity::club AS club, itemClubPopularity::subcat AS subcat, itemClubPopularity::system_item_nbr AS system_item_nbr, itemClubPopularity::order ;

describe popularAvail;

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



recoItemJoin = JOIN recoClub BY (preferred_club,cat_subcat_nbr), popularAvail BY (club,subcat);

recoOut = FOREACH recoItemJoin GENERATE
        recoClub::membership_nbr AS membership_nbr
        , recoClub::cardholder_nbr AS cardholder_nbr
        , recoClub::membership_create_date AS membership_create_date
        --, recoClub::cat_subcat_nbr AS cat_subcat_nbr
        , popularAvail::system_item_nbr AS system_item_nbr
        , recoClub::score + 1e-8/popularAvail::itemClubPopularity::order AS score
        -- --, recoClub::anchor_cat_subcat_nbr AS anchor_cat_subcat_nbr
        -- --, recoClub::preferred_club AS preferred_club
;


STORE recoOut INTO '$OUTPUT' USING PigStorage('\u0001');
