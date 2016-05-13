set default_parallel 1000
set mapred.task.timeout 1800000
SET mapred.output.compress 'true';
SET mapred.output.compression.codec 'org.apache.hadoop.io.compress.GzipCodec';


/* input: 1) reco output: <member|card|createDate|subcat|score> (here the card can be any card or pcard)	2) active member <member \x1 card \x1 member_create_date>
   output: <member \x1 createDate \x1 subcat \x1 score> 
*/



reco = LOAD '$reco' USING PigStorage('\t') AS (
        membership_nbr: chararray
        , cardholder_nbr: chararray
        , membership_create_date: chararray
        , cat_subcat_nbr: chararray
        , score: float
        -- , anchor_cat_subcat_nbr: chararray
);



activeMember = LOAD '$activeMember' USING PigStorage('\u0001') AS (
        membership_nbr: chararray,
        cardholder_nbr: chararray,
        membership_create_date: chararray
);

activeMember = FOREACH activeMember GENERATE membership_nbr, membership_create_date;

activeMemberUniq = DISTINCT activeMember; 

recoActiveJoin = JOIN reco BY (membership_nbr, membership_create_date), activeMemberUniq BY (membership_nbr, membership_create_date);

wmx_output = FOREACH recoActiveJoin GENERATE
        reco::membership_nbr AS membership_nbr
        --, reco::cardholder_nbr AS cardholder_nbr
        , reco::membership_create_date AS membership_create_date
        , reco::cat_subcat_nbr AS cat_subcat_nbr
        , reco::score AS score
;


STORE wmx_output INTO '$recoWMX' USING PigStorage('\u0001');

