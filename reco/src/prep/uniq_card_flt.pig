set default_parallel 1000

/* input: output from member_pcard_fetch.hql
   output: triple key
*/

-- 34^A10^A4870^A4969^A6238^A2014-05-04^A2099-12-31

memberCards = LOAD '$memberCardsLoc' Using PigStorage('\u0001') AS (
                membership_nbr: chararray
                , cardholder_nbr: chararray
                , membership_create_date: chararray
                , preferred_club_nbr: chararray
                , issuing_club_nbr: chararray
                , assigned_club_nbr: chararray
                , snapshot_begin_date: chararray
                , snapshot_end_date: chararray
                , member_status_code: chararray
);

memberGrp = GROUP memberCards BY membership_nbr;

memberTop1 = FOREACH memberGrp {
                memberOrder = ORDER memberCards BY snapshot_begin_date DESC;
                memberTop = LIMIT memberOrder 1;
                --GENERATE group AS membership_nbr, FLATTEN(memberTop.(cardholder_nbr, membership_create_date));
                GENERATE group AS membership_nbr, FLATTEN(memberTop.(cardholder_nbr, membership_create_date,preferred_club_nbr,assigned_club_nbr,issuing_club_nbr));
}

STORE memberTop1 INTO '$memberCardUniq' Using PigStorage('\u0001');

/* get the active members for the wmx team  */
 
memberActive = FOREACH memberGrp {  
                memberCardsFlt = FILTER memberCards BY (member_status_code == 'A' OR member_status_code == 'E' OR member_status_code == 'P'); 
                memberOrder = ORDER memberCardsFlt BY snapshot_begin_date DESC;  
                memberTop = LIMIT memberOrder 1;  
                GENERATE group AS membership_nbr, FLATTEN(memberTop.(cardholder_nbr, membership_create_date));  
                --GENERATE FLATTEN(group) AS (membership_nbr,cardholder_nbr), FLATTEN(memberTop.(membership_create_date,preferred_club_nbr,assigned_club_nbr,issuing_club_nbr));  
}  
 
STORE memberActive INTO '$memberCardUniqActive' Using PigStorage('\u0001'); 

