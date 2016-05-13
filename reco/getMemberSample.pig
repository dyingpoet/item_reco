set default_parallel 1000

score = LOAD '$ScoreInput' USING JsonLoader();

describe score;
/*
--scoreSample = FILTER score BY membershipNumber in ('1130848','764075008','180711'); --pig 0.12
*/

--scoreSample = FILTER score BY (membershipNumber == '1130848') OR (membershipNumber == '764075008') OR (membershipNumber=='180711'); 
--scoreSample = FILTER score BY (membershipNumber == '300330347') OR (membershipNumber == '654991991') ;
scoreSample = FILTER score BY 
    (membershipNumber == '960111979')
    OR (membershipNumber == '960111987')
    OR (membershipNumber == '960111995')
    OR (membershipNumber == '960112001')
    OR (membershipNumber == '960112019')
    OR (membershipNumber == '960112027')
    OR (membershipNumber == '960112035')
    OR (membershipNumber == '960112043')
    OR (membershipNumber == '960112050')
    OR (membershipNumber == '960112068')
    OR (membershipNumber == '960112076')
    OR (membershipNumber == '960112084')
    OR (membershipNumber == '960112092')
    OR (membershipNumber == '960112100')
    OR (membershipNumber == '960112118')
    OR (membershipNumber == '960112126')
    OR (membershipNumber == '960112134')
    OR (membershipNumber == '960112142')
    OR (membershipNumber == '960112159')
    OR (membershipNumber == '960112167')
;

STORE scoreSample INTO '$ScoreSample' USING JsonStorage();

 
