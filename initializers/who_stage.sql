DELIMITER $$
DROP FUNCTION IF EXISTS who_stage$$
CREATE FUNCTION who_stage(pat_id int, date_asked varchar(10)) RETURNS INT 
DETERMINISTIC
BEGIN
DECLARE stage VARCHAR(255) ;
DECLARE pat_age INT;                                                        
DECLARE category VARCHAR(2);                                                               

DECLARE birthdate VARCHAR(10);                                                               
DECLARE birthdate_estimated INT;                                                               
DECLARE date_created VARCHAR(10);
DECLARE adult_or_ped VARCHAR(5); 
DECLARE calculated_stage INT;  
DECLARE stage_number INT;                                                             
DECLARE stage_count INT;
DECLARE staging_observations VARCHAR(255);
DECLARE concept_pos INT;
DECLARE current_concept INT;
DECLARE yes_concept INT;

SET birthdate = (SELECT LEFT(p.birthdate,10) FROM patient p WHERE p.patient_id = pat_id);
SET birthdate_estimated = (SELECT p.birthdate_estimated FROM patient p WHERE p.patient_id = pat_id);
SET date_created = (SELECT LEFT(p.date_created,10) FROM patient p WHERE p.patient_id = pat_id);

SET pat_age = (SELECT age(birthdate,date_asked,date_created,birthdate_estimated));

SET adult_or_ped = (SELECT CASE WHEN pat_age <= 14 THEN "peds" ELSE "adult" END);
SET calculated_stage = 1;

SET yes_concept = (SELECT concept_id FROM concept WHERE name = "YES" LIMIT 1);

SET stage_number = 4;

simple_loop: LOOP
    
    IF calculated_stage > 1 THEN
        LEAVE simple_loop;
    END IF;

    IF COALESCE((SELECT COUNT(obs_id) FROM obs WHERE patient_id = pat_id AND encounter_id = 
        (SELECT encounter_id FROM encounter WHERE patient_id = pat_id AND voided = 0 AND 
            encounter_type = (SELECT encounter_type_id FROM encounter_type WHERE name = "HIV Staging") LIMIT 1) 
            AND value_coded = (SELECT concept_id FROM concept WHERE name = "YES" LIMIT 1) AND 
            concept_id IN (SELECT concept_id FROM concept_set WHERE concept_set = 
                (SELECT concept_id FROM concept WHERE name = CONCAT("WHO stage ", stage_number, " ", adult_or_ped) LIMIT 1))),0) > 0 THEN

        SET calculated_stage = stage_number;
        LEAVE simple_loop;

    END IF;

    SET stage_number=stage_number-1; 
    IF stage_number<2 THEN
        LEAVE simple_loop;
    END IF;
END LOOP simple_loop;

SET stage = calculated_stage;

RETURN stage;
END$$

DELIMITER ;