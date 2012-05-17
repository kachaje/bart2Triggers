DELIMITER $$
DROP FUNCTION IF EXISTS reason_for_art_eligibility$$
CREATE FUNCTION reason_for_art_eligibility(pat_id int) RETURNS INT 
DETERMINISTIC
BEGIN
DECLARE reason INT;
DECLARE low_cd4_count_250 INT;
DECLARE low_cd4_count_350 INT;
DECLARE sex VARCHAR(6);
DECLARE pregnant_woman BIT;
DECLARE breastfeeding_woman BIT;
DECLARE first_hiv_enc_date VARCHAR(10);
DECLARE yes_concept INT;
DECLARE child_at_initiation BIT;
DECLARE child BIT;
DECLARE pat_age_at_init INT;
DECLARE pat_age INT;         
DECLARE date_of_positive_hiv_test VARCHAR(10);
DECLARE date_of_positive_hiv_test_was_entered INT;
DECLARE latest_staging_date VARCHAR(10);
DECLARE age_in_months INT;
DECLARE cd4_count_less_than_750 INT;
DECLARE presumed_hiv_status_conditions BIT;
DECLARE low_cd4_percent INT;

DECLARE adult_or_ped VARCHAR(5); 
DECLARE low_lymphocyte_count INT;
DECLARE first_hiv_test_was_pcr INT;
DECLARE first_hiv_test_was_rapid INT;
DECLARE pneumocystis_pneumonia INT;
DECLARE candidiasis_of_oesophagus INT;
DECLARE cryptococcal_meningitis INT;
DECLARE severe_unexplained_wasting INT;
DECLARE toxoplasmosis_of_the_brain INT;
DECLARE oral_thrush INT;
DECLARE sepsis_severe INT;
DECLARE pneumonia_severe INT;
DECLARE hiv_staging INT;
DECLARE whostage INT;
DECLARE birthdate VARCHAR(10);                                                               
DECLARE birthdate_estimated INT;                                                                 
DECLARE date_created VARCHAR(10); 
DECLARE checkrange INT;

SET low_cd4_count_250 = COALESCE((SELECT COUNT(*) FROM obs WHERE patient_id = pat_id AND value_numeric <= 250 AND 
        concept_id = (SELECT concept_id FROM concept WHERE name = "CD4 count" limit 1) AND (concept_id = 
        (SELECT concept_id FROM concept WHERE name = "CD4 < 250") OR value_coded = (yes_concept)) AND voided = 0),0);

SET low_cd4_count_350 = COALESCE((SELECT COUNT(*) FROM obs WHERE patient_id = pat_id AND value_numeric <= 350 AND 
        concept_id = (SELECT concept_id FROM concept WHERE name = "CD4 count" limit 1) AND (concept_id = 
        (SELECT concept_id FROM concept WHERE name = "CD4 < 350") OR value_coded = (yes_concept)) AND voided = 0),0);

SET pregnant_woman = 0;
SET breastfeeding_woman = 0;

SET first_hiv_enc_date = COALESCE((SELECT DATE(encounter_datetime) FROM encounter WHERE encounter_type = 
    (SELECT encounter_type_id FROM encounter_type WHERE name = "HIV Staging" LIMIT 1) 
        ORDER BY encounter_datetime ASC LIMIT 1),"2010-01-01");

SET whostage = who_stage(pat_id, first_hiv_enc_date);

SET sex = (SELECT gender FROM patient WHERE patient_id = pat_id LIMIT 1);
SET yes_concept = (SELECT concept_id FROM concept WHERE name = "Yes" LIMIT 1);

SET birthdate = (SELECT LEFT(p.birthdate,10) FROM patient p WHERE p.patient_id = pat_id);
SET birthdate_estimated = (SELECT p.birthdate_estimated FROM patient p WHERE p.patient_id = pat_id);
SET date_created = (SELECT LEFT(p.date_created,10) FROM patient p WHERE p.patient_id = pat_id);

SET pat_age_at_init = (SELECT age(birthdate,first_hiv_enc_date,date_created,birthdate_estimated));

SET pat_age = (SELECT age(birthdate,first_hiv_enc_date,date_created,birthdate_estimated));

SET child_at_initiation = (SELECT CASE WHEN pat_age_at_init <= 14 THEN 1 ELSE 0 END);

SET child = (SELECT CASE WHEN pat_age <= 14 THEN 1 ELSE 0 END);
SET adult_or_ped = (SELECT CASE WHEN pat_age <= 14 THEN "peds" ELSE "adult" END);

SET date_of_positive_hiv_test_was_entered = COALESCE((SELECT DATE(value_datetime) FROM obs WHERE concept_id = (SELECT concept_id FROM concept 
        WHERE name = "Date of positive HIV test" LIMIT 1) ORDER BY obs_datetime DESC LIMIT 1),"");

SET latest_staging_date = (SELECT DATE(encounter_datetime) FROM encounter WHERE encounter_type = 
    (SELECT encounter_type_id FROM encounter_type WHERE name = "HIV Staging" LIMIT 1) ORDER BY encounter_datetime DESC LIMIT 1);

IF date_of_positive_hiv_test_was_entered != "" THEN
    SET date_of_positive_hiv_test = date_of_positive_hiv_test_was_entered;
ELSE 
    SET date_of_positive_hiv_test = "";
END IF;

IF sex = "Female" THEN

    IF first_hiv_enc_date >= "2010-01-01" THEN
        IF COALESCE((SELECT COUNT(*) FROM obs WHERE concept_id = (SELECT concept_id FROM 
            concept WHERE name = "Pregnant" LIMIT 1) AND value_coded = (yes_concept) AND voided = 0) ,0) > 0 THEN
            SET pregnant_woman = 1;
        END IF;
        IF COALESCE((SELECT COUNT(*) FROM obs WHERE concept_id = (SELECT concept_id FROM 
            concept WHERE name = "Breastfeeding" LIMIT 1) AND value_coded = (yes_concept) AND voided = 0) ,0) > 0 THEN
            SET breastfeeding_woman = 1;
        END IF;
    END IF;

END IF;

IF child_at_initiation != 0 OR child != 0 THEN
    SET age_in_months = (SELECT age(birthdate,latest_staging_date,date_created,birthdate_estimated)*12);
    SET cd4_count_less_than_750 = 0;
     
    IF age_in_months >= 24 AND age_in_months < 56 THEN
        SET cd4_count_less_than_750 = (SELECT obs_id FROM obs WHERE (value_numeric <= 750 AND concept_id = 
            (SELECT concept_id FROM concept WHERE name = "CD4 count" LIMIT 1)) OR (concept_id = (SELECT concept_id FROM 
                concept WHERE name = "CD4 Count < 750" LIMIT 1) AND value_coded = yes_concept) AND voided = 0 LIMIT 1);
    END IF;

    SET low_cd4_percent = COALESCE((SELECT COUNT(obs_id) FROM obs WHERE concept_id = (SELECT concept_id FROM concept WHERE name = 
        "CD4 percentage < 25" LIMIT 1) AND value_coded = yes_concept AND voided = 0 LIMIT 1), 0);

    SET checkrange = (SELECT (CASE pat_age WHEN 0 OR 1 OR 2 THEN 4000
                         WHEN 3 OR 4 THEN 3000
                         WHEN 5 THEN 2500
                         WHEN pat_age >= 6 AND pat_age <=15 THEN 2000 END));

    SET low_lymphocyte_count = COALESCE((SELECT COUNT(obs_id) FROM obs WHERE value_numeric <= checkrange
            AND concept_id = (SELECT concept_id FROM concept WHERE 
                    name = "Lymphocyte count") AND voided = 0 LIMIT 1), 0);

    SET first_hiv_test_was_pcr = COALESCE((SELECT COUNT(obs_id) FROM obs WHERE concept_id = (SELECT concept_id FROM concept WHERE name = 
        "First positive HIV Test") AND value_coded = (SELECT concept_id FROM concept WHERE name = "PCR Test") AND voided = 0 LIMIT 1), 0);

    SET first_hiv_test_was_rapid = COALESCE((SELECT COUNT(obs_id) FROM obs WHERE concept_id = (SELECT concept_id FROM concept WHERE name = 
        "First positive HIV Test") AND value_coded = (SELECT concept_id FROM concept WHERE name = "Rapid Test") AND voided = 0 LIMIT 1), 0);

    SET pneumocystis_pneumonia = COALESCE((SELECT COUNT(obs_id) FROM obs WHERE concept_id = (SELECT concept_id FROM concept WHERE name = 
        "Pneumocystis pneumonia") AND value_coded = yes_concept AND voided = 0 LIMIT 1), 0);

    SET candidiasis_of_oesophagus = COALESCE((SELECT COUNT(obs_id) FROM obs WHERE concept_id = (SELECT concept_id FROM concept WHERE name = 
        "Candidiasis of oesophagus") AND value_coded = yes_concept AND voided = 0 LIMIT 1), 0);

    SET cryptococcal_meningitis = COALESCE((SELECT COUNT(obs_id) FROM obs WHERE concept_id = (SELECT concept_id FROM concept WHERE name = 
        "Cryptococcal meningitis") AND value_coded = yes_concept AND voided = 0 LIMIT 1), 0);

    SET severe_unexplained_wasting = COALESCE((SELECT COUNT(obs_id) FROM obs WHERE concept_id = (SELECT concept_id FROM concept WHERE name = 
        "Severe unexplained wasting / malnutrition not responding to treatment(weight-for-height/ -age less than 70% or MUAC less than 11cm or oedema)") AND value_coded = yes_concept AND voided = 0 LIMIT 1), 0);

    SET toxoplasmosis_of_the_brain = COALESCE((SELECT COUNT(obs_id) FROM obs WHERE concept_id = (SELECT concept_id FROM concept WHERE name = 
        "Toxoplasmosis of the brain (from age 1 month)") AND value_coded = yes_concept AND voided = 0 LIMIT 1), 0);

    SET oral_thrush = COALESCE((SELECT COUNT(obs_id) FROM obs WHERE concept_id = (SELECT concept_id FROM concept WHERE name = 
        "Oral thrush") AND value_coded = yes_concept AND voided = 0 LIMIT 1), 0);

    SET sepsis_severe = COALESCE((SELECT COUNT(obs_id) FROM obs WHERE concept_id = (SELECT concept_id FROM concept WHERE name = 
        "Sepsis, severe") AND value_coded = yes_concept AND voided = 0 LIMIT 1), 0);

    SET pneumonia_severe = COALESCE((SELECT COUNT(obs_id) FROM obs WHERE concept_id = (SELECT concept_id FROM concept WHERE name = 
        "Pneumonia, severe") AND value_coded = yes_concept AND voided = 0 LIMIT 1), 0);

    SET hiv_staging = COALESCE((SELECT COUNT(*) FROM encounter WHERE encounter_type = (SELECT encounter_type_id 
        FROM encounter_type WHERE name = "HIV Staging") AND patient_id = pat_id LIMIT 1),0);

    IF pneumocystis_pneumonia != 0 OR candidiasis_of_oesophagus != 0 OR cryptococcal_meningitis != 0 OR severe_unexplained_wasting != 0 OR 
        toxoplasmosis_of_the_brain != 0 OR (oral_thrush != 0 AND sepsis_severe != 0) OR (oral_thrush != 0 AND pneumonia_severe != 0) OR 
        (sepsis_severe != 0 AND pneumonia_severe != 0) THEN

        SET presumed_hiv_status_conditions = 1;
    
    END IF;

    IF age_in_months <= 17 AND first_hiv_test_was_rapid != 0 AND presumed_hiv_status_conditions != 0 THEN
        SET reason = (SELECT concept_id FROM concept WHERE name = "Presumed HIV Disease" LIMIT 1);
    ELSEIF age_in_months <= 12 AND first_hiv_test_was_pcr != 0 AND hiv_staging > 0 THEN
        SET reason = (SELECT concept_id FROM concept WHERE name = "PCR Test" LIMIT 1);
    ELSEIF whostage >= 3 THEN
        SET reason = (SELECT concept_id FROM concept WHERE name = CONCAT("WHO stage ", whostage, " ", adult_or_ped) LIMIT 1);
    ELSEIF age_in_months >= 12 and age_in_months < 24 THEN
        SET reason = (SELECT concept_id FROM concept WHERE name = "Child HIV positive" LIMIT 1);
    ELSEIF (age_in_months >= 24 AND age_in_months < 56) AND cd4_count_less_than_750 THEN
        SET reason = (SELECT concept_id FROM concept WHERE name = "CD4 count < 750" LIMIT 1);
    ELSEIF low_cd4_count_350 != 0 and first_hiv_enc_date >= '2011-07-01' THEN
        SET reason = (SELECT concept_id FROM concept WHERE name = "CD4 count < 350" LIMIT 1);
    ELSEIF low_cd4_count_250 != 0 THEN
        SET reason = (SELECT concept_id FROM concept WHERE name = "CD4 count < 250" LIMIT 1);
    ELSEIF low_lymphocyte_count != 0 and whostage = 2 THEN
        SET reason = (SELECT concept_id FROM concept WHERE name = "Lymphocyte count below threshold with WHO stage 2" LIMIT 1);
    ELSEIF pregnant_woman != 0 THEN
        SET reason = (SELECT concept_id FROM concept WHERE name = "Pregnant" LIMIT 1);
    ELSEIF breastfeeding_woman != 0 THEN
        SET reason = (SELECT concept_id FROM concept WHERE name = "Breastfeeding" LIMIT 1);
    END IF;

ELSE

    IF whostage >= 3 THEN
        SET reason = (SELECT concept_id FROM concept WHERE name = CONCAT("WHO stage ", whostage, " ", adult_or_ped) LIMIT 1);
    ELSEIF low_cd4_count_350 != 0 AND first_hiv_enc_date >= '2011-07-01' THEN
        SET reason = (SELECT concept_id FROM concept WHERE name = "CD4 count < 350" LIMIT 1);
    ELSEIF low_cd4_count_250 != 0 THEN
        SET reason = (SELECT concept_id FROM concept WHERE name = "CD4 count < 250" LIMIT 1);
    ELSEIF low_lymphocyte_count != 0 and whostage = 2 THEN
        SET reason = (SELECT concept_id FROM concept WHERE name = "Lymphocyte count below threshold with WHO stage 2" LIMIT 1);
    ELSEIF pregnant_woman != 0 THEN
        SET reason = (SELECT concept_id FROM concept WHERE name = "Pregnant" LIMIT 1);
    ELSEIF breastfeeding_woman != 0 THEN
            SET reason = (SELECT concept_id FROM concept WHERE name = "Breastfeeding" LIMIT 1);    
    END IF; 
 
END IF;

RETURN reason;
END$$

DELIMITER ;