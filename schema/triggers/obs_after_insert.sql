/* The target here is to INSERT a date only when a patient is pregnant */
DELIMITER $$
DROP TRIGGER IF EXISTS `obs_after_insert`$$
CREATE TRIGGER `obs_after_insert` AFTER INSERT 
ON `obs`
FOR EACH ROW
BEGIN
  /* patient_pregnant */
  SET @art_start_date = (SELECT art_start_date FROM patient_report WHERE patient_id = new.person_id);
  IF NOT ISNULL(@art_start_date) THEN
	  IF new.concept_id = (SELECT concept_id FROM concept_name WHERE name = "IS PATIENT PREGNANT?") AND new.value_coded IN (SELECT concept_id FROM concept_name WHERE name = "YES") THEN		  	  
		  UPDATE patient_report SET patient_pregnant_date = new.obs_datetime WHERE patient_id = new.person_id;
	  END IF;   
  END IF; 

  /* reason_for_art_eligibility */
  IF new.concept_id = (SELECT concept_id FROM concept_name WHERE name = "Reason for ART eligibility") THEN		  	  
		  UPDATE patient_report SET reason_for_art_eligibility = (SELECT CASE COALESCE(new.value_coded_name_id,'') WHEN '' THEN (SELECT name FROM concept_name WHERE concept_id = new.value_coded LIMIT 0,1) ELSE (SELECT name FROM concept_name WHERE concept_name_id = new.value_coded_name_id LIMIT 0,1) END) WHERE patient_id = new.person_id; 
  END IF;  

  /* tb */                  
  IF (new.concept_id = (SELECT concept_id FROM concept_name WHERE name = "Pulmonary TB (current)") AND new.value_coded IN (SELECT concept_id FROM concept_name WHERE name = "YES")) OR (new.value_coded IN (SELECT concept_id FROM concept_name WHERE name = "Pulmonary TB (current)")) THEN		  	  
	UPDATE patient_report SET current_episode_of_tb = new.obs_datetime WHERE patient_id = new.person_id;
	INSERT INTO patient_report_details (patient_id, current_episode_of_tb) VALUES (new.person_id, new.obs_datetime);
  END IF;    
              
  IF (new.concept_id = (SELECT concept_id FROM concept_name WHERE name = "Pulmonary tuberculosis within the last 2 years") AND new.value_coded IN (SELECT concept_id FROM concept_name WHERE name = "YES")) OR (new.value_coded IN (SELECT concept_id FROM concept_name WHERE name = "Pulmonary tuberculosis within the last 2 years")) THEN		  	  
	UPDATE patient_report SET tb_within_the_last_2_years = new.obs_datetime WHERE patient_id = new.person_id;
	INSERT INTO patient_report_details (patient_id, tb_within_the_last_2_years) VALUES (new.person_id, new.obs_datetime);
  END IF; 

  /* KS */
  IF (new.concept_id = (SELECT concept_id FROM concept_name WHERE name = "Karposi's sarcoma") AND new.value_coded IN (SELECT concept_id FROM concept_name WHERE name = "YES")) OR (new.value_coded IN (SELECT concept_id FROM concept_name WHERE name = "Karposi's sarcoma")) THEN		  	  
	UPDATE patient_report SET karposis_sarcoma = new.obs_datetime WHERE patient_id = new.person_id;
	INSERT INTO patient_report_details (patient_id, karposis_sarcoma) VALUES (new.person_id, new.obs_datetime);
  END IF; 

  /* ART REGIMENS */  
  IF (new.concept_id = (SELECT concept_id FROM concept_name WHERE name = "Regimen Category" LIMIT 0,1) AND (SELECT name FROM encounter_type WHERE encounter_type_id = (SELECT encounter_type FROM encounter WHERE encounter_id = new.encounter_id)) = "DISPENSING") THEN
	UPDATE patient_report SET latest_regimen = new.value_text, latest_regimen_date = new.obs_datetime WHERE patient_id = new.person_id;
	INSERT INTO patient_report_details (patient_id, latest_regimen, latest_regimen_date) VALUES (new.person_id, new.value_text, new.obs_datetime);
  END IF;

  /* SIDE EFFECTS */
  IF (new.concept_id = (SELECT concept_id FROM concept_name WHERE name = "Drug induced" LIMIT 0,1)) AND (SELECT name FROM encounter_type WHERE encounter_type_id = (SELECT encounter_type FROM encounter WHERE encounter_id = new.encounter_id)) = "HIV CLINIC CONSULTATION" THEN
	UPDATE patient_report SET latest_side_effects = new.obs_datetime WHERE patient_id = new.person_id;
	INSERT INTO patient_report_details (patient_id, latest_side_effects) VALUES (new.person_id, new.obs_datetime);
  END IF;

  /* LAST VISIT DATE*/
  UPDATE patient_report SET last_visit_date = new.obs_datetime WHERE patient_id = new.person_id;
  /* INSERT INTO patient_report_details (patient_id, last_visit_date) VALUES (new.person_id, new.obs_datetime); */

  /* MISSED DOSES */
  IF (new.concept_id = (SELECT concept_id FROM concept_name WHERE name = "Missed antiretroviral drug construct" LIMIT 0,1)) AND (SELECT name FROM encounter_type WHERE encounter_type_id = (SELECT encounter_type FROM encounter WHERE encounter_id = new.encounter_id)) = "ART ADHERENCE" THEN
	UPDATE patient_report SET missed_drugs_count = new.value_numeric, last_missed_drugs_date = new.obs_datetime WHERE patient_id = new.person_id;
	INSERT INTO patient_report_details (patient_id, missed_drugs_count, last_missed_drugs_date) VALUES (new.person_id, new.value_numeric, new.obs_datetime);
  END IF;

  /* TB STATUS */
  IF (new.concept_id = (SELECT concept_id FROM concept_name WHERE name = "TB status" LIMIT 0,1)) AND (SELECT name FROM encounter_type WHERE encounter_type_id = (SELECT encounter_type FROM encounter WHERE encounter_id = new.encounter_id)) = "HIV CLINIC CONSULTATION" THEN
        SET @status = (SELECT name FROM concept_name WHERE concept_id = new.value_coded LIMIT 0,1);
	UPDATE patient_report SET tb_status = @status, tb_status_date = new.obs_datetime WHERE patient_id = new.person_id;
        INSERT INTO patient_report_details (patient_id, tb_status, tb_status_date) VALUES (new.person_id, @status, new.obs_datetime);
  END IF;

  /* Re-initiation */
  IF (new.concept_id = (SELECT concept_id FROM concept_name WHERE name = "Has the patient taken ART in the last two months" LIMIT 0,1)) AND (SELECT name FROM encounter_type WHERE encounter_type_id = (SELECT encounter_type FROM encounter WHERE encounter_id = new.encounter_id)) = "HIV CLINIC REGISTRATION" AND (new.value_coded IN (SELECT concept_id FROM concept_name WHERE name = "NO") OR new.value_text = "NO") THEN
	UPDATE patient_report SET patient_did_not_take_arvs_in_last_two_months = new.obs_datetime WHERE patient_id = new.person_id;
  	INSERT INTO patient_report_details (patient_id, patient_did_not_take_arvs_in_last_two_months) VALUES (new.person_id, new.obs_datetime);
  END IF;

END$$

DELIMITER ;
