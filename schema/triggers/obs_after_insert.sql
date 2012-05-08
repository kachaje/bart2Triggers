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
		  UPDATE patient_report SET reason_for_art_eligibility = (SELECT name FROM concept_name WHERE concept_id = new.value_coded LIMIT 0,1) WHERE patient_id = new.person_id; 
  END IF;  

  /* tb */                  
  IF (new.concept_id = (SELECT concept_id FROM concept_name WHERE name = "Pulmonary TB (current)") AND new.value_coded IN (SELECT concept_id FROM concept_name WHERE name = "YES")) OR (new.value_coded IN (SELECT concept_id FROM concept_name WHERE name = "Pulmonary TB (current)")) THEN		  	  
		  UPDATE patient_report SET current_episode_of_tb = new.obs_datetime WHERE patient_id = new.person_id;
  END IF;    
              
  IF (new.concept_id = (SELECT concept_id FROM concept_name WHERE name = "Pulmonary tuberculosis within the last 2 years") AND new.value_coded IN (SELECT concept_id FROM concept_name WHERE name = "YES")) OR (new.value_coded IN (SELECT concept_id FROM concept_name WHERE name = "Pulmonary tuberculosis within the last 2 years")) THEN		  	  
		  UPDATE patient_report SET tb_within_the_last_2_years = new.obs_datetime WHERE patient_id = new.person_id;
  END IF; 

  /* KS */
  IF (new.concept_id = (SELECT concept_id FROM concept_name WHERE name = "Karposi's sarcoma") AND new.value_coded IN (SELECT concept_id FROM concept_name WHERE name = "YES")) OR (new.value_coded IN (SELECT concept_id FROM concept_name WHERE name = "Karposi's sarcoma")) THEN		  	  
		  UPDATE patient_report SET karposis_sarcoma = new.obs_datetime WHERE patient_id = new.person_id;
  END IF; 

  /* ART REGIMENS */
  IF (new.concept_id = (SELECT concept_id FROM concept_name WHERE name = "Regimen Category" LIMIT 0,1) OR new.concept_id = (SELECT concept_id FROM concept_name WHERE name = "ARV regimens received abstracted construct")) AND (SELECT name FROM encounter_type WHERE encounter_type_id = (SELECT encounter_type FROM encounter WHERE encounter_id = new.encounter_id)) = "DISPENSING" THEN
	SET @regimen = (SELECT CASE WHEN COALESCE(new.value_text, "") <> "" THEN new.value_text ELSE "OTHER" END);
	UPDATE patient_report SET latest_regimen = @regimen, latest_regimen_date = new.obs_datetime WHERE patient_id = new.person_id;
  END IF;

  /* SIDE EFFECTS */
  IF (new.concept_id = (SELECT concept_id FROM concept_name WHERE name = "Drug induced" LIMIT 0,1)) AND (SELECT name FROM encounter_type WHERE encounter_type_id = (SELECT encounter_type FROM encounter WHERE encounter_id = new.encounter_id)) = "HIV CLINIC CONSULTATION" THEN
	UPDATE patient_report SET latest_side_effects = new.obs_datetime WHERE patient_id = new.person_id;
  END IF;

  /* LAST VISIT DATE*/
  UPDATE patient_report SET last_visit_date = new.obs_datetime WHERE patient_id = new.person_id;

  /* MISSED DOSES */
  IF (new.concept_id = (SELECT concept_id FROM concept_name WHERE name = "Missed antiretroviral drug construct" LIMIT 0,1)) AND (SELECT name FROM encounter_type WHERE encounter_type_id = (SELECT encounter_type FROM encounter WHERE encounter_id = new.encounter_id)) = "ART ADHERENCE" THEN
	UPDATE patient_report SET missed_drugs_count = new.value_numeric, last_missed_drugs_date = new.obs_datetime WHERE patient_id = new.person_id;
  END IF;

  /* TB STATUS */
  IF (new.concept_id = (SELECT concept_id FROM concept_name WHERE name = "TB status" LIMIT 0,1)) AND (SELECT name FROM encounter_type WHERE encounter_type_id = (SELECT encounter_type FROM encounter WHERE encounter_id = new.encounter_id)) = "HIV CLINIC CONSULTATION" THEN
        SET @status = (SELECT name FROM concept_name WHERE concept_id = new.value_coded LIMIT 0,1);
	UPDATE patient_report SET tb_status = @status, tb_status_date = new.obs_datetime WHERE patient_id = new.person_id;
  END IF;

END$$

DELIMITER ;
