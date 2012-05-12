DELIMITER $$
DROP TRIGGER IF EXISTS `program_state_insert`$$
CREATE TRIGGER `program_state_insert` AFTER INSERT 
ON `patient_state`
FOR EACH ROW
BEGIN
  SET @art_start_date = (SELECT COALESCE(art_start_date,NULL) FROM patient_report WHERE patient_id = (SELECT patient_id FROM patient_program WHERE patient_program_id = new.patient_program_id LIMIT 0,1));
  SET @age_initiation = (SELECT COALESCE(age_initiation,NULL) FROM patient_report WHERE patient_id = (SELECT patient_id FROM patient_program WHERE patient_program_id = new.patient_program_id LIMIT 0,1));
  SET @registration_date = (SELECT COALESCE(registration_date,NULL) FROM patient_report WHERE patient_id = (SELECT patient_id FROM patient_program WHERE patient_program_id = new.patient_program_id LIMIT 0,1));

  SET @art_start_date = (SELECT COALESCE(art_start_date,"") FROM patient_report WHERE patient_id = (SELECT patient_id FROM patient_program WHERE patient_program_id = new.patient_program_id LIMIT 0,1));
  SET @age  = (SELECT (DATEDIFF(NOW(), birthdate)/365) FROM person p WHERE p.person_id = (SELECT patient_id FROM patient_program WHERE patient_program_id = new.patient_program_id LIMIT 0,1));

  
     SET @startdate = new.start_date;
     SET @report_id = (SELECT COALESCE(patient_report_details_id,"") FROM patient_report_details WHERE ((FLOOR((MONTH(latest_state_date) - 1)/3) = FLOOR((MONTH(@startdate) - 1)/3) AND YEAR(latest_state_date) = YEAR(@startdate)) OR (FLOOR((MONTH(last_visit_date) - 1)/3) = FLOOR((MONTH(@startdate) - 1)/3) AND YEAR(last_visit_date) = YEAR(@startdate)) OR (FLOOR((MONTH(latest_regimen_date) - 1)/3) = FLOOR((MONTH(@startdate) - 1)/3) AND YEAR(latest_regimen_date) = YEAR(@startdate)) OR (FLOOR((MONTH(tb_status_date) - 1)/3) = FLOOR((MONTH(@startdate) - 1)/3) AND YEAR(tb_status_date) = YEAR(@startdate))) AND patient_id = (SELECT patient_id FROM patient_program WHERE patient_program_id = new.patient_program_id LIMIT 0,1));


  IF new.state = (SELECT program_workflow_state_id FROM program_workflow_state WHERE concept_id = (SELECT concept_id FROM concept_name WHERE name = "Treatment stopped" LIMIT 0,1) AND program_workflow_id = (SELECT program_workflow_id FROM program_workflow WHERE program_id = (SELECT program_id FROM program WHERE concept_id = (SELECT concept_id FROM concept_name WHERE name = "HIV PROGRAM" LIMIT 0,1)))) THEN 
     UPDATE patient_report SET arv_drugs_stopped = new.start_date WHERE patient_id = (SELECT patient_id FROM patient_program WHERE patient_program_id = new.patient_program_id LIMIT 0,1);
     
	     IF  @report_id != "" THEN
		UPDATE patient_report_details SET arv_drugs_stopped = new.start_date WHERE patient_report_details_id = @report_id;
	     ELSE
	     	INSERT INTO patient_report_details (patient_id, art_start_date, age_initiation, registration_date, arv_drugs_stopped) VALUES ((SELECT patient_id FROM patient_program WHERE patient_program_id = new.patient_program_id LIMIT 0,1), @art_start_date, @age_initiation, @registration_date, new.start_date);
	     END IF;
  END IF;
   
  SET @state = (SELECT name FROM concept_name WHERE concept_id = (SELECT concept_id FROM program_workflow_state WHERE program_workflow_state_id = new.state LIMIT 0,1) LIMIT 0,1); 
  UPDATE patient_report SET latest_state = @state, latest_state_date = new.start_date WHERE patient_id = (SELECT patient_id FROM patient_program WHERE patient_program_id = new.patient_program_id LIMIT 0,1);
  
	     IF  @report_id != "" THEN
		UPDATE patient_report_details SET latest_state = @state, latest_state_date = new.start_date WHERE patient_report_details_id = @report_id;
	     ELSE
	     	INSERT INTO patient_report_details (patient_id, art_start_date, age_initiation, registration_date, latest_state, latest_state_date) VALUES ((SELECT patient_id FROM patient_program WHERE patient_program_id = new.patient_program_id LIMIT 0,1), @art_start_date, @age_initiation, @registration_date, @state, new.start_date);
	     END IF;

  IF new.state = (SELECT program_workflow_state_id FROM program_workflow_state WHERE concept_id = (SELECT concept_id FROM concept_name WHERE name = "On antiretrovirals" LIMIT 0,1) AND program_workflow_id = (SELECT program_workflow_id FROM program_workflow WHERE program_id = (SELECT program_id FROM program WHERE concept_id = (SELECT concept_id FROM concept_name WHERE name = "HIV PROGRAM" LIMIT 0,1)))) AND @art_start_date = "" THEN 

     UPDATE patient_report SET art_start_date = new.start_date, age_initiation = @age, registration_date = new.start_date WHERE patient_id = (SELECT patient_id FROM patient_program WHERE patient_program_id = new.patient_program_id LIMIT 0,1);

  END IF;      
              
END$$

DELIMITER ;
