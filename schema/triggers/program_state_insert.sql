DELIMITER $$
DROP TRIGGER IF EXISTS `program_state_insert`$$
CREATE TRIGGER `program_state_insert` AFTER INSERT 
ON `patient_state`
FOR EACH ROW
BEGIN
  IF new.state = (SELECT program_workflow_state_id FROM program_workflow_state WHERE concept_id = (SELECT concept_id FROM concept_name WHERE name = "Treatment stopped" LIMIT 0,1) AND program_workflow_id = (SELECT program_workflow_id FROM program_workflow WHERE program_id = (SELECT program_id FROM program WHERE concept_id = (SELECT concept_id FROM concept_name WHERE name = "HIV PROGRAM" LIMIT 0,1)))) THEN 
     UPDATE patient_report SET arv_drugs_stopped = new.start_date WHERE patient_id = (SELECT patient_id FROM patient_program WHERE patient_program_id = new.patient_program_id LIMIT 0,1);
  END IF;
   
  SET @state = (SELECT name FROM concept_name WHERE concept_id = (SELECT concept_id FROM program_workflow_state WHERE program_workflow_state_id = new.state LIMIT 0,1) LIMIT 0,1); 
  UPDATE patient_report SET latest_state = @state, latest_state_date = new.start_date WHERE patient_id = (SELECT patient_id FROM patient_program WHERE patient_program_id = new.patient_program_id LIMIT 0,1);
                    
END$$

DELIMITER ;
