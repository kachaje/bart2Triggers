DELIMITER $$
DROP TRIGGER IF EXISTS `person_after_insert`$$
CREATE TRIGGER `person_after_insert` AFTER INSERT 
ON `person`
FOR EACH ROW
BEGIN
  IF new.dead = 1 THEN 
     SET @art_start_date = (SELECT COALESCE(art_start_date,NULL) FROM patient_report WHERE patient_id = new.person_id);
     SET @age_initiation = (SELECT COALESCE(age_initiation,NULL) FROM patient_report WHERE patient_id = new.person_id);
     SET @registration_date = (SELECT COALESCE(registration_date,NULL) FROM patient_report WHERE patient_id = new.person_id);

     SET @startdate = new.date_created;
     SET @report_id = (SELECT COALESCE(patient_report_details_id,"") FROM patient_report_details WHERE ((FLOOR((MONTH(latest_state_date) - 1)/3) = FLOOR((MONTH(@startdate) - 1)/3) AND YEAR(latest_state_date) = YEAR(@startdate)) OR (FLOOR((MONTH(last_visit_date) - 1)/3) = FLOOR((MONTH(@startdate) - 1)/3) AND YEAR(last_visit_date) = YEAR(@startdate)) OR (FLOOR((MONTH(latest_regimen_date) - 1)/3) = FLOOR((MONTH(@startdate) - 1)/3) AND YEAR(latest_regimen_date) = YEAR(@startdate)) OR (FLOOR((MONTH(tb_status_date) - 1)/3) = FLOOR((MONTH(@startdate) - 1)/3) AND YEAR(tb_status_date) = YEAR(@startdate))) AND patient_id = new.person_id);

     UPDATE patient_report SET death_date = new.death_date WHERE patient_id = new.person_id;
     
	     IF  @report_id != "" THEN
		UPDATE patient_report_details SET death_date = new.death_date WHERE patient_report_details_id = @report_id;
	     ELSE
	     	INSERT INTO patient_report_details (patient_id, art_start_date, age_initiation, registration_date, death_date) VALUES (new.person_id, @art_start_date, @age_initiation, @registration_date, new.death_date);
             END IF;
  END IF;
                        
END$$

DELIMITER ;
