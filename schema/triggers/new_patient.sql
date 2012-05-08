DELIMITER $$
DROP TRIGGER IF EXISTS `new_patient`$$
CREATE TRIGGER `new_patient` AFTER INSERT 
ON `patient`
FOR EACH ROW
BEGIN
  SET @patient_id = new.patient_id;
  SET @registration_date  = new.date_created;
  SET @birth_date = (SELECT birthdate FROM person WHERE person_id=@patient_id);
  SET @gender = (SELECT gender FROM person WHERE person_id=@patient_id);
  
  INSERT INTO patient_report (patient_id, registration_date, birth_date, gender) VALUES(@patient_id, @registration_date, @birth_date, @gender);
                        
END$$

DELIMITER ;
