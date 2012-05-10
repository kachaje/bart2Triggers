DELIMITER $$
DROP TRIGGER IF EXISTS `initiation_age`$$
CREATE TRIGGER `initiation_age` AFTER INSERT 
ON `encounter`
FOR EACH ROW
BEGIN
  SET @patient_id = new.patient_id;
  SET @age  = (SELECT (DATEDIFF(NOW(), birthdate)/365) FROM person p WHERE p.person_id = new.patient_id);
  SET @encounter_date = (SELECT DATE(new.encounter_datetime));
  SET @type_id = (SELECT encounter_type_id FROM encounter_type WHERE name = 'DISPENSING');
  SET @type = new.encounter_type;
  SET @previous_initiation_date = (SELECT DATE(COALESCE(art_start_date,NOW())) FROM patient_report WHERE patient_id = new.patient_id);

  IF @type = @type_id AND @encounter_date < @previous_initiation_date THEN
     UPDATE patient_report SET art_start_date = @encounter_date, age_initiation = @age WHERE patient_id = new.patient_id;
  END IF;                        

  SET @least_encounter_date = (SELECT DATE(COALESCE(MIN(encounter_datetime),NOW())) FROM encounter WHERE patient_id = new.patient_id);
  IF @encounter_date <= @least_encounter_date THEN
     UPDATE patient_report SET registration_date = @encounter_date WHERE patient_id = new.patient_id;
  END IF;                        

END$$

DELIMITER ;
