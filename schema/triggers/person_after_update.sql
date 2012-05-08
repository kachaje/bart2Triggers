DELIMITER $$
DROP TRIGGER IF EXISTS `person_after_update`$$
CREATE TRIGGER `person_after_update` AFTER UPDATE 
ON `person`
FOR EACH ROW
BEGIN
  IF new.dead = 1 THEN 
     UPDATE patient_report SET death_date = new.death_date WHERE patient_id = new.person_id;
  END IF;
                        
END$$

DELIMITER ;
