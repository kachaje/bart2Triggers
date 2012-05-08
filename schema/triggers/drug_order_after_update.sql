DELIMITER $$
DROP TRIGGER IF EXISTS `drug_order_after_update`$$
CREATE TRIGGER `drug_order_after_update` AFTER UPDATE 
ON `drug_order`
FOR EACH ROW
BEGIN
  /*SET @group = (select distinct concept_id from concept_name where concept_id in (select concept_id from concept_set where concept_set = (select concept_id from concept_name where name = "Arvs")));*/

  IF new.quantity > 0 AND new.drug_inventory_id IN (SELECT drug_id FROM drug WHERE drug_id = new.drug_inventory_id AND concept_id IN (select distinct concept_id from concept_name where concept_id in (select concept_id from concept_set where concept_set = (select concept_id from concept_name where name = "Arvs")))) THEN 
     SET @auto_expiry = (SELECT auto_expire_date FROM orders WHERE order_id = new.order_id);
     SET @expiry = (SELECT DATE_ADD(@auto_expiry, INTERVAL (COALESCE(new.quantity, 0)/COALESCE(new.equivalent_daily_dose,1)) DAY));

     UPDATE patient_report SET expiry_date_for_last_arvs = @expiry WHERE patient_id = (SELECT patient_id FROM orders WHERE order_id = new.order_id);
  END IF;
                        
END$$

DELIMITER ;
