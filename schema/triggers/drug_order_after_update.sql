DELIMITER $$
DROP TRIGGER IF EXISTS `drug_order_after_update`$$
CREATE TRIGGER `drug_order_after_update` AFTER UPDATE 
ON `drug_order`
FOR EACH ROW
BEGIN
  /*SET @group = (select distinct concept_id from concept_name where concept_id in (select concept_id from concept_set where concept_set = (select concept_id from concept_name where name = "Arvs")));*/

  IF new.quantity > 0 AND (new.drug_inventory_id IN (SELECT drug_id FROM drug WHERE drug_id = new.drug_inventory_id AND concept_id IN (select distinct concept_id from concept_name where concept_id in (select concept_id from concept_set where concept_set = (select concept_id from concept_name where name = "Arvs")))) OR ((SELECT  concept_id FROM drug WHERE drug_id = new.drug_inventory_id) IN (792,1610,1613,2988,7994,1612))) THEN 
     SET @auto_expiry = (SELECT auto_expire_date FROM orders WHERE order_id = new.order_id);
     SET @expiry = (SELECT DATE_ADD(@auto_expiry, INTERVAL (COALESCE(new.quantity, 0)/COALESCE(new.equivalent_daily_dose,1)) DAY));

     SET @art_start_date = (SELECT COALESCE(art_start_date,NULL) FROM patient_report WHERE patient_id = (SELECT patient_id FROM orders WHERE order_id = new.order_id));
     SET @age_initiation = (SELECT COALESCE(age_initiation,NULL) FROM patient_report WHERE patient_id = (SELECT patient_id FROM orders WHERE order_id = new.order_id));
     SET @registration_date = (SELECT COALESCE(registration_date,NULL) FROM patient_report WHERE patient_id = (SELECT patient_id FROM orders WHERE order_id = new.order_id));

     UPDATE patient_report SET expiry_date_for_last_arvs = @expiry WHERE patient_id = (SELECT patient_id FROM orders WHERE order_id = new.order_id);

     SET @startdate = (SELECT start_date FROM orders WHERE order_id = new.order_id);
     SET @report_id = (SELECT COALESCE(patient_report_details_id,"") FROM patient_report_details WHERE ((FLOOR((MONTH(latest_state_date) - 1)/3) = FLOOR((MONTH(@startdate) - 1)/3) AND YEAR(latest_state_date) = YEAR(@startdate)) OR (FLOOR((MONTH(last_visit_date) - 1)/3) = FLOOR((MONTH(@startdate) - 1)/3) AND YEAR(last_visit_date) = YEAR(@startdate)) OR (FLOOR((MONTH(latest_regimen_date) - 1)/3) = FLOOR((MONTH(@startdate) - 1)/3) AND YEAR(latest_regimen_date) = YEAR(@startdate)) OR (FLOOR((MONTH(tb_status_date) - 1)/3) = FLOOR((MONTH(@startdate) - 1)/3) AND YEAR(tb_status_date) = YEAR(@startdate))) AND patient_id = (SELECT patient_id FROM orders WHERE order_id = new.order_id));

     IF  @report_id != "" THEN
        UPDATE patient_report_details SET expiry_date_for_last_arvs = @expiry WHERE patient_report_details_id = @report_id;
     ELSE
     	INSERT INTO patient_report_details (patient_id, art_start_date, age_initiation, registration_date, expiry_date_for_last_arvs) VALUES ((SELECT patient_id FROM orders WHERE order_id = new.order_id), @art_start_date, @age_initiation, @registration_date, @expiry);
     END IF;
  END IF;
                        
END$$

DELIMITER ;
