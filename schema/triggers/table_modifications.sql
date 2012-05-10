ALTER TABLE `patient_report` ADD COLUMN `patient_did_not_take_arvs_in_last_two_months` DATETIME  AFTER `tb_status_date`;
ALTER TABLE `patient_report_details` ADD COLUMN `last_visit_date` DATETIME  DEFAULT NULL AFTER `tb_status_date`;
ALTER TABLE `patient_report_details` ADD COLUMN `patient_did_not_take_arvs_in_last_two_months` DATETIME  DEFAULT NULL AFTER `last_visit_date`;
ALTER TABLE `test_bart`.`patient_report_details` MODIFY COLUMN `patient_report_details_id` INTEGER  NOT NULL AUTO_INCREMENT;



