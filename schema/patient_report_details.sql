-- MySQL dump 10.13  Distrib 5.1.62, for debian-linux-gnu (i686)
--
-- Host: localhost    Database: test_bart
-- ------------------------------------------------------
-- Server version	5.1.62-0ubuntu0.11.04.1

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `patient_report_details`
--

DROP TABLE IF EXISTS `patient_report_details`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `patient_report_details` (
  `patient_report_details_id` int(11) NOT NULL AUTO_INCREMENT,
  `patient_id` int(11) DEFAULT NULL,
  `art_start_date` date DEFAULT NULL,
  `age_initiation` double DEFAULT NULL,
  `registration_date` datetime DEFAULT NULL,
  `patient_pregnant_date` datetime DEFAULT NULL,
  `reason_for_art_eligibility` varchar(255) DEFAULT NULL,
  `current_episode_of_tb` datetime DEFAULT NULL,
  `tb_within_the_last_2_years` datetime DEFAULT NULL,
  `karposis_sarcoma` datetime DEFAULT NULL,
  `death_date` datetime DEFAULT NULL,
  `expiry_date_for_last_arvs` datetime DEFAULT NULL,
  `arv_drugs_stopped` datetime DEFAULT NULL,
  `latest_state` varchar(255) DEFAULT NULL,
  `latest_state_date` datetime DEFAULT NULL,
  `latest_regimen` varchar(255) DEFAULT NULL,
  `latest_regimen_date` datetime DEFAULT NULL,
  `latest_side_effects` datetime DEFAULT NULL,
  `last_visit_date` datetime DEFAULT NULL,
  `missed_drugs_count` int(11) DEFAULT NULL,
  `last_missed_drugs_date` datetime DEFAULT NULL,
  `tb_status` varchar(64) DEFAULT NULL,
  `tb_status_date` datetime DEFAULT NULL,
  `patient_did_not_take_arvs_in_last_two_months` datetime DEFAULT NULL,
  `date_created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`patient_report_details_id`)
) ENGINE=InnoDB AUTO_INCREMENT=764 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2012-05-12 11:10:29
