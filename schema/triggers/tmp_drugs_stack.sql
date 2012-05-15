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
-- Table structure for table `tmp_drug_stack`
--

DROP TABLE IF EXISTS `tmp_drug_stack`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `tmp_drug_stack` (
  `drug_id` int(11) DEFAULT NULL,
  `bart_one_name` varchar(255) DEFAULT NULL,
  `bart_two_name` varchar(255) DEFAULT NULL,
  `new_drug_id` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `tmp_drug_stack`
--

LOCK TABLES `tmp_drug_stack` WRITE;
/*!40000 ALTER TABLE `tmp_drug_stack` DISABLE KEYS */;
INSERT INTO `tmp_drug_stack` VALUES (1,'Stavudine 30 Lamivudine 150','D4T / 3TC (Stavudine Lamivudine 30/150 tablet)',738),(2,'Stavudine 40 Lamivudine 150','Coviro40 (Lamivudine + Stavudine 150/40mg tablet)',91),(5,'Stavudine 30 Lamivudine 150 Nevirapine 200','Triomune-30',2),(6,'Stavudine 40 Lamivudine 150 Nevirapine 200','Triomune-40',3),(7,'Efavirenz 600','EFV (Efavirenz 600mg tablet)',11),(8,'Zidovudine 300 Lamivudine 150','AZT+3TC (Zidovudine and Lamivudine tablet)',39),(9,'Nevirapine 200','NVP (Nevirapine 200 mg tablet)',22),(10,'Abacavir 300','ABC (Abacavir 300mg tablet)',40),(11,'Didanosine 125','DDI (Didanosine 125mg tablet)',9),(12,'Lopinavir 133 Ritonavir 33','LPV/r (Lopinavir and Ritonavir 133/33mg tablet)',739),(13,'Didanosine 200','DDI (Didanosine 200mg tablet)',10),(14,'Tenofovir 300','TDF (Tenofavir 300 mg tablet)',14),(16,'Cotrimoxazole 480','Cotrimoxazole (480mg tablet)',297),(17,'Lopinavir 200 Ritonavir 50','LPV/r (Lopinavir and Ritonavir 200/50mg tablet)',73),(18,'Zidovudine Lamivudine Nevirapine','AZT+3TC+NVP',614),(22,'Lamivudine 150','3TC (Lamivudine 150mg tablet)',42),(51,'Efavirenz 200','EFV (Efavirenz 200mg tablet)',30),(56,'Stavudine 6 Lamivudine 30 Nevirapine 50','Triomune baby (d4T + 3TC + NVP 6/30/50mg tablet)',72),(57,'Stavudine 6 Lamivudine 30','Lamivir baby (Stavudine and Lamivudine 6/30mg tabl',73),(59,'Zidovudine 300 Lamivudine 150 Nevirapine 200','AZT/3TC/NVP (300/150/200mg tablet)',731),(148,'Tenofovir Disoproxil Fumarate/Lamivudine 300mg/300','TDF+3TC (Tenofavir and Lamivudine 300/300mg tablet',734);
/*!40000 ALTER TABLE `tmp_drug_stack` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2012-05-15 13:56:10
