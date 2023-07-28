-- --------------------------------------------------------
-- Hôte:                         192.168.61.196
-- Version du serveur:           10.0.19-MariaDB - MariaDB Server
-- SE du serveur:                Linux
-- HeidiSQL Version:             12.5.0.6677
-- --------------------------------------------------------

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET NAMES utf8 */;
/*!50503 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

-- Listage de la structure de la table DM_RF. rf_billing_type
CREATE TABLE IF NOT EXISTS `rf_billing_type` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(45) NOT NULL,
  `color` varchar(6) NOT NULL,
  PRIMARY KEY (`id`)
)DEFAULT CHARSET=utf8 ;

-- Listage des données de la table DM_RF.rf_billing_type : 5 rows
/*!40000 ALTER TABLE `rf_billing_type` DISABLE KEYS */;
INSERT INTO `rf_billing_type` (`id`, `name`, `color`) VALUES
	(1, 'Prepaid', ''),
	(2, 'Postpaid', ''),
	(3, 'Hybride', ''),
	(4, 'Postpaid Controlle', ''),
	(5, 'Taratra', '');
/*!40000 ALTER TABLE `rf_billing_type` ENABLE KEYS */;

/*!40103 SET TIME_ZONE=IFNULL(@OLD_TIME_ZONE, 'system') */;
/*!40101 SET SQL_MODE=IFNULL(@OLD_SQL_MODE, '') */;
/*!40014 SET FOREIGN_KEY_CHECKS=IFNULL(@OLD_FOREIGN_KEY_CHECKS, 1) */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40111 SET SQL_NOTES=IFNULL(@OLD_SQL_NOTES, 1) */;
