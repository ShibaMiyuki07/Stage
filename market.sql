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

-- Listage de la structure de la table DM_RF. rf_market
CREATE TABLE IF NOT EXISTS `rf_market` (
  `id_market` int(5) DEFAULT NULL,
  `description` varchar(20) DEFAULT NULL,
  `id_billing_type` varchar(5) DEFAULT NULL,
  KEY `id_market` (`id_market`)
)DEFAULT CHARSET=utf8;

-- Listage des données de la table DM_RF.rf_market : 12 rows
/*!40000 ALTER TABLE `rf_market` DISABLE KEYS */;
INSERT INTO `rf_market` (`id_market`, `description`, `id_billing_type`) VALUES
	(1, 'B2C Prepaid', '1'),
	(2, 'B2C Postpaid', '2'),
	(3, 'BroadBand', '2'),
	(4, 'B2B Postpaid', '2'),
	(5, 'WHOLESALE', ''),
	(6, 'Non-facturable', ''),
	(2, 'B2C Postpaid', '3'),
	(2, 'B2C Postpaid', '4'),
	(3, 'BroadBand', '3'),
	(3, 'BroadBand', '4'),
	(4, 'B2B Postpaid', '3'),
	(4, 'B2B Postpaid', '4');
/*!40000 ALTER TABLE `rf_market` ENABLE KEYS */;

/*!40103 SET TIME_ZONE=IFNULL(@OLD_TIME_ZONE, 'system') */;
/*!40101 SET SQL_MODE=IFNULL(@OLD_SQL_MODE, '') */;
/*!40014 SET FOREIGN_KEY_CHECKS=IFNULL(@OLD_FOREIGN_KEY_CHECKS, 1) */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40111 SET SQL_NOTES=IFNULL(@OLD_SQL_NOTES, 1) */;
