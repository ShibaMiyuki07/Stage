BEGIN
 DECLARE xsemaine VARCHAR(8);
 DECLARE mxmin_date DATE;
 DECLARE xmax_date DATE;
 DECLARE xmois VARCHAR(8);
 DECLARE mxmin_month DATE;
 DECLARE xmax_month DATE;
-- /***********
      -- JOUR
-- ************/
-- 3 min 8 sec
-- table1
 DELETE FROM `WORK`.ech_msisdn_capillarite_jour WHERE creationDate = vDate;
	
 INSERT INTO `WORK`.ech_msisdn_capillarite_jour
  SELECT
   DATE_FORMAT(DATE(creationDate),'S%v-%x') semaine,
   DATE(creationDate) creationDate,
   DATE_FORMAT(DATE(creationDate),'%m-%x') mois,
   nom.Identifiant_vendeur,
   region region_rf_nomad,
   zone zone_rf_nomad,
   code_canal,
   canal,
   distributeur
  FROM `WORK`.`kyc_nomad` nom
  LEFT JOIN `DM_RF`.`rf_Nomad` rf
   ON rf.Identifiant_vendeur = nom.Identifiant_vendeur
  WHERE DATE(creationDate) = vDate
  GROUP BY
   DATE(creationDate),
   Identifiant_vendeur
 ; -- 1 547
  
-- table2
 UPDATE `WORK`.ech_msisdn_capillarite_jour
  SET Identifiant_vendeur = CONCAT(261,SUBSTR(Identifiant_vendeur,2,9)) 
  WHERE Identifiant_vendeur LIKE '032%' AND creationDate = vDate;

-- table3
 DELETE FROM `WORK`.final_capillarite_jour WHERE creationDate = vDate;

 insert into `WORK`.final_capillarite_jour
  SELECT
   creationDate,
   sig_nom_site,
   canal,
   COUNT(Identifiant_vendeur) nb_vendeur_nomad
  FROM (
   SELECT
    creationDate,
    Identifiant_vendeur,
    canal,
    IF (rf.sig_nom_site IS NULL,'inconnu',rf.sig_nom_site) sig_nom_site
   FROM `WORK`.ech_msisdn_capillarite_jour tm
   LEFT JOIN `WORK`.`caller_daily_location_ofl` od
    ON tm.Identifiant_vendeur = od.msisdn
    AND tm.creationDate = od.upd_dt
   LEFT JOIN `DM_RF`.`rf_sig_cell_krill_new` rf
    ON rf.sig_id = od.site_id
   WHERE creationDate = vDate
    AND Identifiant_vendeur LIKE '261%')a
   GROUP BY
    creationDate,
    sig_nom_site,
    canal
 ;
-- /***********
   -- SEMAINE
-- ************/
-- 1° msisdn distinct par semaine

delete from `WORK`.ech_msisdn_capillarite_semaine where creationDate = vDate;
INSERT INTO `WORK`.ech_msisdn_capillarite_semaine
SELECT creationDate,DATE_FORMAT(creationDate,'S%v-%x') semaine,Identifiant_vendeur 
FROM `WORK`.ech_msisdn_capillarite_jour WHERE Identifiant_vendeur LIKE '26132%'
and creationDate = vDate
GROUP BY Identifiant_vendeur,semaine; 
select distinct semaine into xsemaine FROM `WORK`.ech_msisdn_capillarite_semaine where creationDate = vDate;
-- début semaine:lundi
SELECT  DISTINCT `WORK`.FIRST_DAY_OF_WEEK(creationDate) into mxmin_date FROM  `WORK`.ech_msisdn_capillarite_semaine WHERE semaine = xsemaine;
-- fin semaine dimanche
SELECT  DISTINCT `WORK`.END_DAY_OF_WEEK(creationDate) into xmax_date FROM  `WORK`.ech_msisdn_capillarite_semaine WHERE semaine = xsemaine;
-- 2° loaclisation msisdn 
delete from `WORK`.capillarite_semaine where semaine = xsemaine;
INSERT INTO `WORK`.capillarite_semaine
SELECT * FROM(
SELECT semaine,Identifiant_vendeur msisdn,site_id,COUNT(site_id) nb_site FROM
(
SELECT ec.creationDate,od.upd_dt,ec.semaine,ec.Identifiant_vendeur,od.msisdn,IFNULL(od.site_id,'') site_id
FROM `WORK`.ech_msisdn_capillarite_semaine ec
LEFT JOIN `WORK`.`caller_daily_location_ofl` od ON ec.Identifiant_vendeur = od.msisdn  AND ec.creationDate = od.upd_dt
WHERE creationDate BETWEEN mxmin_date AND xmax_date
GROUP BY creationDate,semaine,Identifiant_vendeur,site_id -- 15 704
) a 
GROUP BY Identifiant_vendeur,site_id
ORDER BY nb_site DESC)b
GROUP BY semaine,msisdn
HAVING MAX(nb_site);
-- 3° table final capillarite semaine
DELETE FROM `WORK`.final_capillarite_semaine WHERE semaine = xsemaine;
INSERT INTO `WORK`.final_capillarite_semaine
SELECT semaine,mxmin_date mxmin_date,sig_nom_site,canal,COUNT(msisdn) nb_msisdn FROM(
SELECT semaine,msisdn,IF(sig.sig_nom_site IS NULL,'',sig_nom_site) sig_nom_site ,rf.canal canal 
FROM `WORK`.capillarite_semaine cap
LEFT JOIN `DM_RF`.`rf_Nomad` rf ON rf.Identifiant_vendeur = CONCAT(0,SUBSTR(cap.msisdn,4,9)) 
LEFT JOIN `DM_RF`.`rf_sig_cell_krill_new` sig ON sig.sig_id = cap.site_id
WHERE semaine = xsemaine)
a GROUP BY sig_nom_site,canal;
-- /***********
   -- MOIS
-- ************/
-- 1° msisdn distinct par mois
DELETE FROM `WORK`.ech_msisdn_capillarite_month WHERE creationDate = vDate;
INSERT INTO `WORK`.ech_msisdn_capillarite_month
SELECT creationDate,DATE_FORMAT(creationDate,'%m-%x') mois,Identifiant_vendeur 
FROM `WORK`.ech_msisdn_capillarite_jour WHERE Identifiant_vendeur LIKE '26132%'
AND creationDate = vDate
GROUP BY Identifiant_vendeur,mois; -- 1534
SELECT DISTINCT mois INTO xmois FROM `WORK`.ech_msisdn_capillarite_month WHERE creationDate = vDate;
-- jour du début mois
SELECT  DISTINCT DATE_FORMAT(vDate ,'%Y-%m-01') INTO mxmin_month FROM  `WORK`.ech_msisdn_capillarite_month WHERE mois = xmois;
-- jour du fin mois
SELECT  DISTINCT LAST_DAY(vDate) INTO xmax_month  FROM  `WORK`.ech_msisdn_capillarite_month WHERE mois = xmois;
-- 2° loaclisation msisdn 
DELETE FROM `WORK`.capillarite_mois WHERE mois = xmois;
INSERT INTO `WORK`.capillarite_mois
SELECT * FROM(
SELECT mois,Identifiant_vendeur msisdn,site_id,COUNT(site_id) nb_site FROM
(
SELECT ec.creationDate,od.upd_dt,ec.mois,ec.Identifiant_vendeur,od.msisdn,IFNULL(od.site_id,'') site_id
FROM `WORK`.ech_msisdn_capillarite_month ec
LEFT JOIN `WORK`.`caller_daily_location_ofl` od ON ec.Identifiant_vendeur = od.msisdn AND ec.creationDate = od.upd_dt
WHERE  creationDate BETWEEN  mxmin_month AND xmax_month
GROUP BY creationDate,mois,Identifiant_vendeur,site_id 
) a 
GROUP BY Identifiant_vendeur,site_id
ORDER BY nb_site DESC)b
GROUP BY mois,msisdn
HAVING MAX(nb_site); 
-- 3° table final capillarite semaine
DELETE FROM `WORK`.final_capillarite_mois WHERE mois = xmois;
INSERT INTO `WORK`.final_capillarite_mois
SELECT mois,mxmin_month,sig_nom_site,canal,COUNT(msisdn) nb_msisdn FROM(
SELECT mois,msisdn,IF(sig.sig_nom_site IS NULL,'',sig_nom_site) sig_nom_site,rf.canal canal 
FROM `WORK`.capillarite_mois cap
LEFT JOIN `DM_RF`.`rf_Nomad` rf ON rf.Identifiant_vendeur = CONCAT(0,SUBSTR(cap.msisdn,4,9)) 
LEFT JOIN `DM_RF`.`rf_sig_cell_krill_new` sig ON sig.sig_id = cap.site_id
WHERE mois = xmois)
a GROUP BY sig_nom_site,canal; 
	/*CREATE TABLE `WORK`.ech_msisdn_capillarite_jour_save_juillet2022
SELECT * FROM `WORK`.ech_msisdn_capillarite_jour WHERE creationDate < = '2022-07-31';*/
COMMIT;
END