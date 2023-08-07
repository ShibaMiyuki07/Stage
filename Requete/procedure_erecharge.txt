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
-- 
-- table1
DELETE FROM `WORK`.ech_msisdn_capillarite_erec_jour WHERE tra_date=vDate;
INSERT INTO `WORK`.ech_msisdn_capillarite_erec_jour
SELECT DATE_FORMAT(tra_date,'S%v-%x') semaine,tra_date,DATE_FORMAT(tra_date,'%m-%x')  mois,ze.tra_sndr_msisdn tra_sndr_msisdn,region region_ref_zebra,zone zone_rf_zebra,secteur secteur_rf_zebra,axe_livraison,distributeur
FROM `DWH`.`zebra_rp2p_transaction` ze
LEFT JOIN `DM_RF`.`rf_Zebra` rf ON ze.tra_sndr_msisdn=rf.user_msisdn
WHERE tra_date=vDate 
AND tra_channel='C2S' GROUP BY tra_date,tra_sndr_msisdn
;
-- table2
UPDATE `WORK`.ech_msisdn_capillarite_erec_jour SET tra_sndr_msisdn=CONCAT(261,SUBSTR(tra_sndr_msisdn,2,9)) 
WHERE tra_sndr_msisdn LIKE '032%' AND tra_date=vDate;
-- table3
DELETE FROM `WORK`.final_capillarite_erec_jour WHERE tra_date=vDate;
INSERT INTO `WORK`.final_capillarite_erec_jour
SELECT tra_date,sig_nom_site,COUNT(tra_sndr_msisdn) capillarite_erecharge FROM(
SELECT ec.tra_date,ec.tra_sndr_msisdn,IF(rf.sig_nom_site IS NULL,'inconnu',rf.sig_nom_site) sig_nom_site 
FROM `WORK`.ech_msisdn_capillarite_erec_jour ec
LEFT JOIN  `WORK`.`caller_daily_location_ofl` od ON ec.tra_sndr_msisdn=od.msisdn  AND ec.tra_date=od.upd_dt
LEFT JOIN  `DM_RF`.`rf_sig_cell_krill_new` rf ON rf.sig_id=od.site_id
WHERE  tra_date=vDate
)a GROUP BY tra_date,sig_nom_site
;
-- /***********
   -- SEMAINE
-- ************/
-- maka semaine
SELECT DISTINCT semaine INTO xsemaine FROM `WORK`.ech_msisdn_capillarite_erec_jour WHERE tra_date=vDate;
-- début semaine:lundi
SELECT  DISTINCT `WORK`.FIRST_DAY_OF_WEEK(tra_date) INTO mxmin_date FROM  `WORK`.ech_msisdn_capillarite_erec_jour WHERE semaine=xsemaine;
-- fin semaine dimanche
SELECT  DISTINCT `WORK`.END_DAY_OF_WEEK(tra_date) INTO xmax_date FROM  `WORK`.ech_msisdn_capillarite_erec_jour WHERE semaine=xsemaine;
-- 1° loaclisation msisdn 
DELETE FROM `WORK`.capillarite_erec_semaine WHERE semaine=xsemaine;
INSERT INTO `WORK`.capillarite_erec_semaine
SELECT * FROM(
SELECT semaine,tra_sndr_msisdn msisdn,site_id,COUNT(site_id) nb_site FROM
(
SELECT ec.tra_date,od.upd_dt,ec.semaine,ec.tra_sndr_msisdn,od.msisdn,IFNULL(od.site_id,'') site_id
FROM `WORK`.ech_msisdn_capillarite_erec_jour ec
LEFT JOIN `WORK`.`caller_daily_location_ofl` od ON ec.tra_sndr_msisdn=od.msisdn AND ec.tra_date=od.upd_dt
WHERE tra_date BETWEEN mxmin_date AND xmax_date
GROUP BY tra_date,semaine,tra_sndr_msisdn,site_id -- 78 361
) a 
GROUP BY tra_sndr_msisdn,site_id
ORDER BY nb_site DESC)b
GROUP BY semaine,msisdn
HAVING MAX(nb_site); -- 14 157
-- 2° table final capillarite semaine
DELETE FROM `WORK`.final_capillarite_erec_semaine WHERE semaine=xsemaine;
INSERT INTO `WORK`.final_capillarite_erec_semaine
SELECT semaine,mxmin_date debut_semaine,sig_nom_site,COUNT(msisdn) nb_msisdn FROM(
SELECT semaine,msisdn,IF(sig.sig_nom_site IS NULL,'inconnu',sig_nom_site) sig_nom_site 
FROM `WORK`.capillarite_erec_semaine cap
LEFT JOIN `DM_RF`.`rf_sig_cell_krill_new` sig ON sig.sig_id=cap.site_id
WHERE semaine=xsemaine)
a GROUP BY sig_nom_site;
-- /***********
   -- MOIS
-- ************/
-- 1° msisdn distinct par mois
SELECT DISTINCT mois INTO xmois FROM `WORK`.ech_msisdn_capillarite_erec_jour WHERE tra_date=vDate;
-- jour du début mois
SELECT  DISTINCT DATE_FORMAT(vDate ,'%Y-%m-01') INTO mxmin_month FROM  `WORK`.ech_msisdn_capillarite_erec_jour  WHERE mois=xmois;
-- jour du fin mois
SELECT  DISTINCT LAST_DAY(vDate) INTO xmax_month  FROM  `WORK`.ech_msisdn_capillarite_erec_jour  WHERE mois=xmois;
-- 1° loaclisation msisdn 
DELETE FROM `WORK`.capillarite_erec_mois WHERE mois=xmois;
INSERT INTO `WORK`.capillarite_erec_mois
SELECT * FROM(
SELECT mois,tra_sndr_msisdn msisdn,site_id,COUNT(site_id) nb_site FROM
(
SELECT ec.tra_date,od.upd_dt,ec.mois,ec.tra_sndr_msisdn,od.msisdn,IFNULL(od.site_id,'') site_id
FROM `WORK`.ech_msisdn_capillarite_erec_jour ec
LEFT JOIN `WORK`.`caller_daily_location_ofl` od ON ec.tra_sndr_msisdn=od.msisdn AND ec.tra_date=od.upd_dt
WHERE tra_date BETWEEN mxmin_month AND xmax_month
GROUP BY tra_date,mois,tra_sndr_msisdn,site_id -- 78 361
) a 
GROUP BY tra_sndr_msisdn,site_id
ORDER BY nb_site DESC)b
GROUP BY mois,msisdn
HAVING MAX(nb_site); -- 14 157
-- 2° table final capillarite mois
DELETE FROM `WORK`.final_capillarite_erec_mois WHERE mois=xmois;
INSERT INTO `WORK`.final_capillarite_erec_mois
SELECT mois,mxmin_month debut_mois,sig_nom_site,COUNT(msisdn) nb_msisdn FROM(
SELECT mois,msisdn,IF(sig.sig_nom_site IS NULL,'inconnu',sig_nom_site) sig_nom_site 
FROM `WORK`.capillarite_erec_mois cap
LEFT JOIN `DM_RF`.`rf_sig_cell_krill_new` sig ON sig.sig_id=cap.site_id
WHERE mois=xmois)
a GROUP BY sig_nom_site;
/*CREATE TABLE `WORK`.ech_msisdn_capillarite_jour_save_juillet2022
SELECT * FROM `WORK`.ech_msisdn_capillarite_jour WHERE creationDate <='2022-07-31';*/
COMMIT;
END