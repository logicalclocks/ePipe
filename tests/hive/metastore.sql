-- MySQL dump 10.13  Distrib 5.7.25-ndb-7.6.9, for linux-glibc2.12 (x86_64)
--
-- Host: localhost    Database: metastore
-- ------------------------------------------------------
-- Server version	5.7.25-ndb-7.6.9-cluster-gpl

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
-- Table structure for table `AUX_TABLE`
--

DROP TABLE IF EXISTS `AUX_TABLE`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `AUX_TABLE` (
  `MT_KEY1` varchar(128) NOT NULL,
  `MT_KEY2` bigint(20) NOT NULL,
  `MT_COMMENT` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`MT_KEY1`,`MT_KEY2`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `AUX_TABLE`
--

LOCK TABLES `AUX_TABLE` WRITE;
/*!40000 ALTER TABLE `AUX_TABLE` DISABLE KEYS */;
/*!40000 ALTER TABLE `AUX_TABLE` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `BUCKETING_COLS`
--

DROP TABLE IF EXISTS `BUCKETING_COLS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `BUCKETING_COLS` (
  `SD_ID` bigint(20) NOT NULL,
  `BUCKET_COL_NAME` varchar(256) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `INTEGER_IDX` int(11) NOT NULL,
  PRIMARY KEY (`SD_ID`,`INTEGER_IDX`),
  KEY `BUCKETING_COLS_N49` (`SD_ID`),
  CONSTRAINT `BUCKETING_COLS_FK1` FOREIGN KEY (`SD_ID`) REFERENCES `SDS` (`SD_ID`) ON DELETE CASCADE ON UPDATE RESTRICT
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `BUCKETING_COLS`
--

LOCK TABLES `BUCKETING_COLS` WRITE;
/*!40000 ALTER TABLE `BUCKETING_COLS` DISABLE KEYS */;
/*!40000 ALTER TABLE `BUCKETING_COLS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `CDS`
--

DROP TABLE IF EXISTS `CDS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `CDS` (
  `CD_ID` bigint(20) NOT NULL,
  PRIMARY KEY (`CD_ID`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `CDS`
--

LOCK TABLES `CDS` WRITE;
/*!40000 ALTER TABLE `CDS` DISABLE KEYS */;
INSERT INTO `CDS` VALUES (1),(11),(2);
/*!40000 ALTER TABLE `CDS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `COLUMNS_V2`
--

DROP TABLE IF EXISTS `COLUMNS_V2`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `COLUMNS_V2` (
  `CD_ID` bigint(20) NOT NULL,
  `COMMENT` varchar(256) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `COLUMN_NAME` varchar(767) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `TYPE_NAME` mediumtext,
  `INTEGER_IDX` int(11) NOT NULL,
  PRIMARY KEY (`CD_ID`,`COLUMN_NAME`),
  KEY `COLUMNS_V2_N49` (`CD_ID`),
  CONSTRAINT `COLUMNS_V2_FK1` FOREIGN KEY (`CD_ID`) REFERENCES `CDS` (`CD_ID`) ON DELETE CASCADE ON UPDATE RESTRICT
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `COLUMNS_V2`
--

LOCK TABLES `COLUMNS_V2` WRITE;
/*!40000 ALTER TABLE `COLUMNS_V2` DISABLE KEYS */;
INSERT INTO `COLUMNS_V2` VALUES (1,NULL,'latitude','float',10),(2,NULL,'price','float',8),(11,NULL,'longitude','float',11),(1,NULL,'price','float',9),(1,NULL,'city','string',1),(11,NULL,'price','float',9),(2,NULL,'city','string',1),(2,NULL,'baths','int',4),(2,NULL,'longitude','float',10),(11,NULL,'latitude','float',10),(1,NULL,'state','string',3),(1,NULL,'sale_date','string',8),(2,NULL,'state','string',2),(11,NULL,'zip','int',2),(2,NULL,'sale_date','string',7),(1,NULL,'longitude','float',11),(1,NULL,'beds','int',4),(11,NULL,'sales_type','string',7),(11,NULL,'state','string',3),(2,NULL,'street','string',0),(1,NULL,'baths','int',5),(11,NULL,'beds','int',4),(2,NULL,'sales_type','string',6),(1,NULL,'street','string',0),(1,NULL,'zip','int',2),(11,NULL,'sale_date','string',8),(11,NULL,'sq__ft','float',6),(2,NULL,'latitude','float',9),(2,NULL,'sq__ft','float',5),(11,NULL,'city','string',1),(11,NULL,'street','string',0),(1,NULL,'sq__ft','float',6),(1,NULL,'sales_type','string',7),(2,NULL,'beds','int',3),(11,NULL,'baths','int',5);
/*!40000 ALTER TABLE `COLUMNS_V2` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `COMPACTION_QUEUE`
--

DROP TABLE IF EXISTS `COMPACTION_QUEUE`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `COMPACTION_QUEUE` (
  `CQ_ID` bigint(20) NOT NULL,
  `CQ_DATABASE` varchar(128) NOT NULL,
  `CQ_TABLE` varchar(128) NOT NULL,
  `CQ_PARTITION` varchar(767) DEFAULT NULL,
  `CQ_STATE` char(1) NOT NULL,
  `CQ_TYPE` char(1) NOT NULL,
  `CQ_TBLPROPERTIES` varchar(2048) DEFAULT NULL,
  `CQ_WORKER_ID` varchar(128) DEFAULT NULL,
  `CQ_START` bigint(20) DEFAULT NULL,
  `CQ_RUN_AS` varchar(128) DEFAULT NULL,
  `CQ_HIGHEST_WRITE_ID` bigint(20) DEFAULT NULL,
  `CQ_META_INFO` varbinary(2048) DEFAULT NULL,
  `CQ_HADOOP_JOB_ID` varchar(32) DEFAULT NULL,
  PRIMARY KEY (`CQ_ID`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `COMPACTION_QUEUE`
--

LOCK TABLES `COMPACTION_QUEUE` WRITE;
/*!40000 ALTER TABLE `COMPACTION_QUEUE` DISABLE KEYS */;
/*!40000 ALTER TABLE `COMPACTION_QUEUE` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `COMPLETED_COMPACTIONS`
--

DROP TABLE IF EXISTS `COMPLETED_COMPACTIONS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `COMPLETED_COMPACTIONS` (
  `CC_ID` bigint(20) NOT NULL,
  `CC_DATABASE` varchar(128) NOT NULL,
  `CC_TABLE` varchar(128) NOT NULL,
  `CC_PARTITION` varchar(767) DEFAULT NULL,
  `CC_STATE` char(1) NOT NULL,
  `CC_TYPE` char(1) NOT NULL,
  `CC_TBLPROPERTIES` varchar(2048) DEFAULT NULL,
  `CC_WORKER_ID` varchar(128) DEFAULT NULL,
  `CC_START` bigint(20) DEFAULT NULL,
  `CC_END` bigint(20) DEFAULT NULL,
  `CC_RUN_AS` varchar(128) DEFAULT NULL,
  `CC_HIGHEST_WRITE_ID` bigint(20) DEFAULT NULL,
  `CC_META_INFO` varbinary(2048) DEFAULT NULL,
  `CC_HADOOP_JOB_ID` varchar(32) DEFAULT NULL,
  PRIMARY KEY (`CC_ID`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `COMPLETED_COMPACTIONS`
--

LOCK TABLES `COMPLETED_COMPACTIONS` WRITE;
/*!40000 ALTER TABLE `COMPLETED_COMPACTIONS` DISABLE KEYS */;
/*!40000 ALTER TABLE `COMPLETED_COMPACTIONS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `COMPLETED_TXN_COMPONENTS`
--

DROP TABLE IF EXISTS `COMPLETED_TXN_COMPONENTS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `COMPLETED_TXN_COMPONENTS` (
  `CTC_TXNID` bigint(20) NOT NULL,
  `CTC_DATABASE` varchar(128) NOT NULL,
  `CTC_TABLE` varchar(256) DEFAULT NULL,
  `CTC_PARTITION` varchar(767) DEFAULT NULL,
  `CTC_TIMESTAMP` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `CTC_WRITEID` bigint(20) DEFAULT NULL,
  KEY `COMPLETED_TXN_COMPONENTS_IDX` (`CTC_DATABASE`,`CTC_TABLE`,`CTC_PARTITION`) USING BTREE
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `COMPLETED_TXN_COMPONENTS`
--

LOCK TABLES `COMPLETED_TXN_COMPONENTS` WRITE;
/*!40000 ALTER TABLE `COMPLETED_TXN_COMPONENTS` DISABLE KEYS */;
/*!40000 ALTER TABLE `COMPLETED_TXN_COMPONENTS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `CTLGS`
--

DROP TABLE IF EXISTS `CTLGS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `CTLGS` (
  `CTLG_ID` bigint(20) NOT NULL,
  `NAME` varchar(256) COLLATE latin1_general_cs DEFAULT NULL,
  `DESC` varchar(4000) COLLATE latin1_general_cs DEFAULT NULL,
  `SD_ID` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`CTLG_ID`),
  UNIQUE KEY `UNIQUE_CATALOG` (`NAME`),
  KEY `DB_SD_FK` (`SD_ID`),
  CONSTRAINT `DB_SD_FK` FOREIGN KEY (`SD_ID`) REFERENCES `SDS` (`SD_ID`) ON DELETE CASCADE ON UPDATE RESTRICT
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `CTLGS`
--

LOCK TABLES `CTLGS` WRITE;
/*!40000 ALTER TABLE `CTLGS` DISABLE KEYS */;
INSERT INTO `CTLGS` VALUES (1,'hive','Default catalog, for Hive',1);
/*!40000 ALTER TABLE `CTLGS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `DATABASE_PARAMS`
--

DROP TABLE IF EXISTS `DATABASE_PARAMS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `DATABASE_PARAMS` (
  `DB_ID` bigint(20) NOT NULL,
  `PARAM_KEY` varchar(180) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `PARAM_VALUE` varchar(4000) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  PRIMARY KEY (`DB_ID`,`PARAM_KEY`),
  KEY `DATABASE_PARAMS_N49` (`DB_ID`),
  CONSTRAINT `DATABASE_PARAMS_FK1` FOREIGN KEY (`DB_ID`) REFERENCES `DBS` (`DB_ID`) ON DELETE CASCADE ON UPDATE RESTRICT
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `DATABASE_PARAMS`
--

LOCK TABLES `DATABASE_PARAMS` WRITE;
/*!40000 ALTER TABLE `DATABASE_PARAMS` DISABLE KEYS */;
/*!40000 ALTER TABLE `DATABASE_PARAMS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `DBS`
--

DROP TABLE IF EXISTS `DBS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `DBS` (
  `DB_ID` bigint(20) NOT NULL,
  `DESC` varchar(4000) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `NAME` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `OWNER_NAME` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `OWNER_TYPE` varchar(10) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `SD_ID` bigint(20) DEFAULT NULL,
  `CTLG_NAME` varchar(256) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  PRIMARY KEY (`DB_ID`),
  UNIQUE KEY `UNIQUE_DATABASE` (`NAME`,`CTLG_NAME`),
  KEY `CTLG_FK1` (`CTLG_NAME`),
  KEY `DB_SD_FK` (`SD_ID`),
  CONSTRAINT `CTLG_FK1` FOREIGN KEY (`CTLG_NAME`) REFERENCES `CTLGS` (`NAME`) ON DELETE CASCADE ON UPDATE RESTRICT,
  CONSTRAINT `DB_SD_FK` FOREIGN KEY (`SD_ID`) REFERENCES `SDS` (`SD_ID`) ON DELETE CASCADE ON UPDATE RESTRICT
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `DBS`
--

LOCK TABLES `DBS` WRITE;
/*!40000 ALTER TABLE `DBS` DISABLE KEYS */;
INSERT INTO `DBS` VALUES (1,'Default Hive database','default','public','ROLE',2,'hive'),(3,'Project general-purpose Hive database','flinking','hive','USER',74,'hive'),(5,'Project general-purpose Hive database','beam_meetup','hive','USER',76,'hive'),(4,'Featurestore database for project: flinking','flinking_featurestore','hive','USER',75,'hive'),(2,'Project general-purpose Hive database','fabio','hive','USER',3,'hive'),(6,'Featurestore database for project: beam_meetup','beam_meetup_featurestore','hive','USER',77,'hive');
/*!40000 ALTER TABLE `DBS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `DB_PRIVS`
--

DROP TABLE IF EXISTS `DB_PRIVS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `DB_PRIVS` (
  `DB_GRANT_ID` bigint(20) NOT NULL,
  `CREATE_TIME` int(11) NOT NULL,
  `DB_ID` bigint(20) DEFAULT NULL,
  `GRANT_OPTION` smallint(6) NOT NULL,
  `GRANTOR` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `GRANTOR_TYPE` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `PRINCIPAL_NAME` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `PRINCIPAL_TYPE` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `DB_PRIV` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  PRIMARY KEY (`DB_GRANT_ID`),
  UNIQUE KEY `DBPRIVILEGEINDEX` (`DB_ID`,`PRINCIPAL_NAME`,`PRINCIPAL_TYPE`,`DB_PRIV`,`GRANTOR`,`GRANTOR_TYPE`),
  KEY `DB_PRIVS_N49` (`DB_ID`),
  CONSTRAINT `DB_PRIVS_FK1` FOREIGN KEY (`DB_ID`) REFERENCES `DBS` (`DB_ID`) ON DELETE CASCADE ON UPDATE RESTRICT
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `DB_PRIVS`
--

LOCK TABLES `DB_PRIVS` WRITE;
/*!40000 ALTER TABLE `DB_PRIVS` DISABLE KEYS */;
/*!40000 ALTER TABLE `DB_PRIVS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `DELEGATION_TOKENS`
--

DROP TABLE IF EXISTS `DELEGATION_TOKENS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `DELEGATION_TOKENS` (
  `TOKEN_IDENT` varchar(767) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
  `TOKEN` varchar(767) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  PRIMARY KEY (`TOKEN_IDENT`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `DELEGATION_TOKENS`
--

LOCK TABLES `DELEGATION_TOKENS` WRITE;
/*!40000 ALTER TABLE `DELEGATION_TOKENS` DISABLE KEYS */;
/*!40000 ALTER TABLE `DELEGATION_TOKENS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `FUNCS`
--

DROP TABLE IF EXISTS `FUNCS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `FUNCS` (
  `FUNC_ID` bigint(20) NOT NULL,
  `CLASS_NAME` varchar(4000) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `CREATE_TIME` int(11) NOT NULL,
  `DB_ID` bigint(20) DEFAULT NULL,
  `FUNC_NAME` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `FUNC_TYPE` int(11) NOT NULL,
  `OWNER_NAME` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `OWNER_TYPE` varchar(10) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  PRIMARY KEY (`FUNC_ID`),
  UNIQUE KEY `UNIQUEFUNCTION` (`FUNC_NAME`,`DB_ID`),
  KEY `FUNCS_N49` (`DB_ID`),
  CONSTRAINT `FUNCS_FK1` FOREIGN KEY (`DB_ID`) REFERENCES `DBS` (`DB_ID`) ON DELETE CASCADE ON UPDATE RESTRICT
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `FUNCS`
--

LOCK TABLES `FUNCS` WRITE;
/*!40000 ALTER TABLE `FUNCS` DISABLE KEYS */;
/*!40000 ALTER TABLE `FUNCS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `FUNC_RU`
--

DROP TABLE IF EXISTS `FUNC_RU`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `FUNC_RU` (
  `FUNC_ID` bigint(20) NOT NULL,
  `RESOURCE_TYPE` int(11) NOT NULL,
  `RESOURCE_URI` varchar(4000) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `INTEGER_IDX` int(11) NOT NULL,
  PRIMARY KEY (`FUNC_ID`,`INTEGER_IDX`),
  CONSTRAINT `FUNC_RU_FK1` FOREIGN KEY (`FUNC_ID`) REFERENCES `FUNCS` (`FUNC_ID`) ON DELETE CASCADE ON UPDATE RESTRICT
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `FUNC_RU`
--

LOCK TABLES `FUNC_RU` WRITE;
/*!40000 ALTER TABLE `FUNC_RU` DISABLE KEYS */;
/*!40000 ALTER TABLE `FUNC_RU` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `GLOBAL_PRIVS`
--

DROP TABLE IF EXISTS `GLOBAL_PRIVS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `GLOBAL_PRIVS` (
  `USER_GRANT_ID` bigint(20) NOT NULL,
  `CREATE_TIME` int(11) NOT NULL,
  `GRANT_OPTION` smallint(6) NOT NULL,
  `GRANTOR` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `GRANTOR_TYPE` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `PRINCIPAL_NAME` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `PRINCIPAL_TYPE` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `USER_PRIV` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  PRIMARY KEY (`USER_GRANT_ID`),
  UNIQUE KEY `GLOBALPRIVILEGEINDEX` (`PRINCIPAL_NAME`,`PRINCIPAL_TYPE`,`USER_PRIV`,`GRANTOR`,`GRANTOR_TYPE`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `GLOBAL_PRIVS`
--

LOCK TABLES `GLOBAL_PRIVS` WRITE;
/*!40000 ALTER TABLE `GLOBAL_PRIVS` DISABLE KEYS */;
INSERT INTO `GLOBAL_PRIVS` VALUES (1,1564949108,1,'admin','ROLE','admin','ROLE','All');
/*!40000 ALTER TABLE `GLOBAL_PRIVS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `HIVE_LOCKS`
--

DROP TABLE IF EXISTS `HIVE_LOCKS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `HIVE_LOCKS` (
  `HL_LOCK_EXT_ID` bigint(20) NOT NULL,
  `HL_LOCK_INT_ID` bigint(20) NOT NULL,
  `HL_TXNID` bigint(20) NOT NULL,
  `HL_DB` varchar(128) NOT NULL,
  `HL_TABLE` varchar(128) DEFAULT NULL,
  `HL_PARTITION` varchar(767) DEFAULT NULL,
  `HL_LOCK_STATE` char(1) NOT NULL,
  `HL_LOCK_TYPE` char(1) NOT NULL,
  `HL_LAST_HEARTBEAT` bigint(20) NOT NULL,
  `HL_ACQUIRED_AT` bigint(20) DEFAULT NULL,
  `HL_USER` varchar(128) NOT NULL,
  `HL_HOST` varchar(128) NOT NULL,
  `HL_HEARTBEAT_COUNT` int(11) DEFAULT NULL,
  `HL_AGENT_INFO` varchar(128) DEFAULT NULL,
  `HL_BLOCKEDBY_EXT_ID` bigint(20) DEFAULT NULL,
  `HL_BLOCKEDBY_INT_ID` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`HL_LOCK_EXT_ID`,`HL_LOCK_INT_ID`),
  KEY `HIVE_LOCK_TXNID_INDEX` (`HL_TXNID`),
  KEY `HL_TXNID_IDX` (`HL_TXNID`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `HIVE_LOCKS`
--

LOCK TABLES `HIVE_LOCKS` WRITE;
/*!40000 ALTER TABLE `HIVE_LOCKS` DISABLE KEYS */;
/*!40000 ALTER TABLE `HIVE_LOCKS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `IDXS`
--

DROP TABLE IF EXISTS `IDXS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `IDXS` (
  `INDEX_ID` bigint(20) NOT NULL,
  `CREATE_TIME` int(11) NOT NULL,
  `DEFERRED_REBUILD` bit(1) NOT NULL,
  `INDEX_HANDLER_CLASS` varchar(4000) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `INDEX_NAME` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `INDEX_TBL_ID` bigint(20) DEFAULT NULL,
  `LAST_ACCESS_TIME` int(11) NOT NULL,
  `ORIG_TBL_ID` bigint(20) DEFAULT NULL,
  `SD_ID` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`INDEX_ID`),
  UNIQUE KEY `UNIQUEINDEX` (`INDEX_NAME`,`ORIG_TBL_ID`),
  KEY `IDXS_N51` (`SD_ID`),
  KEY `IDXS_N50` (`INDEX_TBL_ID`),
  KEY `IDXS_N49` (`ORIG_TBL_ID`),
  CONSTRAINT `IDXS_FK2` FOREIGN KEY (`SD_ID`) REFERENCES `SDS` (`SD_ID`) ON DELETE CASCADE ON UPDATE RESTRICT,
  CONSTRAINT `IDXS_FK1` FOREIGN KEY (`ORIG_TBL_ID`) REFERENCES `TBLS` (`TBL_ID`) ON DELETE CASCADE ON UPDATE RESTRICT,
  CONSTRAINT `IDXS_FK3` FOREIGN KEY (`INDEX_TBL_ID`) REFERENCES `TBLS` (`TBL_ID`) ON DELETE CASCADE ON UPDATE RESTRICT
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `IDXS`
--

LOCK TABLES `IDXS` WRITE;
/*!40000 ALTER TABLE `IDXS` DISABLE KEYS */;
/*!40000 ALTER TABLE `IDXS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `INDEX_PARAMS`
--

DROP TABLE IF EXISTS `INDEX_PARAMS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `INDEX_PARAMS` (
  `INDEX_ID` bigint(20) NOT NULL,
  `PARAM_KEY` varchar(256) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `PARAM_VALUE` varchar(4000) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  PRIMARY KEY (`INDEX_ID`,`PARAM_KEY`),
  KEY `INDEX_PARAMS_N49` (`INDEX_ID`),
  CONSTRAINT `INDEX_PARAMS_FK1` FOREIGN KEY (`INDEX_ID`) REFERENCES `IDXS` (`INDEX_ID`) ON DELETE CASCADE ON UPDATE RESTRICT
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `INDEX_PARAMS`
--

LOCK TABLES `INDEX_PARAMS` WRITE;
/*!40000 ALTER TABLE `INDEX_PARAMS` DISABLE KEYS */;
/*!40000 ALTER TABLE `INDEX_PARAMS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `I_SCHEMA`
--

DROP TABLE IF EXISTS `I_SCHEMA`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `I_SCHEMA` (
  `SCHEMA_ID` bigint(20) NOT NULL,
  `SCHEMA_TYPE` int(11) NOT NULL,
  `NAME` varchar(256) DEFAULT NULL,
  `DB_ID` bigint(20) DEFAULT NULL,
  `COMPATIBILITY` int(11) NOT NULL,
  `VALIDATION_LEVEL` int(11) NOT NULL,
  `CAN_EVOLVE` bit(1) NOT NULL,
  `SCHEMA_GROUP` varchar(256) DEFAULT NULL,
  `DESCRIPTION` varchar(4000) DEFAULT NULL,
  PRIMARY KEY (`SCHEMA_ID`),
  KEY `DB_ID` (`DB_ID`),
  KEY `UNIQUE_NAME` (`NAME`),
  CONSTRAINT `FK_348_612` FOREIGN KEY (`DB_ID`) REFERENCES `DBS` (`DB_ID`) ON DELETE CASCADE ON UPDATE RESTRICT
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `I_SCHEMA`
--

LOCK TABLES `I_SCHEMA` WRITE;
/*!40000 ALTER TABLE `I_SCHEMA` DISABLE KEYS */;
/*!40000 ALTER TABLE `I_SCHEMA` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `KEY_CONSTRAINTS`
--

DROP TABLE IF EXISTS `KEY_CONSTRAINTS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `KEY_CONSTRAINTS` (
  `CHILD_CD_ID` bigint(20) DEFAULT NULL,
  `CHILD_INTEGER_IDX` int(11) DEFAULT NULL,
  `CHILD_TBL_ID` bigint(20) DEFAULT NULL,
  `PARENT_CD_ID` bigint(20) DEFAULT NULL,
  `PARENT_INTEGER_IDX` int(11) NOT NULL,
  `PARENT_TBL_ID` bigint(20) NOT NULL,
  `POSITION` bigint(20) NOT NULL,
  `CONSTRAINT_NAME` varchar(400) NOT NULL,
  `CONSTRAINT_TYPE` smallint(6) NOT NULL,
  `UPDATE_RULE` smallint(6) DEFAULT NULL,
  `DELETE_RULE` smallint(6) DEFAULT NULL,
  `ENABLE_VALIDATE_RELY` smallint(6) NOT NULL,
  `DEFAULT_VALUE` varchar(400) DEFAULT NULL,
  PRIMARY KEY (`CONSTRAINT_NAME`,`POSITION`),
  KEY `KEY_CONSTRAINTS_FK1` (`CHILD_CD_ID`),
  KEY `KEY_CONSTRAINTS_FK3` (`PARENT_CD_ID`),
  KEY `KEY_CONSTRAINTS_FK2` (`CHILD_TBL_ID`),
  KEY `CONSTRAINTS_PARENT_TABLE_ID_INDEX` (`PARENT_TBL_ID`) USING BTREE,
  KEY `CONSTRAINTS_CONSTRAINT_TYPE_INDEX` (`CONSTRAINT_TYPE`) USING BTREE,
  CONSTRAINT `KEY_CONSTRAINTS_FK1` FOREIGN KEY (`CHILD_CD_ID`) REFERENCES `CDS` (`CD_ID`) ON DELETE CASCADE ON UPDATE NO ACTION,
  CONSTRAINT `KEY_CONSTRAINTS_FK3` FOREIGN KEY (`PARENT_CD_ID`) REFERENCES `CDS` (`CD_ID`) ON DELETE CASCADE ON UPDATE NO ACTION,
  CONSTRAINT `KEY_CONSTRAINTS_FK2` FOREIGN KEY (`CHILD_TBL_ID`) REFERENCES `TBLS` (`TBL_ID`) ON DELETE CASCADE ON UPDATE NO ACTION,
  CONSTRAINT `KEY_CONSTRAINTS_FK4` FOREIGN KEY (`PARENT_TBL_ID`) REFERENCES `TBLS` (`TBL_ID`) ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `KEY_CONSTRAINTS`
--

LOCK TABLES `KEY_CONSTRAINTS` WRITE;
/*!40000 ALTER TABLE `KEY_CONSTRAINTS` DISABLE KEYS */;
/*!40000 ALTER TABLE `KEY_CONSTRAINTS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `MASTER_KEYS`
--

DROP TABLE IF EXISTS `MASTER_KEYS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `MASTER_KEYS` (
  `KEY_ID` int(11) NOT NULL AUTO_INCREMENT,
  `MASTER_KEY` varchar(767) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  PRIMARY KEY (`KEY_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `MASTER_KEYS`
--

LOCK TABLES `MASTER_KEYS` WRITE;
/*!40000 ALTER TABLE `MASTER_KEYS` DISABLE KEYS */;
/*!40000 ALTER TABLE `MASTER_KEYS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `METASTORE_DB_PROPERTIES`
--

DROP TABLE IF EXISTS `METASTORE_DB_PROPERTIES`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `METASTORE_DB_PROPERTIES` (
  `PROPERTY_KEY` varchar(255) NOT NULL,
  `PROPERTY_VALUE` varchar(1000) NOT NULL,
  `DESCRIPTION` varchar(1000) DEFAULT NULL,
  PRIMARY KEY (`PROPERTY_KEY`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `METASTORE_DB_PROPERTIES`
--

LOCK TABLES `METASTORE_DB_PROPERTIES` WRITE;
/*!40000 ALTER TABLE `METASTORE_DB_PROPERTIES` DISABLE KEYS */;
/*!40000 ALTER TABLE `METASTORE_DB_PROPERTIES` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `MIN_HISTORY_LEVEL`
--

DROP TABLE IF EXISTS `MIN_HISTORY_LEVEL`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `MIN_HISTORY_LEVEL` (
  `MHL_TXNID` bigint(20) NOT NULL,
  `MHL_MIN_OPEN_TXNID` bigint(20) NOT NULL,
  PRIMARY KEY (`MHL_TXNID`),
  KEY `MIN_HISTORY_LEVEL_IDX` (`MHL_MIN_OPEN_TXNID`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `MIN_HISTORY_LEVEL`
--

LOCK TABLES `MIN_HISTORY_LEVEL` WRITE;
/*!40000 ALTER TABLE `MIN_HISTORY_LEVEL` DISABLE KEYS */;
/*!40000 ALTER TABLE `MIN_HISTORY_LEVEL` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `MV_CREATION_METADATA`
--

DROP TABLE IF EXISTS `MV_CREATION_METADATA`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `MV_CREATION_METADATA` (
  `MV_CREATION_METADATA_ID` bigint(20) NOT NULL,
  `CAT_NAME` varchar(256) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `DB_NAME` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `TBL_NAME` varchar(256) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `TXN_LIST` text,
  PRIMARY KEY (`MV_CREATION_METADATA_ID`),
  KEY `MV_UNIQUE_TABLE` (`TBL_NAME`,`DB_NAME`) USING BTREE
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `MV_CREATION_METADATA`
--

LOCK TABLES `MV_CREATION_METADATA` WRITE;
/*!40000 ALTER TABLE `MV_CREATION_METADATA` DISABLE KEYS */;
/*!40000 ALTER TABLE `MV_CREATION_METADATA` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `MV_TABLES_USED`
--

DROP TABLE IF EXISTS `MV_TABLES_USED`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `MV_TABLES_USED` (
  `MV_CREATION_METADATA_ID` bigint(20) NOT NULL,
  `TBL_ID` bigint(20) NOT NULL,
  KEY `MV_TABLES_USED_FK1` (`MV_CREATION_METADATA_ID`),
  KEY `MV_TABLES_USED_FK2` (`TBL_ID`),
  CONSTRAINT `MV_TABLES_USED_FK1` FOREIGN KEY (`MV_CREATION_METADATA_ID`) REFERENCES `MV_CREATION_METADATA` (`MV_CREATION_METADATA_ID`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT `MV_TABLES_USED_FK2` FOREIGN KEY (`TBL_ID`) REFERENCES `TBLS` (`TBL_ID`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `MV_TABLES_USED`
--

LOCK TABLES `MV_TABLES_USED` WRITE;
/*!40000 ALTER TABLE `MV_TABLES_USED` DISABLE KEYS */;
/*!40000 ALTER TABLE `MV_TABLES_USED` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `NEXT_COMPACTION_QUEUE_ID`
--

DROP TABLE IF EXISTS `NEXT_COMPACTION_QUEUE_ID`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `NEXT_COMPACTION_QUEUE_ID` (
  `NCQ_NEXT` bigint(20) NOT NULL
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `NEXT_COMPACTION_QUEUE_ID`
--

LOCK TABLES `NEXT_COMPACTION_QUEUE_ID` WRITE;
/*!40000 ALTER TABLE `NEXT_COMPACTION_QUEUE_ID` DISABLE KEYS */;
INSERT INTO `NEXT_COMPACTION_QUEUE_ID` VALUES (1);
/*!40000 ALTER TABLE `NEXT_COMPACTION_QUEUE_ID` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `NEXT_LOCK_ID`
--

DROP TABLE IF EXISTS `NEXT_LOCK_ID`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `NEXT_LOCK_ID` (
  `NL_NEXT` bigint(20) NOT NULL
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `NEXT_LOCK_ID`
--

LOCK TABLES `NEXT_LOCK_ID` WRITE;
/*!40000 ALTER TABLE `NEXT_LOCK_ID` DISABLE KEYS */;
INSERT INTO `NEXT_LOCK_ID` VALUES (1);
/*!40000 ALTER TABLE `NEXT_LOCK_ID` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `NEXT_TXN_ID`
--

DROP TABLE IF EXISTS `NEXT_TXN_ID`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `NEXT_TXN_ID` (
  `NTXN_NEXT` bigint(20) NOT NULL
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `NEXT_TXN_ID`
--

LOCK TABLES `NEXT_TXN_ID` WRITE;
/*!40000 ALTER TABLE `NEXT_TXN_ID` DISABLE KEYS */;
INSERT INTO `NEXT_TXN_ID` VALUES (1);
/*!40000 ALTER TABLE `NEXT_TXN_ID` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `NEXT_WRITE_ID`
--

DROP TABLE IF EXISTS `NEXT_WRITE_ID`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `NEXT_WRITE_ID` (
  `NWI_DATABASE` varchar(128) NOT NULL,
  `NWI_TABLE` varchar(256) NOT NULL,
  `NWI_NEXT` bigint(20) NOT NULL,
  UNIQUE KEY `NEXT_WRITE_ID_IDX` (`NWI_DATABASE`,`NWI_TABLE`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `NEXT_WRITE_ID`
--

LOCK TABLES `NEXT_WRITE_ID` WRITE;
/*!40000 ALTER TABLE `NEXT_WRITE_ID` DISABLE KEYS */;
/*!40000 ALTER TABLE `NEXT_WRITE_ID` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `NOTIFICATION_LOG`
--

DROP TABLE IF EXISTS `NOTIFICATION_LOG`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `NOTIFICATION_LOG` (
  `NL_ID` bigint(20) NOT NULL,
  `EVENT_ID` bigint(20) NOT NULL,
  `EVENT_TIME` int(11) NOT NULL,
  `EVENT_TYPE` varchar(32) NOT NULL,
  `CAT_NAME` varchar(256) DEFAULT NULL,
  `DB_NAME` varchar(128) DEFAULT NULL,
  `TBL_NAME` varchar(256) DEFAULT NULL,
  `MESSAGE` longtext,
  `MESSAGE_FORMAT` varchar(16) DEFAULT NULL,
  PRIMARY KEY (`NL_ID`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `NOTIFICATION_LOG`
--

LOCK TABLES `NOTIFICATION_LOG` WRITE;
/*!40000 ALTER TABLE `NOTIFICATION_LOG` DISABLE KEYS */;
/*!40000 ALTER TABLE `NOTIFICATION_LOG` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `NOTIFICATION_SEQUENCE`
--

DROP TABLE IF EXISTS `NOTIFICATION_SEQUENCE`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `NOTIFICATION_SEQUENCE` (
  `NNI_ID` bigint(20) NOT NULL,
  `NEXT_EVENT_ID` bigint(20) NOT NULL,
  PRIMARY KEY (`NNI_ID`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `NOTIFICATION_SEQUENCE`
--

LOCK TABLES `NOTIFICATION_SEQUENCE` WRITE;
/*!40000 ALTER TABLE `NOTIFICATION_SEQUENCE` DISABLE KEYS */;
INSERT INTO `NOTIFICATION_SEQUENCE` VALUES (1,1);
/*!40000 ALTER TABLE `NOTIFICATION_SEQUENCE` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `NUCLEUS_TABLES`
--

DROP TABLE IF EXISTS `NUCLEUS_TABLES`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `NUCLEUS_TABLES` (
  `CLASS_NAME` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `TABLE_NAME` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `TYPE` varchar(4) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `OWNER` varchar(2) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `VERSION` varchar(20) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `INTERFACE_NAME` varchar(255) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  PRIMARY KEY (`CLASS_NAME`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `NUCLEUS_TABLES`
--

LOCK TABLES `NUCLEUS_TABLES` WRITE;
/*!40000 ALTER TABLE `NUCLEUS_TABLES` DISABLE KEYS */;
/*!40000 ALTER TABLE `NUCLEUS_TABLES` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `PARTITIONS`
--

DROP TABLE IF EXISTS `PARTITIONS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `PARTITIONS` (
  `PART_ID` bigint(20) NOT NULL,
  `CREATE_TIME` int(11) NOT NULL,
  `LAST_ACCESS_TIME` int(11) NOT NULL,
  `PART_NAME` varchar(767) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `SD_ID` bigint(20) DEFAULT NULL,
  `TBL_ID` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`PART_ID`),
  UNIQUE KEY `UNIQUEPARTITION` (`PART_NAME`,`TBL_ID`),
  KEY `PARTITIONS_N49` (`TBL_ID`),
  KEY `PARTITIONS_N50` (`SD_ID`),
  CONSTRAINT `PARTITIONS_FK2` FOREIGN KEY (`SD_ID`) REFERENCES `SDS` (`SD_ID`) ON DELETE CASCADE ON UPDATE RESTRICT,
  CONSTRAINT `PARTITIONS_FK1` FOREIGN KEY (`TBL_ID`) REFERENCES `TBLS` (`TBL_ID`) ON DELETE CASCADE ON UPDATE RESTRICT
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `PARTITIONS`
--

LOCK TABLES `PARTITIONS` WRITE;
/*!40000 ALTER TABLE `PARTITIONS` DISABLE KEYS */;
INSERT INTO `PARTITIONS` VALUES (16,0,0,'zip=95626',21,2),(1,0,0,'zip=95828',6,2),(12,0,0,'zip=95834',17,2),(8,0,0,'zip=95683',13,2),(25,0,0,'zip=95655',30,2),(52,0,0,'zip=95817',57,2),(58,0,0,'zip=95746',63,2),(41,0,0,'zip=95765',46,2),(46,0,0,'zip=95648',51,2),(3,0,0,'zip=95690',8,2),(67,0,0,'zip=95842',72,2),(5,0,0,'zip=95825',10,2),(7,0,0,'zip=95677',12,2),(32,0,0,'zip=95667',37,2),(44,0,0,'zip=95829',49,2),(63,0,0,'zip=95818',68,2),(28,0,0,'zip=95682',33,2),(56,0,0,'zip=95758',61,2),(36,0,0,'zip=95833',41,2),(11,0,0,'zip=95864',16,2),(39,0,0,'zip=95811',44,2),(62,0,0,'zip=95693',67,2),(21,0,0,'zip=95726',26,2),(45,0,0,'zip=95621',50,2),(54,0,0,'zip=95824',59,2),(31,0,0,'zip=95762',36,2),(55,0,0,'zip=95663',60,2),(50,0,0,'zip=95722',55,2),(66,0,0,'zip=95823',71,2),(10,0,0,'zip=95742',15,2),(23,0,0,'zip=95631',28,2),(14,0,0,'zip=95831',19,2),(35,0,0,'zip=95832',40,2),(37,0,0,'zip=95632',42,2),(51,0,0,'zip=95841',56,2),(47,0,0,'zip=95630',52,2),(30,0,0,'zip=95661',35,2),(64,0,0,'zip=95747',69,2),(65,0,0,'zip=95670',70,2),(42,0,0,'zip=95838',47,2),(4,0,0,'zip=95843',9,2),(68,0,0,'zip=95623',73,2),(20,0,0,'zip=95821',25,2),(18,0,0,'zip=95610',23,2),(17,0,0,'zip=95691',22,2),(38,0,0,'zip=95608',43,2),(34,0,0,'zip=95614',39,2),(15,0,0,'zip=95650',20,2),(29,0,0,'zip=95673',34,2),(9,0,0,'zip=95819',14,2),(13,0,0,'zip=95635',18,2),(2,0,0,'zip=95662',7,2),(49,0,0,'zip=95820',54,2),(6,0,0,'zip=95757',11,2),(24,0,0,'zip=95816',29,2),(19,0,0,'zip=95827',24,2),(22,0,0,'zip=95660',27,2),(33,0,0,'zip=95678',38,2),(60,0,0,'zip=95826',65,2),(43,0,0,'zip=95603',48,2),(26,0,0,'zip=95619',31,2),(48,0,0,'zip=95822',53,2),(53,0,0,'zip=95814',58,2),(61,0,0,'zip=95835',66,2),(27,0,0,'zip=95815',32,2),(40,0,0,'zip=95624',45,2),(57,0,0,'zip=95628',62,2),(59,0,0,'zip=95633',64,2);
/*!40000 ALTER TABLE `PARTITIONS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `PARTITION_EVENTS`
--

DROP TABLE IF EXISTS `PARTITION_EVENTS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `PARTITION_EVENTS` (
  `PART_NAME_ID` bigint(20) NOT NULL,
  `CAT_NAME` varchar(256) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `DB_NAME` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `EVENT_TIME` bigint(20) NOT NULL,
  `EVENT_TYPE` int(11) NOT NULL,
  `PARTITION_NAME` varchar(767) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `TBL_NAME` varchar(256) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  PRIMARY KEY (`PART_NAME_ID`),
  KEY `PARTITIONEVENTINDEX` (`PARTITION_NAME`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `PARTITION_EVENTS`
--

LOCK TABLES `PARTITION_EVENTS` WRITE;
/*!40000 ALTER TABLE `PARTITION_EVENTS` DISABLE KEYS */;
/*!40000 ALTER TABLE `PARTITION_EVENTS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `PARTITION_KEYS`
--

DROP TABLE IF EXISTS `PARTITION_KEYS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `PARTITION_KEYS` (
  `TBL_ID` bigint(20) NOT NULL,
  `PKEY_COMMENT` varchar(4000) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `PKEY_NAME` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `PKEY_TYPE` varchar(767) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `INTEGER_IDX` int(11) NOT NULL,
  PRIMARY KEY (`TBL_ID`,`PKEY_NAME`),
  KEY `PARTITION_KEYS_N49` (`TBL_ID`),
  CONSTRAINT `PARTITION_KEYS_FK1` FOREIGN KEY (`TBL_ID`) REFERENCES `TBLS` (`TBL_ID`) ON DELETE CASCADE ON UPDATE RESTRICT
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `PARTITION_KEYS`
--

LOCK TABLES `PARTITION_KEYS` WRITE;
/*!40000 ALTER TABLE `PARTITION_KEYS` DISABLE KEYS */;
INSERT INTO `PARTITION_KEYS` VALUES (2,NULL,'zip','int',0);
/*!40000 ALTER TABLE `PARTITION_KEYS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `PARTITION_KEY_VALS`
--

DROP TABLE IF EXISTS `PARTITION_KEY_VALS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `PARTITION_KEY_VALS` (
  `PART_ID` bigint(20) NOT NULL,
  `PART_KEY_VAL` varchar(256) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `INTEGER_IDX` int(11) NOT NULL,
  PRIMARY KEY (`PART_ID`,`INTEGER_IDX`),
  KEY `PARTITION_KEY_VALS_N49` (`PART_ID`),
  CONSTRAINT `PARTITION_KEY_VALS_FK1` FOREIGN KEY (`PART_ID`) REFERENCES `PARTITIONS` (`PART_ID`) ON DELETE CASCADE ON UPDATE RESTRICT
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `PARTITION_KEY_VALS`
--

LOCK TABLES `PARTITION_KEY_VALS` WRITE;
/*!40000 ALTER TABLE `PARTITION_KEY_VALS` DISABLE KEYS */;
INSERT INTO `PARTITION_KEY_VALS` VALUES (28,'95682',0),(15,'95650',0),(6,'95757',0),(14,'95831',0),(18,'95610',0),(25,'95655',0),(31,'95762',0),(3,'95690',0),(50,'95722',0),(52,'95817',0),(17,'95691',0),(65,'95670',0),(58,'95746',0),(63,'95818',0),(37,'95632',0),(43,'95603',0),(4,'95843',0),(22,'95660',0),(30,'95661',0),(60,'95826',0),(33,'95678',0),(49,'95820',0),(66,'95823',0),(67,'95842',0),(19,'95827',0),(21,'95726',0),(2,'95662',0),(10,'95742',0),(34,'95614',0),(57,'95628',0),(51,'95841',0),(32,'95667',0),(47,'95630',0),(39,'95811',0),(41,'95765',0),(20,'95821',0),(48,'95822',0),(24,'95816',0),(56,'95758',0),(26,'95619',0),(27,'95815',0),(36,'95833',0),(5,'95825',0),(23,'95631',0),(59,'95633',0),(35,'95832',0),(42,'95838',0),(7,'95677',0),(13,'95635',0),(29,'95673',0),(55,'95663',0),(38,'95608',0),(44,'95829',0),(40,'95624',0),(9,'95819',0),(68,'95623',0),(16,'95626',0),(12,'95834',0),(62,'95693',0),(8,'95683',0),(46,'95648',0),(64,'95747',0),(11,'95864',0),(45,'95621',0),(54,'95824',0),(1,'95828',0),(61,'95835',0),(53,'95814',0);
/*!40000 ALTER TABLE `PARTITION_KEY_VALS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `PARTITION_PARAMS`
--

DROP TABLE IF EXISTS `PARTITION_PARAMS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `PARTITION_PARAMS` (
  `PART_ID` bigint(20) NOT NULL,
  `PARAM_KEY` varchar(256) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `PARAM_VALUE` varchar(4000) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  PRIMARY KEY (`PART_ID`,`PARAM_KEY`),
  KEY `PARTITION_PARAMS_N49` (`PART_ID`),
  CONSTRAINT `PARTITION_PARAMS_FK1` FOREIGN KEY (`PART_ID`) REFERENCES `PARTITIONS` (`PART_ID`) ON DELETE CASCADE ON UPDATE RESTRICT
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `PARTITION_PARAMS`
--

LOCK TABLES `PARTITION_PARAMS` WRITE;
/*!40000 ALTER TABLE `PARTITION_PARAMS` DISABLE KEYS */;
INSERT INTO `PARTITION_PARAMS` VALUES (19,'rawDataSize','4608'),(25,'transient_lastDdlTime','1564997241'),(36,'rawDataSize','10200'),(39,'rawDataSize','1014'),(44,'numFiles','1'),(46,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(58,'rawDataSize','1542'),(60,'transient_lastDdlTime','1564997242'),(66,'numFiles','1'),(2,'numFiles','1'),(7,'totalSize','1645'),(8,'totalSize','1640'),(12,'numRows','22'),(21,'transient_lastDdlTime','1564997241'),(28,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(38,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(45,'transient_lastDdlTime','1564997242'),(46,'numRows','72'),(46,'transient_lastDdlTime','1564997242'),(55,'rawDataSize','510'),(64,'rawDataSize','10240'),(68,'numFiles','1'),(5,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(6,'transient_lastDdlTime','1564997241'),(11,'transient_lastDdlTime','1564997241'),(12,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(15,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(34,'rawDataSize','508'),(37,'rawDataSize','10584'),(56,'numFiles','1'),(58,'totalSize','1594'),(67,'rawDataSize','11220'),(13,'transient_lastDdlTime','1564997241'),(19,'totalSize','1846'),(47,'totalSize','2016'),(52,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(53,'numFiles','1'),(56,'numRows','44'),(68,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(3,'numRows','1'),(14,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(29,'numRows','13'),(30,'totalSize','1738'),(40,'transient_lastDdlTime','1564997242'),(47,'rawDataSize','8602'),(48,'rawDataSize','12192'),(51,'numFiles','1'),(57,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(7,'rawDataSize','3042'),(7,'numFiles','1'),(18,'transient_lastDdlTime','1564997241'),(35,'transient_lastDdlTime','1564997242'),(36,'transient_lastDdlTime','1564997242'),(38,'totalSize','2146'),(42,'numRows','37'),(61,'totalSize','2579'),(28,'rawDataSize','5110'),(50,'numRows','1'),(68,'totalSize','1519'),(16,'rawDataSize','2032'),(19,'numFiles','1'),(20,'rawDataSize','3060'),(24,'numRows','4'),(55,'numRows','1'),(14,'transient_lastDdlTime','1564997241'),(17,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(21,'numRows','3'),(35,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(42,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(47,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(49,'numFiles','1'),(51,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(59,'numFiles','1'),(2,'transient_lastDdlTime','1564997241'),(21,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(24,'totalSize','1552'),(36,'totalSize','2078'),(49,'transient_lastDdlTime','1564997242'),(4,'numFiles','1'),(40,'numFiles','1'),(50,'transient_lastDdlTime','1564997242'),(51,'totalSize','1743'),(10,'transient_lastDdlTime','1564997241'),(12,'totalSize','2121'),(13,'totalSize','1288'),(23,'numRows','1'),(36,'numRows','20'),(44,'totalSize','1864'),(54,'rawDataSize','6096'),(55,'totalSize','1271'),(56,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(58,'transient_lastDdlTime','1564997242'),(65,'numRows','21'),(5,'numRows','13'),(11,'numFiles','1'),(31,'numFiles','1'),(38,'rawDataSize','10180'),(43,'transient_lastDdlTime','1564997242'),(10,'totalSize','1881'),(20,'numFiles','1'),(23,'numFiles','1'),(34,'totalSize','1272'),(44,'transient_lastDdlTime','1564997242'),(2,'rawDataSize','5621'),(18,'numRows','7'),(23,'transient_lastDdlTime','1564997241'),(43,'numFiles','1'),(44,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(49,'numRows','23'),(67,'transient_lastDdlTime','1564997242'),(4,'transient_lastDdlTime','1564997241'),(21,'numFiles','1'),(26,'transient_lastDdlTime','1564997241'),(29,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(30,'rawDataSize','4080'),(39,'transient_lastDdlTime','1564997242'),(62,'numFiles','1'),(6,'numFiles','1'),(30,'numFiles','1'),(41,'numRows','11'),(45,'numFiles','1'),(63,'transient_lastDdlTime','1564997242'),(1,'transient_lastDdlTime','1564997241'),(5,'transient_lastDdlTime','1564997241'),(9,'rawDataSize','2032'),(10,'rawDataSize','5698'),(56,'totalSize','2693'),(64,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(25,'numFiles','1'),(26,'rawDataSize','516'),(57,'transient_lastDdlTime','1564997242'),(61,'numRows','37'),(61,'numFiles','1'),(8,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(14,'numFiles','1'),(20,'totalSize','1682'),(30,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(31,'numRows','23'),(32,'transient_lastDdlTime','1564997242'),(44,'numRows','11'),(56,'rawDataSize','22484'),(57,'rawDataSize','4590'),(24,'transient_lastDdlTime','1564997241'),(27,'rawDataSize','9180'),(31,'transient_lastDdlTime','1564997241'),(32,'numFiles','1'),(33,'totalSize','2085'),(39,'numRows','2'),(39,'totalSize','1462'),(44,'rawDataSize','5621'),(59,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(60,'numFiles','1'),(62,'numRows','5'),(20,'transient_lastDdlTime','1564997241'),(22,'transient_lastDdlTime','1564997241'),(27,'totalSize','2060'),(36,'numFiles','1'),(47,'transient_lastDdlTime','1564997242'),(8,'rawDataSize','2056'),(18,'rawDataSize','3612'),(37,'totalSize','2091'),(39,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(51,'transient_lastDdlTime','1564997242'),(67,'numRows','22'),(12,'numFiles','1'),(16,'numFiles','1'),(17,'numFiles','1'),(48,'numRows','24'),(50,'totalSize','1286'),(54,'transient_lastDdlTime','1564997242'),(54,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(55,'transient_lastDdlTime','1564997242'),(60,'numRows','18'),(62,'totalSize','1633'),(62,'rawDataSize','2540'),(4,'numRows','33'),(25,'totalSize','1274'),(31,'rawDataSize','11868'),(32,'numRows','10'),(36,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(62,'transient_lastDdlTime','1564997242'),(27,'numRows','18'),(30,'transient_lastDdlTime','1564997241'),(46,'numFiles','1'),(61,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(65,'rawDataSize','10815'),(66,'transient_lastDdlTime','1564997242'),(7,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(33,'rawDataSize','10160'),(61,'rawDataSize','18907'),(64,'numFiles','1'),(66,'totalSize','3085'),(67,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(20,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(47,'numFiles','1'),(57,'totalSize','1780'),(58,'numFiles','1'),(59,'transient_lastDdlTime','1564997242'),(15,'totalSize','1488'),(15,'transient_lastDdlTime','1564997241'),(16,'totalSize','1584'),(26,'totalSize','1301'),(32,'totalSize','1807'),(34,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(43,'numRows','5'),(26,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(58,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(64,'numRows','20'),(68,'rawDataSize','1024'),(1,'totalSize','2810'),(1,'rawDataSize','23040'),(4,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(11,'numRows','5'),(12,'rawDataSize','11220'),(14,'numRows','10'),(31,'totalSize','2143'),(34,'numRows','1'),(45,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(52,'numFiles','1'),(62,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(15,'numRows','2'),(25,'numRows','1'),(28,'numFiles','1'),(35,'totalSize','1872'),(37,'numFiles','1'),(48,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(63,'rawDataSize','3563'),(27,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(39,'numFiles','1'),(56,'transient_lastDdlTime','1564997242'),(60,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(11,'totalSize','1639'),(30,'numRows','8'),(31,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(32,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(28,'numRows','10'),(29,'rawDataSize','6591'),(42,'transient_lastDdlTime','1564997242'),(42,'numFiles','1'),(51,'rawDataSize','3577'),(63,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(64,'totalSize','2158'),(6,'rawDataSize','18396'),(9,'totalSize','1580'),(17,'numRows','3'),(18,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(19,'transient_lastDdlTime','1564997241'),(53,'totalSize','1530'),(7,'numRows','6'),(11,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(22,'numFiles','1'),(33,'numFiles','1'),(13,'numRows','1'),(17,'rawDataSize','1548'),(54,'numRows','12'),(67,'numFiles','1'),(9,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(22,'rawDataSize','10815'),(33,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(49,'rawDataSize','11684'),(54,'totalSize','1843'),(6,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(8,'numRows','4'),(14,'totalSize','1807'),(17,'totalSize','1554'),(29,'totalSize','1874'),(45,'totalSize','2361'),(2,'totalSize','1852'),(3,'transient_lastDdlTime','1564997241'),(23,'totalSize','1243'),(27,'transient_lastDdlTime','1564997241'),(28,'totalSize','1872'),(35,'rawDataSize','6132'),(40,'rawDataSize','17340'),(41,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(46,'rawDataSize','36504'),(53,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(1,'numRows','45'),(8,'numFiles','1'),(13,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(16,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(19,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(43,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(53,'numRows','3'),(64,'transient_lastDdlTime','1564997242'),(65,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(14,'rawDataSize','5120'),(22,'totalSize','2134'),(23,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(24,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(24,'numFiles','1'),(34,'transient_lastDdlTime','1564997242'),(40,'numRows','34'),(2,'numRows','11'),(18,'totalSize','1793'),(29,'numFiles','1'),(42,'rawDataSize','18870'),(43,'rawDataSize','2530'),(49,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(52,'transient_lastDdlTime','1564997242'),(54,'numFiles','1'),(13,'rawDataSize','514'),(15,'rawDataSize','1010'),(21,'rawDataSize','1533'),(28,'transient_lastDdlTime','1564997241'),(45,'numRows','28'),(50,'numFiles','1'),(66,'rawDataSize','31110'),(3,'rawDataSize','513'),(8,'transient_lastDdlTime','1564997241'),(47,'numRows','17'),(68,'numRows','2'),(9,'numRows','4'),(9,'numFiles','1'),(37,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(38,'transient_lastDdlTime','1564997242'),(63,'numFiles','1'),(16,'numRows','4'),(25,'rawDataSize','510'),(33,'numRows','20'),(46,'totalSize','2574'),(48,'totalSize','2184'),(49,'totalSize','2066'),(66,'numRows','61'),(3,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(3,'numFiles','1'),(4,'totalSize','2462'),(5,'totalSize','1933'),(43,'totalSize','1630'),(51,'numRows','7'),(16,'transient_lastDdlTime','1564997241'),(26,'numRows','1'),(37,'transient_lastDdlTime','1564997242'),(42,'totalSize','2520'),(68,'transient_lastDdlTime','1564997242'),(2,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(6,'numRows','36'),(53,'transient_lastDdlTime','1564997242'),(60,'totalSize','2088'),(23,'rawDataSize','507'),(35,'numFiles','1'),(40,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(41,'numFiles','1'),(53,'rawDataSize','1518'),(60,'rawDataSize','9180'),(7,'transient_lastDdlTime','1564997241'),(41,'transient_lastDdlTime','1564997242'),(45,'rawDataSize','14392'),(57,'numRows','9'),(19,'numRows','9'),(21,'totalSize','1558'),(22,'numRows','21'),(63,'totalSize','1706'),(5,'numFiles','1'),(33,'transient_lastDdlTime','1564997242'),(38,'numFiles','1'),(48,'numFiles','1'),(48,'transient_lastDdlTime','1564997242'),(52,'rawDataSize','3549'),(55,'numFiles','1'),(63,'numRows','7'),(17,'transient_lastDdlTime','1564997241'),(24,'rawDataSize','2016'),(27,'numFiles','1'),(41,'totalSize','1808'),(67,'totalSize','2219'),(4,'rawDataSize','16797'),(5,'rawDataSize','6604'),(10,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(11,'rawDataSize','2555'),(52,'totalSize','1671'),(12,'transient_lastDdlTime','1564997241'),(18,'numFiles','1'),(25,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(35,'numRows','12'),(37,'numRows','21'),(38,'numRows','20'),(40,'totalSize','2522'),(50,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(1,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(1,'numFiles','1'),(9,'transient_lastDdlTime','1564997241'),(13,'numFiles','1'),(41,'rawDataSize','5577'),(6,'totalSize','2509'),(22,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(32,'rawDataSize','5140'),(50,'rawDataSize','515'),(58,'numRows','3'),(10,'numFiles','1'),(15,'numFiles','1'),(3,'totalSize','1281'),(34,'numFiles','1'),(52,'numRows','7'),(55,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(57,'numFiles','1'),(59,'numRows','1'),(59,'totalSize','1280'),(61,'transient_lastDdlTime','1564997242'),(65,'numFiles','1'),(65,'totalSize','2264'),(66,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(10,'numRows','11'),(20,'numRows','6'),(26,'numFiles','1'),(29,'transient_lastDdlTime','1564997241'),(59,'rawDataSize','514'),(65,'transient_lastDdlTime','1564997242');
/*!40000 ALTER TABLE `PARTITION_PARAMS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `PART_COL_PRIVS`
--

DROP TABLE IF EXISTS `PART_COL_PRIVS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `PART_COL_PRIVS` (
  `PART_COLUMN_GRANT_ID` bigint(20) NOT NULL,
  `COLUMN_NAME` varchar(767) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `CREATE_TIME` int(11) NOT NULL,
  `GRANT_OPTION` smallint(6) NOT NULL,
  `GRANTOR` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `GRANTOR_TYPE` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `PART_ID` bigint(20) DEFAULT NULL,
  `PRINCIPAL_NAME` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `PRINCIPAL_TYPE` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `PART_COL_PRIV` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  PRIMARY KEY (`PART_COLUMN_GRANT_ID`),
  KEY `PART_COL_PRIVS_N49` (`PART_ID`),
  KEY `PARTITIONCOLUMNPRIVILEGEINDEX` (`PART_ID`,`COLUMN_NAME`,`PRINCIPAL_NAME`,`PRINCIPAL_TYPE`,`PART_COL_PRIV`,`GRANTOR`,`GRANTOR_TYPE`),
  CONSTRAINT `PART_COL_PRIVS_FK1` FOREIGN KEY (`PART_ID`) REFERENCES `PARTITIONS` (`PART_ID`) ON DELETE CASCADE ON UPDATE RESTRICT
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `PART_COL_PRIVS`
--

LOCK TABLES `PART_COL_PRIVS` WRITE;
/*!40000 ALTER TABLE `PART_COL_PRIVS` DISABLE KEYS */;
/*!40000 ALTER TABLE `PART_COL_PRIVS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `PART_COL_STATS`
--

DROP TABLE IF EXISTS `PART_COL_STATS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `PART_COL_STATS` (
  `CS_ID` bigint(20) NOT NULL,
  `CAT_NAME` varchar(256) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `DB_NAME` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `TABLE_NAME` varchar(256) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `PARTITION_NAME` varchar(767) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `COLUMN_NAME` varchar(767) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `COLUMN_TYPE` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `PART_ID` bigint(20) NOT NULL,
  `LONG_LOW_VALUE` bigint(20) DEFAULT NULL,
  `LONG_HIGH_VALUE` bigint(20) DEFAULT NULL,
  `DOUBLE_HIGH_VALUE` double(53,4) DEFAULT NULL,
  `DOUBLE_LOW_VALUE` double(53,4) DEFAULT NULL,
  `BIG_DECIMAL_LOW_VALUE` varchar(4000) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `BIG_DECIMAL_HIGH_VALUE` varchar(4000) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `NUM_NULLS` bigint(20) NOT NULL,
  `NUM_DISTINCTS` bigint(20) DEFAULT NULL,
  `BIT_VECTOR` blob,
  `AVG_COL_LEN` double(53,4) DEFAULT NULL,
  `MAX_COL_LEN` bigint(20) DEFAULT NULL,
  `NUM_TRUES` bigint(20) DEFAULT NULL,
  `NUM_FALSES` bigint(20) DEFAULT NULL,
  `LAST_ANALYZED` bigint(20) NOT NULL,
  PRIMARY KEY (`CS_ID`),
  KEY `PART_COL_STATS_FK` (`PART_ID`),
  KEY `PCS_STATS_IDX` (`CAT_NAME`,`DB_NAME`,`TABLE_NAME`,`COLUMN_NAME`,`PARTITION_NAME`) USING BTREE,
  CONSTRAINT `PART_COL_STATS_FK` FOREIGN KEY (`PART_ID`) REFERENCES `PARTITIONS` (`PART_ID`) ON DELETE CASCADE ON UPDATE RESTRICT
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `PART_COL_STATS`
--

LOCK TABLES `PART_COL_STATS` WRITE;
/*!40000 ALTER TABLE `PART_COL_STATS` DISABLE KEYS */;
/*!40000 ALTER TABLE `PART_COL_STATS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `PART_PRIVS`
--

DROP TABLE IF EXISTS `PART_PRIVS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `PART_PRIVS` (
  `PART_GRANT_ID` bigint(20) NOT NULL,
  `CREATE_TIME` int(11) NOT NULL,
  `GRANT_OPTION` smallint(6) NOT NULL,
  `GRANTOR` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `GRANTOR_TYPE` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `PART_ID` bigint(20) DEFAULT NULL,
  `PRINCIPAL_NAME` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `PRINCIPAL_TYPE` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `PART_PRIV` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  PRIMARY KEY (`PART_GRANT_ID`),
  KEY `PARTPRIVILEGEINDEX` (`PART_ID`,`PRINCIPAL_NAME`,`PRINCIPAL_TYPE`,`PART_PRIV`,`GRANTOR`,`GRANTOR_TYPE`),
  KEY `PART_PRIVS_N49` (`PART_ID`),
  CONSTRAINT `PART_PRIVS_FK1` FOREIGN KEY (`PART_ID`) REFERENCES `PARTITIONS` (`PART_ID`) ON DELETE CASCADE ON UPDATE RESTRICT
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `PART_PRIVS`
--

LOCK TABLES `PART_PRIVS` WRITE;
/*!40000 ALTER TABLE `PART_PRIVS` DISABLE KEYS */;
/*!40000 ALTER TABLE `PART_PRIVS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `REPL_TXN_MAP`
--

DROP TABLE IF EXISTS `REPL_TXN_MAP`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `REPL_TXN_MAP` (
  `RTM_REPL_POLICY` varchar(256) NOT NULL,
  `RTM_SRC_TXN_ID` bigint(20) NOT NULL,
  `RTM_TARGET_TXN_ID` bigint(20) NOT NULL,
  PRIMARY KEY (`RTM_REPL_POLICY`,`RTM_SRC_TXN_ID`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `REPL_TXN_MAP`
--

LOCK TABLES `REPL_TXN_MAP` WRITE;
/*!40000 ALTER TABLE `REPL_TXN_MAP` DISABLE KEYS */;
/*!40000 ALTER TABLE `REPL_TXN_MAP` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `ROLES`
--

DROP TABLE IF EXISTS `ROLES`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ROLES` (
  `ROLE_ID` bigint(20) NOT NULL,
  `CREATE_TIME` int(11) NOT NULL,
  `OWNER_NAME` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `ROLE_NAME` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  PRIMARY KEY (`ROLE_ID`),
  UNIQUE KEY `ROLEENTITYINDEX` (`ROLE_NAME`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ROLES`
--

LOCK TABLES `ROLES` WRITE;
/*!40000 ALTER TABLE `ROLES` DISABLE KEYS */;
INSERT INTO `ROLES` VALUES (1,1564949108,'admin','admin'),(2,1564949108,'public','public');
/*!40000 ALTER TABLE `ROLES` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `ROLE_MAP`
--

DROP TABLE IF EXISTS `ROLE_MAP`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ROLE_MAP` (
  `ROLE_GRANT_ID` bigint(20) NOT NULL,
  `ADD_TIME` int(11) NOT NULL,
  `GRANT_OPTION` smallint(6) NOT NULL,
  `GRANTOR` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `GRANTOR_TYPE` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `PRINCIPAL_NAME` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `PRINCIPAL_TYPE` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `ROLE_ID` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`ROLE_GRANT_ID`),
  UNIQUE KEY `USERROLEMAPINDEX` (`PRINCIPAL_NAME`,`ROLE_ID`,`GRANTOR`,`GRANTOR_TYPE`),
  KEY `ROLE_MAP_N49` (`ROLE_ID`),
  CONSTRAINT `ROLE_MAP_FK1` FOREIGN KEY (`ROLE_ID`) REFERENCES `ROLES` (`ROLE_ID`) ON DELETE CASCADE ON UPDATE RESTRICT
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ROLE_MAP`
--

LOCK TABLES `ROLE_MAP` WRITE;
/*!40000 ALTER TABLE `ROLE_MAP` DISABLE KEYS */;
/*!40000 ALTER TABLE `ROLE_MAP` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `RUNTIME_STATS`
--

DROP TABLE IF EXISTS `RUNTIME_STATS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `RUNTIME_STATS` (
  `RS_ID` bigint(20) NOT NULL,
  `CREATE_TIME` bigint(20) NOT NULL,
  `WEIGHT` bigint(20) NOT NULL,
  `PAYLOAD` blob,
  PRIMARY KEY (`RS_ID`),
  KEY `IDX_RUNTIME_STATS_CREATE_TIME` (`CREATE_TIME`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `RUNTIME_STATS`
--

LOCK TABLES `RUNTIME_STATS` WRITE;
/*!40000 ALTER TABLE `RUNTIME_STATS` DISABLE KEYS */;
/*!40000 ALTER TABLE `RUNTIME_STATS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `SCHEMA_VERSION`
--

DROP TABLE IF EXISTS `SCHEMA_VERSION`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `SCHEMA_VERSION` (
  `SCHEMA_VERSION_ID` bigint(20) NOT NULL,
  `SCHEMA_ID` bigint(20) DEFAULT NULL,
  `VERSION` int(11) NOT NULL,
  `CREATED_AT` bigint(20) NOT NULL,
  `CD_ID` bigint(20) DEFAULT NULL,
  `STATE` int(11) NOT NULL,
  `DESCRIPTION` varchar(4000) DEFAULT NULL,
  `SCHEMA_TEXT` mediumtext,
  `FINGERPRINT` varchar(256) DEFAULT NULL,
  `SCHEMA_VERSION_NAME` varchar(256) DEFAULT NULL,
  `SERDE_ID` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`SCHEMA_VERSION_ID`),
  KEY `CD_ID` (`CD_ID`),
  KEY `SERDE_ID` (`SERDE_ID`),
  KEY `UNIQUE_VERSION` (`SCHEMA_ID`,`VERSION`),
  CONSTRAINT `FK_329_618` FOREIGN KEY (`CD_ID`) REFERENCES `CDS` (`CD_ID`) ON DELETE CASCADE ON UPDATE RESTRICT,
  CONSTRAINT `FK_376_619` FOREIGN KEY (`SERDE_ID`) REFERENCES `SERDES` (`SERDE_ID`) ON DELETE CASCADE ON UPDATE RESTRICT,
  CONSTRAINT `FK_610_620` FOREIGN KEY (`SCHEMA_ID`) REFERENCES `I_SCHEMA` (`SCHEMA_ID`) ON DELETE CASCADE ON UPDATE RESTRICT
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `SCHEMA_VERSION`
--

LOCK TABLES `SCHEMA_VERSION` WRITE;
/*!40000 ALTER TABLE `SCHEMA_VERSION` DISABLE KEYS */;
/*!40000 ALTER TABLE `SCHEMA_VERSION` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `SDS`
--

DROP TABLE IF EXISTS `SDS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `SDS` (
  `SD_ID` bigint(20) NOT NULL,
  `CD_ID` bigint(20) DEFAULT NULL,
  `INPUT_FORMAT` varchar(4000) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `IS_COMPRESSED` bit(1) NOT NULL,
  `IS_STOREDASSUBDIRECTORIES` bit(1) NOT NULL,
  `LOCATION` varchar(4000) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `NUM_BUCKETS` int(11) NOT NULL,
  `OUTPUT_FORMAT` varchar(4000) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `SERDE_ID` bigint(20) DEFAULT NULL,
  `NAME` varchar(255) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `PARENT_ID` bigint(20) DEFAULT NULL,
  `PARTITION_ID` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`SD_ID`),
  KEY `SDS_N49` (`SERDE_ID`),
  KEY `SDS_N50` (`CD_ID`),
  KEY `INODE_SDS_FK` (`PARTITION_ID`,`PARENT_ID`,`NAME`),
  CONSTRAINT `INODE_SDS_FK` FOREIGN KEY (`PARTITION_ID`,`PARENT_ID`,`NAME`) REFERENCES `hops`.`hdfs_inodes` (`partition_id`,`parent_id`,`name`) ON DELETE CASCADE ON UPDATE NO ACTION,
  CONSTRAINT `SDS_FK2` FOREIGN KEY (`CD_ID`) REFERENCES `CDS` (`CD_ID`) ON DELETE CASCADE ON UPDATE RESTRICT,
  CONSTRAINT `SDS_FK1` FOREIGN KEY (`SERDE_ID`) REFERENCES `SERDES` (`SERDE_ID`) ON DELETE CASCADE ON UPDATE RESTRICT
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `SDS`
--

LOCK TABLES `SDS` WRITE;
/*!40000 ALTER TABLE `SDS` DISABLE KEYS */;
INSERT INTO `SDS` VALUES (16,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95864',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',13,'zip=95864',300027,300027),(1,NULL,NULL,_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse',1,NULL,NULL,'warehouse',100220,100220),(12,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95677',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',9,'zip=95677',300027,300027),(8,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95690',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',5,'zip=95690',300027,300027),(25,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95821',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',22,'zip=95821',300027,300027),(52,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95630',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',49,'zip=95630',300027,300027),(58,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95814',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',55,'zip=95814',300027,300027),(41,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95833',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',38,'zip=95833',300027,300027),(46,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95765',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',43,'zip=95765',300027,300027),(3,NULL,NULL,_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db',1,NULL,NULL,'fabio.db',100221,100221),(67,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95693',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',64,'zip=95693',300027,300027),(5,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',2,'sacramento_properties',300017,300017),(7,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95662',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',4,'zip=95662',300027,300027),(32,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95815',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',29,'zip=95815',300027,300027),(44,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95811',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',41,'zip=95811',300027,300027),(63,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95746',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',60,'zip=95746',300027,300027),(28,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95631',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',25,'zip=95631',300027,300027),(56,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95841',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',53,'zip=95841',300027,300027),(69,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95747',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',66,'zip=95747',300027,300027),(36,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95762',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',33,'zip=95762',300027,300027),(74,NULL,NULL,_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/flinking.db',1,NULL,NULL,'flinking.db',100221,100221),(77,NULL,NULL,_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/beam_meetup_featurestore.db',1,NULL,NULL,'beam_meetup_featurestore.db',100221,100221),(11,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95757',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',8,'zip=95757',300027,300027),(39,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95614',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',36,'zip=95614',300027,300027),(62,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95628',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',59,'zip=95628',300027,300027),(21,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95626',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',18,'zip=95626',300027,300027),(45,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95624',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',42,'zip=95624',300027,300027),(54,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95820',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',51,'zip=95820',300027,300027),(31,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95619',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',28,'zip=95619',300027,300027),(55,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95722',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',52,'zip=95722',300027,300027),(50,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95621',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',47,'zip=95621',300027,300027),(66,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95835',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',63,'zip=95835',300027,300027),(10,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95825',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',7,'zip=95825',300027,300027),(23,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95610',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',20,'zip=95610',300027,300027),(86,11,'org.apache.hadoop.mapred.TextInputFormat',_binary '\0',_binary '','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties_skewed',-1,'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',76,'sacramento_properties_skewed',300017,300017),(14,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95819',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',11,'zip=95819',300027,300027),(35,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95661',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',32,'zip=95661',300027,300027),(37,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95667',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',34,'zip=95667',300027,300027),(51,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95648',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',48,'zip=95648',300027,300027),(47,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95838',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',44,'zip=95838',300027,300027),(71,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95823',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',68,'zip=95823',300027,300027),(30,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95655',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',27,'zip=95655',300027,300027),(64,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95633',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',61,'zip=95633',300027,300027),(65,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95826',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',62,'zip=95826',300027,300027),(42,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95632',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',39,'zip=95632',300027,300027),(4,1,'org.apache.hadoop.mapred.TextInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/Projects/fabio/RawData',-1,'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',1,'RawData',300004,300004),(68,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95818',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',65,'zip=95818',300027,300027),(20,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95650',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',17,'zip=95650',300027,300027),(18,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95635',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',15,'zip=95635',300027,300027),(75,NULL,NULL,_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/flinking_featurestore.db',1,NULL,NULL,'flinking_featurestore.db',100221,100221),(17,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95834',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',14,'zip=95834',300027,300027),(38,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95678',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',35,'zip=95678',300027,300027),(34,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95673',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',31,'zip=95673',300027,300027),(72,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95842',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',69,'zip=95842',300027,300027),(15,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95742',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',12,'zip=95742',300027,300027),(29,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95816',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',26,'zip=95816',300027,300027),(9,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95843',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',6,'zip=95843',300027,300027),(13,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95683',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',10,'zip=95683',300027,300027),(2,NULL,NULL,_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse',1,NULL,NULL,'warehouse',100220,100220),(49,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95829',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',46,'zip=95829',300027,300027),(6,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95828',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',3,'zip=95828',300027,300027),(24,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95827',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',21,'zip=95827',300027,300027),(19,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95831',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',16,'zip=95831',300027,300027),(22,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95691',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',19,'zip=95691',300027,300027),(33,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95682',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',30,'zip=95682',300027,300027),(60,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95663',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',57,'zip=95663',300027,300027),(43,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95608',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',40,'zip=95608',300027,300027),(26,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95726',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',23,'zip=95726',300027,300027),(70,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95670',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',67,'zip=95670',300027,300027),(73,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95623',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',70,'zip=95623',300027,300027),(48,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95603',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',45,'zip=95603',300027,300027),(53,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95822',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',50,'zip=95822',300027,300027),(61,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95758',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',58,'zip=95758',300027,300027),(76,NULL,NULL,_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/beam_meetup.db',1,NULL,NULL,'beam_meetup.db',100221,100221),(27,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95660',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',24,'zip=95660',300027,300027),(40,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95832',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',37,'zip=95832',300027,300027),(57,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95817',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',54,'zip=95817',300027,300027),(59,2,'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',_binary '\0',_binary '\0','hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties/zip=95824',-1,'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',56,'zip=95824',300027,300027);
/*!40000 ALTER TABLE `SDS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `SD_PARAMS`
--

DROP TABLE IF EXISTS `SD_PARAMS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `SD_PARAMS` (
  `SD_ID` bigint(20) NOT NULL,
  `PARAM_KEY` varchar(256) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `PARAM_VALUE` mediumtext CHARACTER SET latin1 COLLATE latin1_general_cs,
  PRIMARY KEY (`SD_ID`,`PARAM_KEY`),
  KEY `SD_PARAMS_N49` (`SD_ID`),
  CONSTRAINT `SD_PARAMS_FK1` FOREIGN KEY (`SD_ID`) REFERENCES `SDS` (`SD_ID`) ON DELETE CASCADE ON UPDATE RESTRICT
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `SD_PARAMS`
--

LOCK TABLES `SD_PARAMS` WRITE;
/*!40000 ALTER TABLE `SD_PARAMS` DISABLE KEYS */;
/*!40000 ALTER TABLE `SD_PARAMS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `SEQUENCE_TABLE`
--

DROP TABLE IF EXISTS `SEQUENCE_TABLE`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `SEQUENCE_TABLE` (
  `SEQUENCE_NAME` varchar(255) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `NEXT_VAL` bigint(20) NOT NULL,
  PRIMARY KEY (`SEQUENCE_NAME`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `SEQUENCE_TABLE`
--

LOCK TABLES `SEQUENCE_TABLE` WRITE;
/*!40000 ALTER TABLE `SEQUENCE_TABLE` DISABLE KEYS */;
INSERT INTO `SEQUENCE_TABLE` VALUES ('org.apache.hadoop.hive.metastore.model.MDatabase',11),('org.apache.hadoop.hive.metastore.model.MRole',6),('org.apache.hadoop.hive.metastore.model.MStringList',96),('org.apache.hadoop.hive.metastore.model.MCatalog',6),('org.apache.hadoop.hive.metastore.model.MGlobalPrivilege',6),('org.apache.hadoop.hive.metastore.model.MTable',16),('org.apache.hadoop.hive.metastore.model.MPartition',71),('org.apache.hadoop.hive.metastore.model.MSerDeInfo',81),('org.apache.hadoop.hive.metastore.model.MColumnDescriptor',16),('org.apache.hadoop.hive.metastore.model.MStorageDescriptor',91),('org.apache.hadoop.hive.metastore.model.MNotificationLog',1);
/*!40000 ALTER TABLE `SEQUENCE_TABLE` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `SERDES`
--

DROP TABLE IF EXISTS `SERDES`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `SERDES` (
  `SERDE_ID` bigint(20) NOT NULL,
  `NAME` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `SLIB` varchar(4000) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `DESCRIPTION` varchar(2000) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `SERIALIZER_CLASS` varchar(2000) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `DESERIALIZER_CLASS` varchar(2000) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `SERDE_TYPE` int(11) DEFAULT NULL,
  PRIMARY KEY (`SERDE_ID`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `SERDES`
--

LOCK TABLES `SERDES` WRITE;
/*!40000 ALTER TABLE `SERDES` DISABLE KEYS */;
INSERT INTO `SERDES` VALUES (16,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(1,NULL,'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',NULL,NULL,NULL,0),(12,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(8,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(25,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(52,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(58,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(41,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(46,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(3,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(67,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(5,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(7,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(32,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(44,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(63,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(28,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(56,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(69,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(36,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(11,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(39,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(62,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(21,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(45,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(54,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(31,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(55,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(50,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(66,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(10,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(23,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(14,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(35,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(37,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(51,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(47,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(30,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(64,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(65,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(42,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(4,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(68,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(20,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(18,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(17,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(38,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(34,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(15,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(29,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(9,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(13,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(2,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(49,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(6,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(24,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(19,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(22,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(33,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(60,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(43,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(26,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(70,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(48,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(53,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(61,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(76,NULL,'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',NULL,NULL,NULL,0),(27,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(40,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(57,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0),(59,NULL,'org.apache.hadoop.hive.ql.io.orc.OrcSerde',NULL,NULL,NULL,0);
/*!40000 ALTER TABLE `SERDES` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `SERDE_PARAMS`
--

DROP TABLE IF EXISTS `SERDE_PARAMS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `SERDE_PARAMS` (
  `SERDE_ID` bigint(20) NOT NULL,
  `PARAM_KEY` varchar(256) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `PARAM_VALUE` mediumtext CHARACTER SET latin1 COLLATE latin1_general_cs,
  PRIMARY KEY (`SERDE_ID`,`PARAM_KEY`),
  KEY `SERDE_PARAMS_N49` (`SERDE_ID`),
  CONSTRAINT `SERDE_PARAMS_FK2` FOREIGN KEY (`SERDE_ID`) REFERENCES `SERDES` (`SERDE_ID`) ON DELETE CASCADE ON UPDATE RESTRICT
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `SERDE_PARAMS`
--

LOCK TABLES `SERDE_PARAMS` WRITE;
/*!40000 ALTER TABLE `SERDE_PARAMS` DISABLE KEYS */;
INSERT INTO `SERDE_PARAMS` VALUES (26,'serialization.format','1'),(29,'serialization.format','1'),(21,'serialization.format','1'),(56,'serialization.format','1'),(18,'serialization.format','1'),(34,'serialization.format','1'),(45,'serialization.format','1'),(6,'serialization.format','1'),(53,'serialization.format','1'),(10,'serialization.format','1'),(41,'serialization.format','1'),(67,'serialization.format','1'),(47,'serialization.format','1'),(28,'serialization.format','1'),(11,'serialization.format','1'),(30,'serialization.format','1'),(22,'serialization.format','1'),(64,'serialization.format','1'),(12,'serialization.format','1'),(9,'serialization.format','1'),(13,'serialization.format','1'),(3,'serialization.format','1'),(16,'serialization.format','1'),(4,'serialization.format','1'),(20,'serialization.format','1'),(65,'serialization.format','1'),(8,'serialization.format','1'),(39,'serialization.format','1'),(25,'serialization.format','1'),(58,'serialization.format','1'),(52,'serialization.format','1'),(23,'serialization.format','1'),(44,'serialization.format','1'),(57,'serialization.format','1'),(59,'serialization.format','1'),(17,'serialization.format','1'),(46,'serialization.format','1'),(48,'serialization.format','1'),(63,'serialization.format','1'),(7,'serialization.format','1'),(54,'serialization.format','1'),(1,'field.delim',','),(27,'serialization.format','1'),(32,'serialization.format','1'),(36,'serialization.format','1'),(55,'serialization.format','1'),(76,'serialization.format','1'),(40,'serialization.format','1'),(43,'serialization.format','1'),(2,'serialization.format','1'),(31,'serialization.format','1'),(60,'serialization.format','1'),(61,'serialization.format','1'),(62,'serialization.format','1'),(68,'serialization.format','1'),(42,'serialization.format','1'),(49,'serialization.format','1'),(70,'serialization.format','1'),(14,'serialization.format','1'),(24,'serialization.format','1'),(38,'serialization.format','1'),(50,'serialization.format','1'),(51,'serialization.format','1'),(5,'serialization.format','1'),(33,'serialization.format','1'),(1,'serialization.format',','),(35,'serialization.format','1'),(37,'serialization.format','1'),(69,'serialization.format','1'),(15,'serialization.format','1'),(19,'serialization.format','1'),(66,'serialization.format','1');
/*!40000 ALTER TABLE `SERDE_PARAMS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `SKEWED_COL_NAMES`
--

DROP TABLE IF EXISTS `SKEWED_COL_NAMES`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `SKEWED_COL_NAMES` (
  `SD_ID` bigint(20) NOT NULL,
  `SKEWED_COL_NAME` varchar(256) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `INTEGER_IDX` int(11) NOT NULL,
  PRIMARY KEY (`SD_ID`,`INTEGER_IDX`),
  KEY `SKEWED_COL_NAMES_N49` (`SD_ID`),
  CONSTRAINT `SKEWED_COL_NAMES_FK1` FOREIGN KEY (`SD_ID`) REFERENCES `SDS` (`SD_ID`) ON DELETE CASCADE ON UPDATE RESTRICT
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `SKEWED_COL_NAMES`
--

LOCK TABLES `SKEWED_COL_NAMES` WRITE;
/*!40000 ALTER TABLE `SKEWED_COL_NAMES` DISABLE KEYS */;
INSERT INTO `SKEWED_COL_NAMES` VALUES (86,'zip',0),(86,'price',1);
/*!40000 ALTER TABLE `SKEWED_COL_NAMES` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `SKEWED_COL_VALUE_LOC_MAP`
--

DROP TABLE IF EXISTS `SKEWED_COL_VALUE_LOC_MAP`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `SKEWED_COL_VALUE_LOC_MAP` (
  `SD_ID` bigint(20) NOT NULL,
  `STRING_LIST_ID_KID` bigint(20) NOT NULL,
  `LOCATION` varchar(4000) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  PRIMARY KEY (`SD_ID`,`STRING_LIST_ID_KID`),
  KEY `SKEWED_COL_VALUE_LOC_MAP_N49` (`STRING_LIST_ID_KID`),
  KEY `SKEWED_COL_VALUE_LOC_MAP_N50` (`SD_ID`),
  CONSTRAINT `SKEWED_COL_VALUE_LOC_MAP_FK1` FOREIGN KEY (`SD_ID`) REFERENCES `SDS` (`SD_ID`) ON DELETE CASCADE ON UPDATE RESTRICT,
  CONSTRAINT `SKEWED_COL_VALUE_LOC_MAP_FK2` FOREIGN KEY (`STRING_LIST_ID_KID`) REFERENCES `SKEWED_STRING_LIST` (`STRING_LIST_ID`) ON DELETE CASCADE ON UPDATE RESTRICT
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `SKEWED_COL_VALUE_LOC_MAP`
--

LOCK TABLES `SKEWED_COL_VALUE_LOC_MAP` WRITE;
/*!40000 ALTER TABLE `SKEWED_COL_VALUE_LOC_MAP` DISABLE KEYS */;
INSERT INTO `SKEWED_COL_VALUE_LOC_MAP` VALUES (86,94,'hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties_skewed/zip=95603/price=285000.0'),(86,91,'hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties_skewed/zip=95603/price=420454.0'),(86,92,'hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties_skewed/zip=95603/price=260000.0'),(86,93,'hdfs://10.0.2.15:8020/apps/hive/warehouse/fabio.db/sacramento_properties_skewed/zip=95603/price=504000.0');
/*!40000 ALTER TABLE `SKEWED_COL_VALUE_LOC_MAP` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `SKEWED_STRING_LIST`
--

DROP TABLE IF EXISTS `SKEWED_STRING_LIST`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `SKEWED_STRING_LIST` (
  `STRING_LIST_ID` bigint(20) NOT NULL,
  PRIMARY KEY (`STRING_LIST_ID`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `SKEWED_STRING_LIST`
--

LOCK TABLES `SKEWED_STRING_LIST` WRITE;
/*!40000 ALTER TABLE `SKEWED_STRING_LIST` DISABLE KEYS */;
INSERT INTO `SKEWED_STRING_LIST` VALUES (91),(93),(79),(89),(82),(92),(77),(85),(88),(81),(86),(90),(80),(83),(78),(75),(87),(84),(94),(76);
/*!40000 ALTER TABLE `SKEWED_STRING_LIST` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `SKEWED_STRING_LIST_VALUES`
--

DROP TABLE IF EXISTS `SKEWED_STRING_LIST_VALUES`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `SKEWED_STRING_LIST_VALUES` (
  `STRING_LIST_ID` bigint(20) NOT NULL,
  `STRING_LIST_VALUE` varchar(256) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `INTEGER_IDX` int(11) NOT NULL,
  PRIMARY KEY (`STRING_LIST_ID`,`INTEGER_IDX`),
  KEY `SKEWED_STRING_LIST_VALUES_N49` (`STRING_LIST_ID`),
  CONSTRAINT `SKEWED_STRING_LIST_VALUES_FK1` FOREIGN KEY (`STRING_LIST_ID`) REFERENCES `SKEWED_STRING_LIST` (`STRING_LIST_ID`) ON DELETE CASCADE ON UPDATE RESTRICT
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `SKEWED_STRING_LIST_VALUES`
--

LOCK TABLES `SKEWED_STRING_LIST_VALUES` WRITE;
/*!40000 ALTER TABLE `SKEWED_STRING_LIST_VALUES` DISABLE KEYS */;
INSERT INTO `SKEWED_STRING_LIST_VALUES` VALUES (77,'95603',0),(94,'285000.0',1),(77,'420454.0',1),(92,'95603',0),(90,'95603',0),(78,'504000.0',1),(75,'95603',0),(82,'504000.0',1),(85,'95603',0),(91,'95603',0),(81,'95603',0),(82,'95603',0),(91,'420454.0',1),(75,'260000.0',1),(80,'95603',0),(83,'260000.0',1),(86,'95603',0),(87,'260000.0',1),(89,'95603',0),(88,'95603',0),(80,'285000.0',1),(83,'95603',0),(93,'95603',0),(84,'285000.0',1),(87,'95603',0),(81,'420454.0',1),(85,'420454.0',1),(93,'504000.0',1),(88,'285000.0',1),(79,'260000.0',1),(84,'95603',0),(78,'95603',0),(90,'504000.0',1),(92,'260000.0',1),(79,'95603',0),(86,'504000.0',1),(94,'95603',0),(76,'95603',0),(76,'285000.0',1),(89,'420454.0',1);
/*!40000 ALTER TABLE `SKEWED_STRING_LIST_VALUES` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `SKEWED_VALUES`
--

DROP TABLE IF EXISTS `SKEWED_VALUES`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `SKEWED_VALUES` (
  `SD_ID_OID` bigint(20) NOT NULL,
  `STRING_LIST_ID_EID` bigint(20) NOT NULL,
  `INTEGER_IDX` int(11) NOT NULL,
  PRIMARY KEY (`SD_ID_OID`,`INTEGER_IDX`),
  KEY `SKEWED_VALUES_N50` (`SD_ID_OID`),
  KEY `SKEWED_VALUES_N49` (`STRING_LIST_ID_EID`),
  CONSTRAINT `SKEWED_VALUES_FK1` FOREIGN KEY (`SD_ID_OID`) REFERENCES `SDS` (`SD_ID`) ON DELETE CASCADE ON UPDATE RESTRICT,
  CONSTRAINT `SKEWED_VALUES_FK2` FOREIGN KEY (`STRING_LIST_ID_EID`) REFERENCES `SKEWED_STRING_LIST` (`STRING_LIST_ID`) ON DELETE CASCADE ON UPDATE RESTRICT
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `SKEWED_VALUES`
--

LOCK TABLES `SKEWED_VALUES` WRITE;
/*!40000 ALTER TABLE `SKEWED_VALUES` DISABLE KEYS */;
INSERT INTO `SKEWED_VALUES` VALUES (86,87,0),(86,88,1),(86,89,2),(86,90,3);
/*!40000 ALTER TABLE `SKEWED_VALUES` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `SORT_COLS`
--

DROP TABLE IF EXISTS `SORT_COLS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `SORT_COLS` (
  `SD_ID` bigint(20) NOT NULL,
  `COLUMN_NAME` varchar(767) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `ORDER` int(11) NOT NULL,
  `INTEGER_IDX` int(11) NOT NULL,
  PRIMARY KEY (`SD_ID`,`INTEGER_IDX`),
  KEY `SORT_COLS_N49` (`SD_ID`),
  CONSTRAINT `SORT_COLS_FK1` FOREIGN KEY (`SD_ID`) REFERENCES `SDS` (`SD_ID`) ON DELETE CASCADE ON UPDATE RESTRICT
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `SORT_COLS`
--

LOCK TABLES `SORT_COLS` WRITE;
/*!40000 ALTER TABLE `SORT_COLS` DISABLE KEYS */;
/*!40000 ALTER TABLE `SORT_COLS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `TABLE_PARAMS`
--

DROP TABLE IF EXISTS `TABLE_PARAMS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `TABLE_PARAMS` (
  `TBL_ID` bigint(20) NOT NULL,
  `PARAM_KEY` varchar(256) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `PARAM_VALUE` mediumtext CHARACTER SET latin1 COLLATE latin1_general_cs,
  PRIMARY KEY (`TBL_ID`,`PARAM_KEY`),
  KEY `TABLE_PARAMS_N49` (`TBL_ID`),
  CONSTRAINT `TABLE_PARAMS_FK1` FOREIGN KEY (`TBL_ID`) REFERENCES `TBLS` (`TBL_ID`) ON DELETE CASCADE ON UPDATE RESTRICT
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `TABLE_PARAMS`
--

LOCK TABLES `TABLE_PARAMS` WRITE;
/*!40000 ALTER TABLE `TABLE_PARAMS` DISABLE KEYS */;
INSERT INTO `TABLE_PARAMS` VALUES (11,'transient_lastDdlTime','1565012453'),(2,'transient_lastDdlTime','1564997115'),(11,'numFiles','5'),(1,'transient_lastDdlTime','1564997100'),(1,'totalSize','113103'),(11,'numRows','985'),(11,'totalSize','115996'),(1,'bucketing_version','2'),(2,'bucketing_version','2'),(11,'COLUMN_STATS_ACCURATE','{\"BASIC_STATS\":\"true\"}'),(11,'bucketing_version','2'),(1,'EXTERNAL','TRUE'),(11,'rawDataSize','115011'),(1,'numFiles','1');
/*!40000 ALTER TABLE `TABLE_PARAMS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `TAB_COL_STATS`
--

DROP TABLE IF EXISTS `TAB_COL_STATS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `TAB_COL_STATS` (
  `CS_ID` bigint(20) NOT NULL,
  `CAT_NAME` varchar(256) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `DB_NAME` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `TABLE_NAME` varchar(256) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `COLUMN_NAME` varchar(767) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `COLUMN_TYPE` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `TBL_ID` bigint(20) NOT NULL,
  `LONG_LOW_VALUE` bigint(20) DEFAULT NULL,
  `LONG_HIGH_VALUE` bigint(20) DEFAULT NULL,
  `DOUBLE_HIGH_VALUE` double(53,4) DEFAULT NULL,
  `DOUBLE_LOW_VALUE` double(53,4) DEFAULT NULL,
  `BIG_DECIMAL_LOW_VALUE` varchar(4000) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `BIG_DECIMAL_HIGH_VALUE` varchar(4000) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `NUM_NULLS` bigint(20) NOT NULL,
  `NUM_DISTINCTS` bigint(20) DEFAULT NULL,
  `BIT_VECTOR` blob,
  `AVG_COL_LEN` double(53,4) DEFAULT NULL,
  `MAX_COL_LEN` bigint(20) DEFAULT NULL,
  `NUM_TRUES` bigint(20) DEFAULT NULL,
  `NUM_FALSES` bigint(20) DEFAULT NULL,
  `LAST_ANALYZED` bigint(20) NOT NULL,
  PRIMARY KEY (`CS_ID`),
  KEY `TAB_COL_STATS_FK` (`TBL_ID`),
  CONSTRAINT `TAB_COL_STATS_FK` FOREIGN KEY (`TBL_ID`) REFERENCES `TBLS` (`TBL_ID`) ON DELETE CASCADE ON UPDATE RESTRICT
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `TAB_COL_STATS`
--

LOCK TABLES `TAB_COL_STATS` WRITE;
/*!40000 ALTER TABLE `TAB_COL_STATS` DISABLE KEYS */;
/*!40000 ALTER TABLE `TAB_COL_STATS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `TBLS`
--

DROP TABLE IF EXISTS `TBLS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `TBLS` (
  `TBL_ID` bigint(20) NOT NULL,
  `CREATE_TIME` int(11) NOT NULL,
  `DB_ID` bigint(20) DEFAULT NULL,
  `LAST_ACCESS_TIME` int(11) NOT NULL,
  `OWNER` varchar(767) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `OWNER_TYPE` varchar(10) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `RETENTION` int(11) NOT NULL,
  `SD_ID` bigint(20) DEFAULT NULL,
  `TBL_NAME` varchar(256) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `TBL_TYPE` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `VIEW_EXPANDED_TEXT` mediumtext,
  `VIEW_ORIGINAL_TEXT` mediumtext,
  `IS_REWRITE_ENABLED` bit(1) NOT NULL DEFAULT b'0',
  PRIMARY KEY (`TBL_ID`),
  UNIQUE KEY `UNIQUETABLE` (`TBL_NAME`,`DB_ID`),
  KEY `TBLS_N50` (`SD_ID`),
  KEY `TBLS_N49` (`DB_ID`),
  CONSTRAINT `TBLS_FK2` FOREIGN KEY (`DB_ID`) REFERENCES `DBS` (`DB_ID`) ON DELETE CASCADE ON UPDATE RESTRICT,
  CONSTRAINT `TBLS_FK1` FOREIGN KEY (`SD_ID`) REFERENCES `SDS` (`SD_ID`) ON DELETE CASCADE ON UPDATE RESTRICT
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `TBLS`
--

LOCK TABLES `TBLS` WRITE;
/*!40000 ALTER TABLE `TBLS` DISABLE KEYS */;
INSERT INTO `TBLS` VALUES (1,1564997100,2,0,'fabio__meb10000','USER',0,4,'sacramento_properties_ext','EXTERNAL_TABLE',NULL,NULL,_binary '\0'),(11,1565012434,2,0,'fabio__meb10000','USER',0,86,'sacramento_properties_skewed','MANAGED_TABLE',NULL,NULL,_binary '\0'),(2,1564997115,2,0,'fabio__meb10000','USER',0,5,'sacramento_properties','MANAGED_TABLE',NULL,NULL,_binary '\0');
/*!40000 ALTER TABLE `TBLS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `TBL_COL_PRIVS`
--

DROP TABLE IF EXISTS `TBL_COL_PRIVS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `TBL_COL_PRIVS` (
  `TBL_COLUMN_GRANT_ID` bigint(20) NOT NULL,
  `COLUMN_NAME` varchar(767) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `CREATE_TIME` int(11) NOT NULL,
  `GRANT_OPTION` smallint(6) NOT NULL,
  `GRANTOR` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `GRANTOR_TYPE` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `PRINCIPAL_NAME` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `PRINCIPAL_TYPE` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `TBL_COL_PRIV` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `TBL_ID` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`TBL_COLUMN_GRANT_ID`),
  KEY `TABLECOLUMNPRIVILEGEINDEX` (`TBL_ID`,`COLUMN_NAME`,`PRINCIPAL_NAME`,`PRINCIPAL_TYPE`,`TBL_COL_PRIV`,`GRANTOR`,`GRANTOR_TYPE`),
  KEY `TBL_COL_PRIVS_N49` (`TBL_ID`),
  CONSTRAINT `TBL_COL_PRIVS_FK1` FOREIGN KEY (`TBL_ID`) REFERENCES `TBLS` (`TBL_ID`) ON DELETE CASCADE ON UPDATE RESTRICT
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `TBL_COL_PRIVS`
--

LOCK TABLES `TBL_COL_PRIVS` WRITE;
/*!40000 ALTER TABLE `TBL_COL_PRIVS` DISABLE KEYS */;
/*!40000 ALTER TABLE `TBL_COL_PRIVS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `TBL_PRIVS`
--

DROP TABLE IF EXISTS `TBL_PRIVS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `TBL_PRIVS` (
  `TBL_GRANT_ID` bigint(20) NOT NULL,
  `CREATE_TIME` int(11) NOT NULL,
  `GRANT_OPTION` smallint(6) NOT NULL,
  `GRANTOR` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `GRANTOR_TYPE` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `PRINCIPAL_NAME` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `PRINCIPAL_TYPE` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `TBL_PRIV` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `TBL_ID` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`TBL_GRANT_ID`),
  KEY `TBL_PRIVS_N49` (`TBL_ID`),
  KEY `TABLEPRIVILEGEINDEX` (`TBL_ID`,`PRINCIPAL_NAME`,`PRINCIPAL_TYPE`,`TBL_PRIV`,`GRANTOR`,`GRANTOR_TYPE`),
  CONSTRAINT `TBL_PRIVS_FK1` FOREIGN KEY (`TBL_ID`) REFERENCES `TBLS` (`TBL_ID`) ON DELETE CASCADE ON UPDATE RESTRICT
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `TBL_PRIVS`
--

LOCK TABLES `TBL_PRIVS` WRITE;
/*!40000 ALTER TABLE `TBL_PRIVS` DISABLE KEYS */;
/*!40000 ALTER TABLE `TBL_PRIVS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `TXNS`
--

DROP TABLE IF EXISTS `TXNS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `TXNS` (
  `TXN_ID` bigint(20) NOT NULL,
  `TXN_STATE` char(1) NOT NULL,
  `TXN_STARTED` bigint(20) NOT NULL,
  `TXN_LAST_HEARTBEAT` bigint(20) NOT NULL,
  `TXN_USER` varchar(128) NOT NULL,
  `TXN_HOST` varchar(128) NOT NULL,
  `TXN_AGENT_INFO` varchar(128) DEFAULT NULL,
  `TXN_META_INFO` varchar(128) DEFAULT NULL,
  `TXN_HEARTBEAT_COUNT` int(11) DEFAULT NULL,
  PRIMARY KEY (`TXN_ID`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `TXNS`
--

LOCK TABLES `TXNS` WRITE;
/*!40000 ALTER TABLE `TXNS` DISABLE KEYS */;
/*!40000 ALTER TABLE `TXNS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `TXN_COMPONENTS`
--

DROP TABLE IF EXISTS `TXN_COMPONENTS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `TXN_COMPONENTS` (
  `TC_TXNID` bigint(20) NOT NULL,
  `TC_DATABASE` varchar(128) NOT NULL,
  `TC_TABLE` varchar(128) DEFAULT NULL,
  `TC_PARTITION` varchar(767) DEFAULT NULL,
  `TC_OPERATION_TYPE` char(1) NOT NULL,
  `TC_WRITEID` bigint(20) DEFAULT NULL,
  KEY `TC_TXNID_INDEX` (`TC_TXNID`),
  CONSTRAINT `FK_580_583` FOREIGN KEY (`TC_TXNID`) REFERENCES `TXNS` (`TXN_ID`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `TXN_COMPONENTS`
--

LOCK TABLES `TXN_COMPONENTS` WRITE;
/*!40000 ALTER TABLE `TXN_COMPONENTS` DISABLE KEYS */;
/*!40000 ALTER TABLE `TXN_COMPONENTS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `TXN_TO_WRITE_ID`
--

DROP TABLE IF EXISTS `TXN_TO_WRITE_ID`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `TXN_TO_WRITE_ID` (
  `T2W_TXNID` bigint(20) NOT NULL,
  `T2W_DATABASE` varchar(128) NOT NULL,
  `T2W_TABLE` varchar(256) NOT NULL,
  `T2W_WRITEID` bigint(20) NOT NULL,
  UNIQUE KEY `TBL_TO_TXN_ID_IDX` (`T2W_DATABASE`,`T2W_TABLE`,`T2W_TXNID`),
  UNIQUE KEY `TBL_TO_WRITE_ID_IDX` (`T2W_DATABASE`,`T2W_TABLE`,`T2W_WRITEID`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `TXN_TO_WRITE_ID`
--

LOCK TABLES `TXN_TO_WRITE_ID` WRITE;
/*!40000 ALTER TABLE `TXN_TO_WRITE_ID` DISABLE KEYS */;
/*!40000 ALTER TABLE `TXN_TO_WRITE_ID` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `TYPES`
--

DROP TABLE IF EXISTS `TYPES`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `TYPES` (
  `TYPES_ID` bigint(20) NOT NULL,
  `TYPE_NAME` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `TYPE1` varchar(767) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `TYPE2` varchar(767) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  PRIMARY KEY (`TYPES_ID`),
  UNIQUE KEY `UNIQUE_TYPE` (`TYPE_NAME`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `TYPES`
--

LOCK TABLES `TYPES` WRITE;
/*!40000 ALTER TABLE `TYPES` DISABLE KEYS */;
/*!40000 ALTER TABLE `TYPES` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `TYPE_FIELDS`
--

DROP TABLE IF EXISTS `TYPE_FIELDS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `TYPE_FIELDS` (
  `TYPE_NAME` bigint(20) NOT NULL,
  `COMMENT` varchar(256) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `FIELD_NAME` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `FIELD_TYPE` varchar(767) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `INTEGER_IDX` int(11) NOT NULL,
  PRIMARY KEY (`TYPE_NAME`,`FIELD_NAME`),
  KEY `TYPE_FIELDS_N49` (`TYPE_NAME`),
  CONSTRAINT `TYPE_FIELDS_FK1` FOREIGN KEY (`TYPE_NAME`) REFERENCES `TYPES` (`TYPES_ID`) ON DELETE CASCADE ON UPDATE RESTRICT
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `TYPE_FIELDS`
--

LOCK TABLES `TYPE_FIELDS` WRITE;
/*!40000 ALTER TABLE `TYPE_FIELDS` DISABLE KEYS */;
/*!40000 ALTER TABLE `TYPE_FIELDS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `VERSION`
--

DROP TABLE IF EXISTS `VERSION`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `VERSION` (
  `VER_ID` bigint(20) NOT NULL,
  `SCHEMA_VERSION` varchar(127) NOT NULL,
  `VERSION_COMMENT` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`VER_ID`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `VERSION`
--

LOCK TABLES `VERSION` WRITE;
/*!40000 ALTER TABLE `VERSION` DISABLE KEYS */;
INSERT INTO `VERSION` VALUES (1,'3.0.0.1','Hive release version 3.0.0.1');
/*!40000 ALTER TABLE `VERSION` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `WM_MAPPING`
--

DROP TABLE IF EXISTS `WM_MAPPING`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `WM_MAPPING` (
  `MAPPING_ID` bigint(20) NOT NULL,
  `RP_ID` bigint(20) NOT NULL,
  `ENTITY_TYPE` varchar(128) NOT NULL,
  `ENTITY_NAME` varchar(128) NOT NULL,
  `POOL_ID` bigint(20) DEFAULT NULL,
  `ORDERING` int(11) DEFAULT NULL,
  PRIMARY KEY (`MAPPING_ID`),
  UNIQUE KEY `UNIQUE_WM_MAPPING` (`RP_ID`,`ENTITY_TYPE`,`ENTITY_NAME`),
  KEY `WM_MAPPING_FK2` (`POOL_ID`),
  CONSTRAINT `WM_MAPPING_FK1` FOREIGN KEY (`RP_ID`) REFERENCES `WM_RESOURCEPLAN` (`RP_ID`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT `WM_MAPPING_FK2` FOREIGN KEY (`POOL_ID`) REFERENCES `WM_POOL` (`POOL_ID`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `WM_MAPPING`
--

LOCK TABLES `WM_MAPPING` WRITE;
/*!40000 ALTER TABLE `WM_MAPPING` DISABLE KEYS */;
/*!40000 ALTER TABLE `WM_MAPPING` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `WM_POOL`
--

DROP TABLE IF EXISTS `WM_POOL`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `WM_POOL` (
  `POOL_ID` bigint(20) NOT NULL,
  `RP_ID` bigint(20) NOT NULL,
  `PATH` varchar(767) NOT NULL,
  `ALLOC_FRACTION` double DEFAULT NULL,
  `QUERY_PARALLELISM` int(11) DEFAULT NULL,
  `SCHEDULING_POLICY` varchar(767) DEFAULT NULL,
  PRIMARY KEY (`POOL_ID`),
  UNIQUE KEY `UNIQUE_WM_POOL` (`RP_ID`,`PATH`),
  CONSTRAINT `WM_POOL_FK1` FOREIGN KEY (`RP_ID`) REFERENCES `WM_RESOURCEPLAN` (`RP_ID`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `WM_POOL`
--

LOCK TABLES `WM_POOL` WRITE;
/*!40000 ALTER TABLE `WM_POOL` DISABLE KEYS */;
/*!40000 ALTER TABLE `WM_POOL` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `WM_POOL_TO_TRIGGER`
--

DROP TABLE IF EXISTS `WM_POOL_TO_TRIGGER`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `WM_POOL_TO_TRIGGER` (
  `POOL_ID` bigint(20) NOT NULL,
  `TRIGGER_ID` bigint(20) NOT NULL,
  PRIMARY KEY (`POOL_ID`,`TRIGGER_ID`),
  KEY `WM_POOL_TO_TRIGGER_FK2` (`TRIGGER_ID`),
  CONSTRAINT `WM_POOL_TO_TRIGGER_FK1` FOREIGN KEY (`POOL_ID`) REFERENCES `WM_POOL` (`POOL_ID`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT `WM_POOL_TO_TRIGGER_FK2` FOREIGN KEY (`TRIGGER_ID`) REFERENCES `WM_TRIGGER` (`TRIGGER_ID`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `WM_POOL_TO_TRIGGER`
--

LOCK TABLES `WM_POOL_TO_TRIGGER` WRITE;
/*!40000 ALTER TABLE `WM_POOL_TO_TRIGGER` DISABLE KEYS */;
/*!40000 ALTER TABLE `WM_POOL_TO_TRIGGER` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `WM_RESOURCEPLAN`
--

DROP TABLE IF EXISTS `WM_RESOURCEPLAN`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `WM_RESOURCEPLAN` (
  `RP_ID` bigint(20) NOT NULL,
  `NAME` varchar(128) NOT NULL,
  `QUERY_PARALLELISM` int(11) DEFAULT NULL,
  `STATUS` varchar(20) NOT NULL,
  `DEFAULT_POOL_ID` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`RP_ID`),
  UNIQUE KEY `UNIQUE_WM_RESOURCEPLAN` (`NAME`),
  KEY `WM_RESOURCEPLAN_FK1` (`DEFAULT_POOL_ID`),
  CONSTRAINT `WM_RESOURCEPLAN_FK1` FOREIGN KEY (`DEFAULT_POOL_ID`) REFERENCES `WM_POOL` (`POOL_ID`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `WM_RESOURCEPLAN`
--

LOCK TABLES `WM_RESOURCEPLAN` WRITE;
/*!40000 ALTER TABLE `WM_RESOURCEPLAN` DISABLE KEYS */;
/*!40000 ALTER TABLE `WM_RESOURCEPLAN` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `WM_TRIGGER`
--

DROP TABLE IF EXISTS `WM_TRIGGER`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `WM_TRIGGER` (
  `TRIGGER_ID` bigint(20) NOT NULL,
  `RP_ID` bigint(20) NOT NULL,
  `NAME` varchar(128) NOT NULL,
  `TRIGGER_EXPRESSION` varchar(1024) DEFAULT NULL,
  `ACTION_EXPRESSION` varchar(1024) DEFAULT NULL,
  `IS_IN_UNMANAGED` bit(1) NOT NULL DEFAULT b'0',
  PRIMARY KEY (`TRIGGER_ID`),
  UNIQUE KEY `UNIQUE_WM_TRIGGER` (`RP_ID`,`NAME`),
  CONSTRAINT `WM_TRIGGER_FK1` FOREIGN KEY (`RP_ID`) REFERENCES `WM_RESOURCEPLAN` (`RP_ID`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `WM_TRIGGER`
--

LOCK TABLES `WM_TRIGGER` WRITE;
/*!40000 ALTER TABLE `WM_TRIGGER` DISABLE KEYS */;
/*!40000 ALTER TABLE `WM_TRIGGER` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `WRITE_SET`
--

DROP TABLE IF EXISTS `WRITE_SET`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `WRITE_SET` (
  `WS_DATABASE` varchar(128) NOT NULL,
  `WS_TABLE` varchar(128) NOT NULL,
  `WS_PARTITION` varchar(767) DEFAULT NULL,
  `WS_TXNID` bigint(20) NOT NULL,
  `WS_COMMIT_ID` bigint(20) NOT NULL,
  `WS_OPERATION_TYPE` char(1) NOT NULL
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `WRITE_SET`
--

LOCK TABLES `WRITE_SET` WRITE;
/*!40000 ALTER TABLE `WRITE_SET` DISABLE KEYS */;
/*!40000 ALTER TABLE `WRITE_SET` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2019-08-05 13:58:52
