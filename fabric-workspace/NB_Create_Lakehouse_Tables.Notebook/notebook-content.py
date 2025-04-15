# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%configure
# MAGIC {
# MAGIC     "defaultLakehouse": {  
# MAGIC         "name": "LH_Silver",
# MAGIC     }
# MAGIC }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC drop table if exists LH_Silver.sf_cons_opportunity;
# MAGIC drop table if exists LH_Silver.sf_cons_clienttype;
# MAGIC drop table if exists LH_Silver.sf_cons_profilecategory;
# MAGIC drop table if exists LH_Silver.sf_cons_account;
# MAGIC drop table if exists LH_Silver.sf_cons_strategic_subcategory;
# MAGIC drop table if exists LH_Silver.sf_cons_region;
# MAGIC drop table if exists LH_Silver.sf_cons_company;
# MAGIC drop table if exists LH_Silver.sf_cons_marketsegment;
# MAGIC drop table if exists LH_Silver.sf_cons_serviceline;
# MAGIC drop table if exists LH_Silver.sf_cons_exchangerate;
# MAGIC 
# MAGIC drop table if exists LH_Silver.sf_conf_opportunity;
# MAGIC drop table if exists LH_Silver.sf_conf_clienttype;
# MAGIC drop table if exists LH_Silver.sf_conf_company;
# MAGIC drop table if exists LH_Silver.sf_conf_marketsegment;
# MAGIC drop table if exists LH_Silver.sf_conf_account;
# MAGIC drop table if exists LH_Silver.sf_conf_region;
# MAGIC drop table if exists LH_Silver.sf_conf_serviceline;
# MAGIC drop table if exists LH_Silver.sf_conf_strategic_subcategory;
# MAGIC drop table if exists LH_Silver.sf_conf_profilecategory;
# MAGIC drop table if exists LH_Silver.sf_conf_exchangerate;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC drop table if exists LH_Gold.sf_conf_profilecategory;
# MAGIC drop table if exists LH_Gold.sf_conf_marketsegment;
# MAGIC drop table if exists LH_Gold.sf_conf_serviceline;
# MAGIC drop table if exists LH_Gold.sf_conf_strategic_subcategory;
# MAGIC drop table if exists LH_Gold.sf_conf_company;
# MAGIC drop table if exists LH_Gold.sf_conf_clienttype;
# MAGIC drop table if exists LH_Gold.sf_conf_opportunity;
# MAGIC drop table if exists LH_Gold.sf_conf_region;
# MAGIC drop table if exists LH_Gold.sf_conf_account;
# MAGIC 
# MAGIC drop table if exists LH_Gold.ghdregion;
# MAGIC drop table if exists LH_Gold.marketsegment;
# MAGIC drop table if exists LH_Gold.ghdcompany;
# MAGIC drop table if exists LH_Gold.profilecategory;
# MAGIC drop table if exists LH_Gold.serviceline;
# MAGIC drop table if exists LH_Gold.clienttype;
# MAGIC drop table if exists LH_Gold.opportunity;
# MAGIC drop table if exists LH_Gold.strategicsubcategory;
# MAGIC drop table if exists LH_Gold.exchangerate;
# MAGIC drop table if exists LH_Gold.client;
# MAGIC drop table if exists LH_Gold.fundingclient;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC --------------------------------------------------------------------------------------------
# MAGIC --Generating DDL Script for All tables in Lakehouse: LH_Silver
# MAGIC 
# MAGIC ----table_name: sf_conf_clienttype
# MAGIC ----tables: []
# MAGIC --Generating DDL Script for Table: sf_conf_clienttype
# MAGIC --__________________________________________________________________________
# MAGIC CREATE TABLE IF NOT EXISTS LH_Silver.sf_conf_clienttype(
# MAGIC     `IsDefault` BOOLEAN,
# MAGIC     `ClientTypeName` STRING,
# MAGIC     `ClientTypeCode` STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.minReaderVersion' = 2,
# MAGIC     'delta.minWriterVersion' = 5,
# MAGIC     'delta.columnMapping.mode' = 'name'
# MAGIC );
# MAGIC ----table_name: sf_conf_marketsegment
# MAGIC ----tables: []
# MAGIC --Generating DDL Script for Table: sf_conf_marketsegment
# MAGIC --__________________________________________________________________________
# MAGIC CREATE TABLE IF NOT EXISTS LH_Silver.sf_conf_marketsegment(
# MAGIC     `MarketSegmentCode` STRING,
# MAGIC     `MarketSegment` STRING,
# MAGIC     `MarketSubSegment` STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.minReaderVersion' = 2,
# MAGIC     'delta.minWriterVersion' = 5,
# MAGIC     'delta.columnMapping.mode' = 'name'
# MAGIC );
# MAGIC ----table_name: sf_conf_opportunity
# MAGIC ----tables: []
# MAGIC --Generating DDL Script for Table: sf_conf_opportunity
# MAGIC --__________________________________________________________________________
# MAGIC CREATE TABLE IF NOT EXISTS LH_Silver.sf_conf_opportunity(
# MAGIC     `OpportunityId` STRING,
# MAGIC     `AccountId` STRING,
# MAGIC     `OpportunityAmount` DECIMAL(18,2),
# MAGIC     `CapitalCost` DECIMAL(18,2),
# MAGIC     `EstimatedTotalCosttoClient` DECIMAL(18,2),
# MAGIC     `CloseDate` DATE,
# MAGIC     `ContractType` STRING,
# MAGIC     `CurrencyCode` STRING,
# MAGIC     `OpportunityDescription` STRING,
# MAGIC     `EstimatedDuration` STRING,
# MAGIC     `EstimatedGrossFee` DECIMAL(18,2),
# MAGIC     `ExpectedRevenue` DECIMAL(18,2),
# MAGIC     `FundingClientId` STRING,
# MAGIC     `FundingClientCode` STRING,
# MAGIC     `GetProbability` STRING,
# MAGIC     `GHDCompanyId` STRING,
# MAGIC     `GHDRegionId` STRING,
# MAGIC     `GoProbability` STRING,
# MAGIC     `IsConfidential` BOOLEAN,
# MAGIC     `LossReason` STRING,
# MAGIC     `LossReasonComments` STRING,
# MAGIC     `MarketSegmentCode` STRING,
# MAGIC     `OpportunityName` STRING,
# MAGIC     `OpportunityCode` STRING,
# MAGIC     `OwnerId` STRING,
# MAGIC     `OpportunityParentId` STRING,
# MAGIC     `PrimaryBusinessUnit` STRING,
# MAGIC     `Probability` DECIMAL(5,2),
# MAGIC     --`ProfileCategory` STRING,
# MAGIC     `ProjectEndDate` DATE,
# MAGIC     `ProjectStartDate` DATE,
# MAGIC     `ProposalDueDate` DATE,
# MAGIC     `RFPDate` DATE,
# MAGIC     `StageName` STRING,
# MAGIC     `ServiceLineCode` STRING,
# MAGIC     `SourceName` STRING,
# MAGIC     `SubmissionType` STRING,
# MAGIC     `AccountClientCode` STRING,
# MAGIC     `CreatedByEmployeeNumber` STRING,
# MAGIC     `GHDCompanyCode` STRING,
# MAGIC     `GHDRegionCode` STRING,
# MAGIC     --`LastModifiedByEmployeeNumber` STRING,
# MAGIC     `ManagerEmployeeNumber` STRING,
# MAGIC     `OwnerEmployeeNumber` STRING,
# MAGIC     `PrimaryBusinessUnitCode` STRING,
# MAGIC     `SponsorEmployeeNumber` STRING,
# MAGIC     `ParentOpportunityCode` STRING,
# MAGIC     `Sponsor__r` STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.minReaderVersion' = 2,
# MAGIC     'delta.minWriterVersion' = 5,
# MAGIC     'delta.columnMapping.mode' = 'name'
# MAGIC );
# MAGIC ----table_name: sf_conf_company
# MAGIC ----tables: []
# MAGIC --Generating DDL Script for Table: sf_conf_company
# MAGIC --__________________________________________________________________________
# MAGIC CREATE TABLE IF NOT EXISTS LH_Silver.sf_conf_company(
# MAGIC     `GHDCompanyID` STRING,
# MAGIC     `GHDCompanyCode` STRING,
# MAGIC     `GHDCompanyName` STRING,
# MAGIC     `CurrencyCode` STRING,
# MAGIC     `IsActive` BOOLEAN
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.minReaderVersion' = 2,
# MAGIC     'delta.minWriterVersion' = 5,
# MAGIC     'delta.columnMapping.mode' = 'name'
# MAGIC );
# MAGIC ----table_name: sf_conf_account
# MAGIC ----tables: []
# MAGIC --Generating DDL Script for Table: sf_conf_account
# MAGIC --__________________________________________________________________________
# MAGIC CREATE TABLE IF NOT EXISTS LH_Silver.sf_conf_account(
# MAGIC     `AccountId` STRING,
# MAGIC     `AccountNumber` STRING,
# MAGIC     `AdditionalName` STRING,
# MAGIC     `AdditionalNotesfromRequestor` STRING,
# MAGIC     `BillingCity` STRING,
# MAGIC     `BillingCountry` STRING,
# MAGIC     `BillingCountryCode` STRING,
# MAGIC     `BillingPostalCode` STRING,
# MAGIC     `BillingState` STRING,
# MAGIC     `BillingStateCode` STRING,
# MAGIC     `BillingStreet` STRING,
# MAGIC     `BSTSyncStatus` STRING,
# MAGIC     `BusinessSize` STRING,
# MAGIC     `IsCarbonNeutralNetZeroGoal` BOOLEAN,
# MAGIC     `CompanyNo` STRING,
# MAGIC     `CorruptionPerceptionIndex` DECIMAL(18,2),
# MAGIC     `CurrencyIsoCode` STRING,
# MAGIC     `Description` STRING,
# MAGIC     `DUNSNumber` STRING,
# MAGIC     `IsDiverseEmploymentPracticeStrategies` BOOLEAN,
# MAGIC     `Engagingfor` STRING,
# MAGIC     `EntityType` STRING,
# MAGIC     `IsEnvironmentalSocialIssuesReview` BOOLEAN,
# MAGIC     `IsEscalatedtoIDATeam` BOOLEAN,
# MAGIC     `IsEscalatedtoInsuranceTeam` BOOLEAN,
# MAGIC     `IsEscalatedtoIntegrityTeam` BOOLEAN,
# MAGIC     `EstimatedStartDate` DATE,
# MAGIC     `ExpectedCompletionDate` DATE,
# MAGIC     `IsGHDRepresentativeWillbeAtsite` BOOLEAN,
# MAGIC     `IsHighRiskActivities` BOOLEAN,
# MAGIC     `HighRiskAssessmentOutcome` STRING,
# MAGIC     `IDATeamComments` STRING,
# MAGIC     `Industry` STRING,
# MAGIC     `InitialContactEmail` STRING,
# MAGIC     `InitialContactName` STRING,
# MAGIC     `IsInsuranceCertificatesCoverParent` BOOLEAN,
# MAGIC     `InsuranceTeamAssessmentoutcome` STRING,
# MAGIC     `IntegrityTeamAssessmentoutcome` STRING,
# MAGIC     `IsDeleted` BOOLEAN,
# MAGIC     `IsIDAProject` BOOLEAN,
# MAGIC     `IsSingleUseOnly` BOOLEAN,
# MAGIC     `IsStrategicRelationship` BOOLEAN,
# MAGIC     `IsVigilantVendor` BOOLEAN,
# MAGIC     `LastActivityDate` DATE,
# MAGIC     `LastModifiedById` STRING,
# MAGIC     `LegalName` STRING,
# MAGIC     `MailingOfficeName` STRING,
# MAGIC     `MasterRecordId` STRING,
# MAGIC     `IsModernSlaveryPolicies` BOOLEAN,
# MAGIC     `Name` STRING,
# MAGIC     `NumberOfEmployees` STRING,
# MAGIC     `OtherEntityType` STRING,
# MAGIC     `OwnerId` STRING,
# MAGIC     `Ownership` STRING,
# MAGIC     `ParentId` STRING,
# MAGIC     `Parent` STRING,
# MAGIC     `IsParentOrgOffersSameServices` BOOLEAN,
# MAGIC     `IsPendingInactivation` BOOLEAN,
# MAGIC     `Phone` STRING,
# MAGIC     `PrimaryOfficeName` STRING,
# MAGIC     `PriorName` STRING,
# MAGIC     `ProjectNumbers` STRING,
# MAGIC     `Rating` STRING,
# MAGIC     `IsReferredforHighRiskActivities` BOOLEAN,
# MAGIC     `RegistrationDate` DATE,
# MAGIC     `RegistrationExpiryDate` DATE,
# MAGIC     `RegistrationStatus` STRING,
# MAGIC     `RequestorLocation` STRING,
# MAGIC     `RequestorName` STRING,
# MAGIC     `Scope` STRING,
# MAGIC     `Service` STRING,
# MAGIC     `ShippingCity` STRING,
# MAGIC     `ShippingCountry` STRING,
# MAGIC     `ShippingCountryCode` STRING,
# MAGIC     `ShippingGeocodeAccuracy` STRING,
# MAGIC     `ShippingLatitude` STRING,
# MAGIC     `ShippingLongitude` STRING,
# MAGIC     `ShippingPostalCode` STRING,
# MAGIC     `ShippingState` STRING,
# MAGIC     `ShippingStateCode` STRING,
# MAGIC     `ShippingStreet` STRING,
# MAGIC     `Stage` STRING,
# MAGIC     `StateTerritoryProvinceofInitialWork` STRING,
# MAGIC     `Status` STRING,
# MAGIC     `StatusNotes` STRING,
# MAGIC     `SummaryofAssessment` STRING,
# MAGIC     `SummaryofQHSEAssessment` STRING,
# MAGIC     `SustainabilityComments` STRING,
# MAGIC     `IsSustainableProcurementPolicy` BOOLEAN,
# MAGIC     `TaxId` STRING,
# MAGIC     `TaxIdType` STRING,
# MAGIC     `TobeDeleted` BOOLEAN,
# MAGIC     `Type` STRING,
# MAGIC     `IsUndertheSameHSEQManagementSystems` BOOLEAN,
# MAGIC     `Variance` STRING,
# MAGIC     `VarianceNotes` STRING,
# MAGIC     `VendorSubType` STRING,
# MAGIC     `VendorType` STRING,
# MAGIC     `VendorCode` STRING,
# MAGIC     `AttributesType` STRING,
# MAGIC     `OwnerEmployeeNumber` STRING,
# MAGIC     `RecordTypeName` STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.minReaderVersion' = 2,
# MAGIC     'delta.minWriterVersion' = 5,
# MAGIC     'delta.columnMapping.mode' = 'name'
# MAGIC );
# MAGIC ----table_name: sf_conf_region
# MAGIC ----tables: []
# MAGIC --Generating DDL Script for Table: sf_conf_region
# MAGIC --__________________________________________________________________________
# MAGIC CREATE TABLE IF NOT EXISTS LH_Silver.sf_conf_region(
# MAGIC     `GHDRegionID` STRING,
# MAGIC     `GHDRegionCode` STRING,
# MAGIC     `RegionName` STRING,
# MAGIC     `GHDCompanyCode` STRING,
# MAGIC     `CurrencyCode` STRING,
# MAGIC     `IsActive` BOOLEAN
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.minReaderVersion' = 2,
# MAGIC     'delta.minWriterVersion' = 5,
# MAGIC     'delta.columnMapping.mode' = 'name'
# MAGIC );
# MAGIC ----table_name: sf_conf_serviceline
# MAGIC ----tables: []
# MAGIC --Generating DDL Script for Table: sf_conf_serviceline
# MAGIC --__________________________________________________________________________
# MAGIC CREATE TABLE IF NOT EXISTS LH_Silver.sf_conf_serviceline(
# MAGIC     `ServiceLineID` STRING,
# MAGIC     `ServiceLineCode` STRING,
# MAGIC     `ServiceLineName` STRING,
# MAGIC     `OpportunityId` STRING,
# MAGIC     `OppurtunityCode` STRING,
# MAGIC     `Fee` DECIMAL(5,2),
# MAGIC     `IsDeleted` BOOLEAN
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.minReaderVersion' = 2,
# MAGIC     'delta.minWriterVersion' = 5,
# MAGIC     'delta.columnMapping.mode' = 'name'
# MAGIC );
# MAGIC ----table_name: sf_conf_strategic_subcategory
# MAGIC ----tables: []
# MAGIC --Generating DDL Script for Table: sf_conf_strategic_subcategory
# MAGIC --__________________________________________________________________________
# MAGIC CREATE TABLE IF NOT EXISTS LH_Silver.sf_conf_strategic_subcategory(
# MAGIC     `CategoryID` STRING,
# MAGIC     `Category` STRING,
# MAGIC     `Subcategory` STRING,
# MAGIC     `IsDeleted` BOOLEAN,
# MAGIC     `OpportunityId` STRING,
# MAGIC     `OpportunityCode` STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.minReaderVersion' = 2,
# MAGIC     'delta.minWriterVersion' = 5,
# MAGIC     'delta.columnMapping.mode' = 'name'
# MAGIC );
# MAGIC ----table_name: sf_conf_profilecategory
# MAGIC ----tables: []
# MAGIC --Generating DDL Script for Table: sf_conf_profilecategory
# MAGIC --__________________________________________________________________________
# MAGIC CREATE TABLE IF NOT EXISTS LH_Silver.sf_conf_profilecategory(
# MAGIC     `ProfileCategoryID` STRING,
# MAGIC     `OpportunityId` STRING,
# MAGIC     `ProfileCategory` STRING,
# MAGIC     `ProfileCategoryLabel` STRING,
# MAGIC     `isActive` BOOLEAN
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.minReaderVersion' = 2,
# MAGIC     'delta.minWriterVersion' = 5,
# MAGIC     'delta.columnMapping.mode' = 'name'
# MAGIC );
# MAGIC ----table_name: sf_cons_clienttype
# MAGIC ----tables: []
# MAGIC --Generating DDL Script for Table: sf_cons_clienttype
# MAGIC --__________________________________________________________________________
# MAGIC CREATE TABLE IF NOT EXISTS LH_Silver.sf_cons_clienttype(
# MAGIC     `IsDefault` BOOLEAN,
# MAGIC     `ClientTypeName` STRING,
# MAGIC     `ClientTypeCode` STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.minReaderVersion' = 2,
# MAGIC     'delta.minWriterVersion' = 5,
# MAGIC     'delta.columnMapping.mode' = 'name'
# MAGIC );
# MAGIC ----table_name: sf_cons_marketsegment
# MAGIC ----tables: []
# MAGIC --Generating DDL Script for Table: sf_cons_marketsegment
# MAGIC --__________________________________________________________________________
# MAGIC CREATE TABLE IF NOT EXISTS LH_Silver.sf_cons_marketsegment(
# MAGIC     `MarketSegmentCode` STRING,
# MAGIC     `MarketSegment` STRING,
# MAGIC     `MarketSubSegment` STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.minReaderVersion' = 2,
# MAGIC     'delta.minWriterVersion' = 5,
# MAGIC     'delta.columnMapping.mode' = 'name'
# MAGIC );
# MAGIC ----table_name: sf_cons_account
# MAGIC ----tables: []
# MAGIC --Generating DDL Script for Table: sf_cons_account
# MAGIC --__________________________________________________________________________
# MAGIC CREATE TABLE IF NOT EXISTS LH_Silver.sf_cons_account(
# MAGIC     `AccountId` STRING,
# MAGIC     `AccountNumber` STRING,
# MAGIC     `AdditionalName` STRING,
# MAGIC     `AdditionalNotesfromRequestor` STRING,
# MAGIC     `BillingCity` STRING,
# MAGIC     `BillingCountry` STRING,
# MAGIC     `BillingCountryCode` STRING,
# MAGIC     `BillingPostalCode` STRING,
# MAGIC     `BillingState` STRING,
# MAGIC     `BillingStateCode` STRING,
# MAGIC     `BillingStreet` STRING,
# MAGIC     `BSTSyncStatus` STRING,
# MAGIC     `BusinessSize` STRING,
# MAGIC     `IsCarbonNeutralNetZeroGoal` BOOLEAN,
# MAGIC     `CompanyNo` STRING,
# MAGIC     `CorruptionPerceptionIndex` DECIMAL(18,2),
# MAGIC     `CurrencyIsoCode` STRING,
# MAGIC     `Description` STRING,
# MAGIC     `DUNSNumber` STRING,
# MAGIC     `IsDiverseEmploymentPracticeStrategies` BOOLEAN,
# MAGIC     `Engagingfor` STRING,
# MAGIC     `EntityType` STRING,
# MAGIC     `IsEnvironmentalSocialIssuesReview` BOOLEAN,
# MAGIC     `IsEscalatedtoIDATeam` BOOLEAN,
# MAGIC     `IsEscalatedtoInsuranceTeam` BOOLEAN,
# MAGIC     `IsEscalatedtoIntegrityTeam` BOOLEAN,
# MAGIC     `EstimatedStartDate` DATE,
# MAGIC     `ExpectedCompletionDate` DATE,
# MAGIC     `IsGHDRepresentativeWillbeAtsite` BOOLEAN,
# MAGIC     `IsHighRiskActivities` BOOLEAN,
# MAGIC     `HighRiskAssessmentOutcome` STRING,
# MAGIC     `IDATeamComments` STRING,
# MAGIC     `Industry` STRING,
# MAGIC     `InitialContactEmail` STRING,
# MAGIC     `InitialContactName` STRING,
# MAGIC     `IsInsuranceCertificatesCoverParent` BOOLEAN,
# MAGIC     `InsuranceTeamAssessmentoutcome` STRING,
# MAGIC     `IntegrityTeamAssessmentoutcome` STRING,
# MAGIC     `IsDeleted` BOOLEAN,
# MAGIC     `IsIDAProject` BOOLEAN,
# MAGIC     `IsSingleUseOnly` BOOLEAN,
# MAGIC     `IsStrategicRelationship` BOOLEAN,
# MAGIC     `IsVigilantVendor` BOOLEAN,
# MAGIC     `LastActivityDate` DATE,
# MAGIC     `LastModifiedById` STRING,
# MAGIC     `LegalName` STRING,
# MAGIC     `MailingOfficeName` STRING,
# MAGIC     `MasterRecordId` STRING,
# MAGIC     `IsModernSlaveryPolicies` BOOLEAN,
# MAGIC     `Name` STRING,
# MAGIC     `NumberOfEmployees` STRING,
# MAGIC     `OtherEntityType` STRING,
# MAGIC     `OwnerId` STRING,
# MAGIC     `Ownership` STRING,
# MAGIC     `ParentId` STRING,
# MAGIC     `Parent` STRING,
# MAGIC     `IsParentOrgOffersSameServices` BOOLEAN,
# MAGIC     `IsPendingInactivation` BOOLEAN,
# MAGIC     `Phone` STRING,
# MAGIC     `PrimaryOfficeName` STRING,
# MAGIC     `PriorName` STRING,
# MAGIC     `ProjectNumbers` STRING,
# MAGIC     `Rating` STRING,
# MAGIC     `IsReferredforHighRiskActivities` BOOLEAN,
# MAGIC     `RegistrationDate` DATE,
# MAGIC     `RegistrationExpiryDate` DATE,
# MAGIC     `RegistrationStatus` STRING,
# MAGIC     `RequestorLocation` STRING,
# MAGIC     `RequestorName` STRING,
# MAGIC     `Scope` STRING,
# MAGIC     `Service` STRING,
# MAGIC     `ShippingCity` STRING,
# MAGIC     `ShippingCountry` STRING,
# MAGIC     `ShippingCountryCode` STRING,
# MAGIC     `ShippingGeocodeAccuracy` STRING,
# MAGIC     `ShippingLatitude` STRING,
# MAGIC     `ShippingLongitude` STRING,
# MAGIC     `ShippingPostalCode` STRING,
# MAGIC     `ShippingState` STRING,
# MAGIC     `ShippingStateCode` STRING,
# MAGIC     `ShippingStreet` STRING,
# MAGIC     `Stage` STRING,
# MAGIC     `StateTerritoryProvinceofInitialWork` STRING,
# MAGIC     `Status` STRING,
# MAGIC     `StatusNotes` STRING,
# MAGIC     `SummaryofAssessment` STRING,
# MAGIC     `SummaryofQHSEAssessment` STRING,
# MAGIC     `SustainabilityComments` STRING,
# MAGIC     `IsSustainableProcurementPolicy` BOOLEAN,
# MAGIC     `TaxId` STRING,
# MAGIC     `TaxIdType` STRING,
# MAGIC     `TobeDeleted` BOOLEAN,
# MAGIC     `Type` STRING,
# MAGIC     `IsUndertheSameHSEQManagementSystems` BOOLEAN,
# MAGIC     `Variance` STRING,
# MAGIC     `VarianceNotes` STRING,
# MAGIC     `VendorSubType` STRING,
# MAGIC     `VendorType` STRING,
# MAGIC     `VendorCode` STRING,
# MAGIC     `AttributesType` STRING,
# MAGIC     `OwnerEmployeeNumber` STRING,
# MAGIC     `RecordTypeName` STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.minReaderVersion' = 2,
# MAGIC     'delta.minWriterVersion' = 5,
# MAGIC     'delta.columnMapping.mode' = 'name'
# MAGIC );
# MAGIC ----table_name: sf_cons_company
# MAGIC ----tables: []
# MAGIC --Generating DDL Script for Table: sf_cons_company
# MAGIC --__________________________________________________________________________
# MAGIC CREATE TABLE IF NOT EXISTS LH_Silver.sf_cons_company(
# MAGIC     `GHDCompanyID` STRING,
# MAGIC     `GHDCompanyCode` STRING,
# MAGIC     `GHDCompanyName` STRING,
# MAGIC     `CurrencyCode` STRING,
# MAGIC     `IsActive` BOOLEAN
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.minReaderVersion' = 2,
# MAGIC     'delta.minWriterVersion' = 5,
# MAGIC     'delta.columnMapping.mode' = 'name'
# MAGIC );
# MAGIC ----table_name: sf_cons_opportunity
# MAGIC ----tables: []
# MAGIC --Generating DDL Script for Table: sf_cons_opportunity
# MAGIC --__________________________________________________________________________
# MAGIC CREATE TABLE IF NOT EXISTS LH_Silver.sf_cons_opportunity(
# MAGIC     `OpportunityId` STRING,
# MAGIC     `AccountId` STRING,
# MAGIC     `OpportunityAmount` DECIMAL(18,2),
# MAGIC     `CapitalCost` DECIMAL(18,2),
# MAGIC     `EstimatedTotalCosttoClient` DECIMAL(18,2),
# MAGIC     `CloseDate` DATE,
# MAGIC     `ContractType` STRING,
# MAGIC     `CurrencyCode` STRING,
# MAGIC     `OpportunityDescription` STRING,
# MAGIC     `EstimatedDuration` STRING,
# MAGIC     `EstimatedGrossFee` DECIMAL(18,2),
# MAGIC     `ExpectedRevenue` DECIMAL(18,2),
# MAGIC     `FundingClientId` STRING,
# MAGIC     `FundingClientCode` STRING,
# MAGIC     `GetProbability` STRING,
# MAGIC     `GHDCompanyId` STRING,
# MAGIC     `GHDRegionId` STRING,
# MAGIC     `GoProbability` STRING,
# MAGIC     `IsConfidential` BOOLEAN,
# MAGIC     `LossReason` STRING,
# MAGIC     `LossReasonComments` STRING,
# MAGIC     `MarketSegmentCode` STRING,
# MAGIC     `OpportunityName` STRING,
# MAGIC     `OpportunityCode` STRING,
# MAGIC     `OwnerId` STRING,
# MAGIC     `OpportunityParentId` STRING,
# MAGIC     `PrimaryBusinessUnit` STRING,
# MAGIC     `Probability` DECIMAL(5,2),
# MAGIC     --`ProfileCategory` STRING,
# MAGIC     `ProjectEndDate` DATE,
# MAGIC     `ProjectStartDate` DATE,
# MAGIC     `ProposalDueDate` DATE,
# MAGIC     `RFPDate` DATE,
# MAGIC     `StageName` STRING,
# MAGIC     `ServiceLineCode` STRING,
# MAGIC     `SourceName` STRING,
# MAGIC     `SubmissionType` STRING,
# MAGIC     `AccountClientCode` STRING,
# MAGIC     `CreatedByEmployeeNumber` STRING,
# MAGIC     `GHDCompanyCode` STRING,
# MAGIC     `GHDRegionCode` STRING,
# MAGIC     --`LastModifiedByEmployeeNumber` STRING,
# MAGIC     `ManagerEmployeeNumber` STRING,
# MAGIC     `OwnerEmployeeNumber` STRING,
# MAGIC     `PrimaryBusinessUnitCode` STRING,
# MAGIC     `SponsorEmployeeNumber` STRING,
# MAGIC     `ParentOpportunityCode` STRING,
# MAGIC     `Sponsor__r` STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.minReaderVersion' = 2,
# MAGIC     'delta.minWriterVersion' = 5,
# MAGIC     'delta.columnMapping.mode' = 'name'
# MAGIC );
# MAGIC ----table_name: sf_cons_profilecategory
# MAGIC ----tables: []
# MAGIC --Generating DDL Script for Table: sf_cons_profilecategory
# MAGIC --__________________________________________________________________________
# MAGIC CREATE TABLE IF NOT EXISTS LH_Silver.sf_cons_profilecategory(
# MAGIC     `ProfileCategoryID` STRING,
# MAGIC     `OpportunityId` STRING,
# MAGIC     `ProfileCategory` STRING,
# MAGIC     `ProfileCategoryLabel` STRING,
# MAGIC     `isActive` BOOLEAN
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.minReaderVersion' = 2,
# MAGIC     'delta.minWriterVersion' = 5,
# MAGIC     'delta.columnMapping.mode' = 'name'
# MAGIC );
# MAGIC ----table_name: sf_cons_region
# MAGIC ----tables: []
# MAGIC --Generating DDL Script for Table: sf_cons_region
# MAGIC --__________________________________________________________________________
# MAGIC CREATE TABLE IF NOT EXISTS LH_Silver.sf_cons_region(
# MAGIC     `GHDRegionID` STRING,
# MAGIC     `GHDRegionCode` STRING,
# MAGIC     `RegionName` STRING,
# MAGIC     `GHDCompanyCode` STRING,
# MAGIC     `CurrencyCode` STRING,
# MAGIC     `IsActive` BOOLEAN
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.minReaderVersion' = 2,
# MAGIC     'delta.minWriterVersion' = 5,
# MAGIC     'delta.columnMapping.mode' = 'name'
# MAGIC );
# MAGIC ----table_name: sf_cons_serviceline
# MAGIC ----tables: []
# MAGIC --Generating DDL Script for Table: sf_cons_serviceline
# MAGIC --__________________________________________________________________________
# MAGIC CREATE TABLE IF NOT EXISTS LH_Silver.sf_cons_serviceline(
# MAGIC     `ServiceLineID` STRING,
# MAGIC     `ServiceLineCode` STRING,
# MAGIC     `ServiceLineName` STRING,
# MAGIC     `OpportunityId` STRING,
# MAGIC     `OppurtunityCode` STRING,
# MAGIC     `Fee` DECIMAL(5,2),
# MAGIC     `IsDeleted` BOOLEAN
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.minReaderVersion' = 2,
# MAGIC     'delta.minWriterVersion' = 5,
# MAGIC     'delta.columnMapping.mode' = 'name'
# MAGIC );
# MAGIC ----table_name: sf_cons_strategic_subcategory
# MAGIC ----tables: []
# MAGIC --Generating DDL Script for Table: sf_cons_strategic_subcategory
# MAGIC --__________________________________________________________________________
# MAGIC CREATE TABLE IF NOT EXISTS LH_Silver.sf_cons_strategic_subcategory(
# MAGIC     `CategoryID` STRING,
# MAGIC     `Category` STRING,
# MAGIC     `Subcategory` STRING,
# MAGIC     `IsDeleted` BOOLEAN,
# MAGIC     `OpportunityId` STRING,
# MAGIC     `OpportunityCode` STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.minReaderVersion' = 2,
# MAGIC     'delta.minWriterVersion' = 5,
# MAGIC     'delta.columnMapping.mode' = 'name'
# MAGIC );
# MAGIC ----table_name: sf_conf_exchangerate
# MAGIC ----tables: []
# MAGIC --Generating DDL Script for Table: sf_conf_exchangerate
# MAGIC --__________________________________________________________________________
# MAGIC CREATE TABLE IF NOT EXISTS LH_Silver.sf_conf_exchangerate(
# MAGIC     `CurrencyID` STRING,
# MAGIC     `RateType` STRING,
# MAGIC     `CurrencyCode` STRING,
# MAGIC     `ExchangeRatePeriod` STRING,
# MAGIC     `ExchangeRate` STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.minReaderVersion' = 2,
# MAGIC     'delta.minWriterVersion' = 5,
# MAGIC     'delta.columnMapping.mode' = 'name'
# MAGIC );
# MAGIC ----table_name: sf_cons_exchangerate
# MAGIC ----tables: []
# MAGIC --Generating DDL Script for Table: sf_cons_exchangerate
# MAGIC --__________________________________________________________________________
# MAGIC CREATE TABLE IF NOT EXISTS LH_Silver.sf_cons_exchangerate(
# MAGIC     `CurrencyID` STRING,
# MAGIC     `RateType` STRING,
# MAGIC     `CurrencyCode` STRING,
# MAGIC     `ExchangeRatePeriod` STRING,
# MAGIC     `ExchangeRate` STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.minReaderVersion' = 2,
# MAGIC     'delta.minWriterVersion' = 5,
# MAGIC     'delta.columnMapping.mode' = 'name'
# MAGIC );

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC --------------------------------------------------------------------------------------------
# MAGIC --Generating DDL Script for All tables in Lakehouse: LH_Gold
# MAGIC 
# MAGIC --table_name: execution_log
# MAGIC --tables: []
# MAGIC --Generating DDL Script for Table: execution_log
# MAGIC --__________________________________________________________________________
# MAGIC CREATE TABLE IF NOT EXISTS LH_Gold.execution_log(
# MAGIC     `notebook` STRING,
# MAGIC     `start_time` DOUBLE,
# MAGIC     `status` STRING,
# MAGIC     `error` STRING,
# MAGIC     `execution_time` DOUBLE,
# MAGIC     `run_order` INT,
# MAGIC     `batch_id` STRING,
# MAGIC     `master_notebook` STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.minReaderVersion' = 2,
# MAGIC     'delta.minWriterVersion' = 5,
# MAGIC     'delta.columnMapping.mode' = 'name'
# MAGIC );
# MAGIC --table_name: batch
# MAGIC --tables: []
# MAGIC --Generating DDL Script for Table: batch
# MAGIC --__________________________________________________________________________
# MAGIC CREATE TABLE IF NOT EXISTS LH_Gold.batch(
# MAGIC     `batch_id` STRING,
# MAGIC     `start_time` BIGINT,
# MAGIC     `status` STRING,
# MAGIC     `master_notebook` STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.minReaderVersion' = 2,
# MAGIC     'delta.minWriterVersion' = 5,
# MAGIC     'delta.columnMapping.mode' = 'name'
# MAGIC );
# MAGIC --table_name: client
# MAGIC --tables: []
# MAGIC --Generating DDL Script for Table: client
# MAGIC --__________________________________________________________________________
# MAGIC CREATE TABLE IF NOT EXISTS LH_Gold.client(
# MAGIC     `AccountId` STRING,
# MAGIC     `AccountNumber` STRING,
# MAGIC     `AdditionalName` STRING,
# MAGIC     `AdditionalNotesfromRequestor` STRING,
# MAGIC     `BillingCity` STRING,
# MAGIC     `BillingCountry` STRING,
# MAGIC     `BillingCountryCode` STRING,
# MAGIC     `BillingPostalCode` STRING,
# MAGIC     `BillingState` STRING,
# MAGIC     `BillingStateCode` STRING,
# MAGIC     `BillingStreet` STRING,
# MAGIC     `BSTSyncStatus` STRING,
# MAGIC     `BusinessSize` STRING,
# MAGIC     `IsCarbonNeutralNetZeroGoal` BOOLEAN,
# MAGIC     `CompanyNo` STRING,
# MAGIC     `CorruptionPerceptionIndex` DECIMAL(18,2),
# MAGIC     `CurrencyIsoCode` STRING,
# MAGIC     `Description` STRING,
# MAGIC     `DUNSNumber` STRING,
# MAGIC     `IsDiverseEmploymentPracticeStrategies` BOOLEAN,
# MAGIC     `Engagingfor` STRING,
# MAGIC     `EntityType` STRING,
# MAGIC     `IsEnvironmentalSocialIssuesReview` BOOLEAN,
# MAGIC     `IsEscalatedtoIDATeam` BOOLEAN,
# MAGIC     `IsEscalatedtoInsuranceTeam` BOOLEAN,
# MAGIC     `IsEscalatedtoIntegrityTeam` BOOLEAN,
# MAGIC     `EstimatedStartDate` DATE,
# MAGIC     `ExpectedCompletionDate` DATE,
# MAGIC     `IsGHDRepresentativeWillbeAtsite` BOOLEAN,
# MAGIC     `IsHighRiskActivities` BOOLEAN,
# MAGIC     `HighRiskAssessmentOutcome` STRING,
# MAGIC     `IDATeamComments` STRING,
# MAGIC     `Industry` STRING,
# MAGIC     `InitialContactEmail` STRING,
# MAGIC     `InitialContactName` STRING,
# MAGIC     `IsInsuranceCertificatesCoverParent` BOOLEAN,
# MAGIC     `InsuranceTeamAssessmentoutcome` STRING,
# MAGIC     `IntegrityTeamAssessmentoutcome` STRING,
# MAGIC     `IsDeleted` BOOLEAN,
# MAGIC     `IsIDAProject` BOOLEAN,
# MAGIC     `IsSingleUseOnly` BOOLEAN,
# MAGIC     `IsStrategicRelationship` BOOLEAN,
# MAGIC     `IsVigilantVendor` BOOLEAN,
# MAGIC     `LastActivityDate` DATE,
# MAGIC     `LastModifiedById` STRING,
# MAGIC     `LegalName` STRING,
# MAGIC     `MailingOfficeName` STRING,
# MAGIC     `MasterRecordId` STRING,
# MAGIC     `IsModernSlaveryPolicies` BOOLEAN,
# MAGIC     `Name` STRING,
# MAGIC     `NumberOfEmployees` STRING,
# MAGIC     `OtherEntityType` STRING,
# MAGIC     `OwnerId` STRING,
# MAGIC     `Ownership` STRING,
# MAGIC     `ParentId` STRING,
# MAGIC     `Parent` STRING,
# MAGIC     `IsParentOrgOffersSameServices` BOOLEAN,
# MAGIC     `IsPendingInactivation` BOOLEAN,
# MAGIC     `Phone` STRING,
# MAGIC     `PrimaryOfficeName` STRING,
# MAGIC     `PriorName` STRING,
# MAGIC     `ProjectNumbers` STRING,
# MAGIC     `Rating` STRING,
# MAGIC     `IsReferredforHighRiskActivities` BOOLEAN,
# MAGIC     `RegistrationDate` DATE,
# MAGIC     `RegistrationExpiryDate` DATE,
# MAGIC     `RegistrationStatus` STRING,
# MAGIC     `RequestorLocation` STRING,
# MAGIC     `RequestorName` STRING,
# MAGIC     `Scope` STRING,
# MAGIC     `Service` STRING,
# MAGIC     `ShippingCity` STRING,
# MAGIC     `ShippingCountry` STRING,
# MAGIC     `ShippingCountryCode` STRING,
# MAGIC     `ShippingGeocodeAccuracy` STRING,
# MAGIC     `ShippingLatitude` STRING,
# MAGIC     `ShippingLongitude` STRING,
# MAGIC     `ShippingPostalCode` STRING,
# MAGIC     `ShippingState` STRING,
# MAGIC     `ShippingStateCode` STRING,
# MAGIC     `ShippingStreet` STRING,
# MAGIC     `Stage` STRING,
# MAGIC     `StateTerritoryProvinceofInitialWork` STRING,
# MAGIC     `Status` STRING,
# MAGIC     `StatusNotes` STRING,
# MAGIC     `SummaryofAssessment` STRING,
# MAGIC     `SummaryofQHSEAssessment` STRING,
# MAGIC     `SustainabilityComments` STRING,
# MAGIC     `IsSustainableProcurementPolicy` BOOLEAN,
# MAGIC     `TaxId` STRING,
# MAGIC     `TaxIdType` STRING,
# MAGIC     `TobeDeleted` BOOLEAN,
# MAGIC     `Type` STRING,
# MAGIC     `IsUndertheSameHSEQManagementSystems` BOOLEAN,
# MAGIC     `Variance` STRING,
# MAGIC     `VarianceNotes` STRING,
# MAGIC     `VendorSubType` STRING,
# MAGIC     `VendorType` STRING,
# MAGIC     `VendorCode` STRING,
# MAGIC     `AttributesType` STRING,
# MAGIC     `OwnerEmployeeNumber` STRING,
# MAGIC     `RecordTypeName` STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.minReaderVersion' = 2,
# MAGIC     'delta.minWriterVersion' = 5,
# MAGIC     'delta.columnMapping.mode' = 'name'
# MAGIC );
# MAGIC --table_name: exchangerate
# MAGIC --tables: []
# MAGIC --Generating DDL Script for Table: exchangerate
# MAGIC --__________________________________________________________________________
# MAGIC CREATE TABLE IF NOT EXISTS LH_Gold.exchangerate(
# MAGIC     `CurrencyID` STRING,
# MAGIC     `RateType` STRING,
# MAGIC     `CurrencyCode` STRING,
# MAGIC     `ExchangeRatePeriod` STRING,
# MAGIC     `ExchangeRate` STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.minReaderVersion' = 2,
# MAGIC     'delta.minWriterVersion' = 5,
# MAGIC     'delta.columnMapping.mode' = 'name'
# MAGIC );
# MAGIC --table_name: fundingclient
# MAGIC --tables: []
# MAGIC --Generating DDL Script for Table: fundingclient
# MAGIC --__________________________________________________________________________
# MAGIC CREATE TABLE IF NOT EXISTS LH_Gold.fundingclient(
# MAGIC     `FundingClientID` STRING,
# MAGIC     `FundingAccountNumber` STRING,
# MAGIC     `FundingClientName` STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.minReaderVersion' = 2,
# MAGIC     'delta.minWriterVersion' = 5,
# MAGIC     'delta.columnMapping.mode' = 'name'
# MAGIC );
# MAGIC --table_name: clienttype
# MAGIC --tables: []
# MAGIC --Generating DDL Script for Table: clienttype
# MAGIC --__________________________________________________________________________
# MAGIC CREATE TABLE IF NOT EXISTS LH_Gold.clienttype(
# MAGIC     `ClientTypeCode` STRING,
# MAGIC     `ClientTypeName` STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.minReaderVersion' = 2,
# MAGIC     'delta.minWriterVersion' = 5,
# MAGIC     'delta.columnMapping.mode' = 'name'
# MAGIC );
# MAGIC --table_name: ghdcompany
# MAGIC --tables: []
# MAGIC --Generating DDL Script for Table: ghdcompany
# MAGIC --__________________________________________________________________________
# MAGIC CREATE TABLE IF NOT EXISTS LH_Gold.ghdcompany(
# MAGIC     `GHDCompanyID` STRING,
# MAGIC     `GHDCompanyCode` STRING,
# MAGIC     `GHDCompanyName` STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.minReaderVersion' = 2,
# MAGIC     'delta.minWriterVersion' = 5,
# MAGIC     'delta.columnMapping.mode' = 'name'
# MAGIC );
# MAGIC --table_name: ghdregion
# MAGIC --tables: []
# MAGIC --Generating DDL Script for Table: ghdregion
# MAGIC --__________________________________________________________________________
# MAGIC CREATE TABLE IF NOT EXISTS LH_Gold.ghdregion(
# MAGIC     `GHDRegionID` STRING,
# MAGIC     `GHDRegionCode` STRING,
# MAGIC     `RegionName` STRING,
# MAGIC     `CurrencyCode` STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.minReaderVersion' = 2,
# MAGIC     'delta.minWriterVersion' = 5,
# MAGIC     'delta.columnMapping.mode' = 'name'
# MAGIC );
# MAGIC --table_name: marketsegment
# MAGIC --tables: []
# MAGIC --Generating DDL Script for Table: marketsegment
# MAGIC --__________________________________________________________________________
# MAGIC CREATE TABLE IF NOT EXISTS LH_Gold.marketsegment(
# MAGIC     `MarketSegmentCode` STRING,
# MAGIC     `MarketSegment` STRING,
# MAGIC     `MarketSubSegment` STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.minReaderVersion' = 2,
# MAGIC     'delta.minWriterVersion' = 5,
# MAGIC     'delta.columnMapping.mode' = 'name'
# MAGIC );
# MAGIC --table_name: opportunity
# MAGIC --tables: []
# MAGIC --Generating DDL Script for Table: opportunity
# MAGIC --__________________________________________________________________________
# MAGIC CREATE TABLE IF NOT EXISTS LH_Gold.opportunity(
# MAGIC     `OpportunityId` STRING,
# MAGIC     `AccountId` STRING,
# MAGIC     `FundingClientId` STRING,
# MAGIC     `GHDCompanyId` STRING,
# MAGIC     `GHDRegionId` STRING,
# MAGIC     `MarketSegmentCode` STRING,
# MAGIC     `OwnerId` STRING,
# MAGIC     `OpportunityParentId` STRING,
# MAGIC     `PrimaryBusinessUnit` STRING,
# MAGIC     `ServiceLineCode` STRING,
# MAGIC     `PrimaryBusinessUnitCode` STRING,
# MAGIC     `ParentOpportunityCode` STRING,
# MAGIC     `OpportunityAmount` DECIMAL(18,2),
# MAGIC     `CapitalCost` DECIMAL(18,2),
# MAGIC     `EstimatedTotalCosttoClient` DECIMAL(18,2),
# MAGIC     `EstimatedDuration` STRING,
# MAGIC     `EstimatedGrossFee` DECIMAL(18,2),
# MAGIC     `ExpectedRevenue` DECIMAL(18,2),
# MAGIC     `CurrencyCode` STRING,
# MAGIC     `GetProbability` STRING,
# MAGIC     `GoProbability` STRING,
# MAGIC     `Probability` DECIMAL(5,2),
# MAGIC     `CloseDate` DATE,
# MAGIC     `ProjectEndDate` DATE,
# MAGIC     `ProjectStartDate` DATE,
# MAGIC     `ProposalDueDate` DATE,
# MAGIC     `RFPDate` DATE,
# MAGIC     `AccountClientCode` STRING,
# MAGIC     `FundingClientCode` STRING,
# MAGIC     `ContractType` STRING,
# MAGIC     `IsConfidential` BOOLEAN,
# MAGIC     `OpportunityCode` STRING,
# MAGIC     `OpportunityName` STRING,
# MAGIC     `OpportunityDescription` STRING,
# MAGIC     `LossReason` STRING,
# MAGIC     `LossReasonComments` STRING,
# MAGIC     --`ProfileCategory` STRING,
# MAGIC     `StageName` STRING,
# MAGIC     `SourceName` STRING,
# MAGIC     `SubmissionType` STRING,
# MAGIC     `SponsorEmployeeNumber` STRING,
# MAGIC     `Sponsor__r` STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.minReaderVersion' = 2,
# MAGIC     'delta.minWriterVersion' = 5,
# MAGIC     'delta.columnMapping.mode' = 'name'
# MAGIC );
# MAGIC --table_name: serviceline
# MAGIC --tables: []
# MAGIC --Generating DDL Script for Table: serviceline
# MAGIC --__________________________________________________________________________
# MAGIC CREATE TABLE IF NOT EXISTS LH_Gold.serviceline(
# MAGIC     `ServiceLineID` STRING,
# MAGIC     `OpportunityId` STRING,
# MAGIC     `ServiceLineCode` STRING,
# MAGIC     `ServiceLineName` STRING,
# MAGIC     `Fee` DECIMAL(5,2)
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.minReaderVersion' = 2,
# MAGIC     'delta.minWriterVersion' = 5,
# MAGIC     'delta.columnMapping.mode' = 'name'
# MAGIC );
# MAGIC --table_name: profilecategory
# MAGIC --tables: []
# MAGIC --Generating DDL Script for Table: profilecategory
# MAGIC --__________________________________________________________________________
# MAGIC CREATE TABLE IF NOT EXISTS LH_Gold.profilecategory(
# MAGIC     `ProfileCategoryID` STRING,
# MAGIC     `OpportunityId` STRING,
# MAGIC     `ProfileCategory` STRING,
# MAGIC     `ProfileCategoryLabel` STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.minReaderVersion' = 2,
# MAGIC     'delta.minWriterVersion' = 5,
# MAGIC     'delta.columnMapping.mode' = 'name'
# MAGIC );
# MAGIC --table_name: strategicsubcategory
# MAGIC --tables: []
# MAGIC --Generating DDL Script for Table: strategicsubcategory
# MAGIC --__________________________________________________________________________
# MAGIC CREATE TABLE IF NOT EXISTS LH_Gold.strategicsubcategory(
# MAGIC     `CategoryID` STRING,
# MAGIC     `OpportunityId` STRING,
# MAGIC     `Category` STRING,
# MAGIC     `Subcategory` STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.minReaderVersion' = 2,
# MAGIC     'delta.minWriterVersion' = 5,
# MAGIC     'delta.columnMapping.mode' = 'name'
# MAGIC );

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
