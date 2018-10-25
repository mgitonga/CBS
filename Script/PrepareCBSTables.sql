----prepare tables 
delete from dbo.cbsPatientEncounters
go
delete from [dbo].[cbsARTRegimenEvent]
go
delete from  [dbo].[cbsCD4CountEvent]
go
delete from [dbo].[cbsCD4PercentEvent]
go
delete from [dbo].[cbsViralLoadEvent]
go
delete  from  [dbo].[cbsReportingFacility]
go
delete from [dbo].[cbsPatientProfile]
go
--delete from [MPI].[dbo].[PatientMatchingTable]
--go 

INSERT cbsReportingFacility(FacilityName, FacilityCounty, FacilitySubCounty, FacilityCode)
SELECT FacilityName,County, District,FacilityCode FROM Facilities
go 

PRINT 'Patient Profile...'
GO
INSERT INTO [dbo].[cbsPatientProfile]
           (
           [PatientUID]
           ,[Sex]
           ,[BirthDate]
           ,[ReportingFacilityCode]
           ,[MaritalStatus]
           ,[CCCNumber]
           ,[HEINumber]
           ,[MotherCCCNumber]
           ,[PopulationType]
           ,[HivDiagnosisDate]
           ,[HivDiagnosisType]
           ,[HivCareEntryDate]
           ,[EnrollmentWHO]
           ,[ModeOfTransmission]
           ,[ArtInitiationDate]
           ,[DeathDate]
           ,[ResidenceWard]
           ,[ResidenceVillage]
           ,[ResidenceCounty]
           ,[ResidenceSubCounty]
           ,[ARTHistory]
           ,[HivRecencyTestConducted]
           ,[HivRecent]
           ,[TbInfectedAtEnrollment]
           ,[PregnantAtEnrollment]
           ,[BreastFeedingAtEnrollment])
SELECT NEWID(), 
p.Gender, 
CAST(p.Dob as Date),
p.SiteCode,
MaritalStatus,p.PatientID AS CCCNumber, NULL AS HEINUmber,NULL AS MotherCCCNumber, 
NULL AS PopulationType,DateConfirmedHIVPositive,
NULL AS HIVDiagnosisType, 
RegistrationAtCCC as HIVCareEntryDate, 
[eWHO] AS EnrollmentWHO,
NULL AS ModeOfTransmission, 
ART.StartARTDate AS ARTInitiationDate, ---Case Statement Previous or This MMb 
S.ExitDate AS DeatDate, 
NULL as ResidenceWard, 
p.Village, 
NULL AS ResidenceCounty, 
P.District AS SubCounty, 
NULL as ARTHistory,
NULL AS HiVRecencyTestConducted, 
NULL AS HivRecent, 
NULL AS TBInfectedAtEnrollment, 
CASE WHEN P.Gender='F' THEN 
	 CASE 
		WHEN Preg.[PregnantAtEnrol]=1 THEN '1'ELSE '0' 
	 END 
	 ELSE NULL 
END  AS PregnantAtEnrollment, 
NULL AS BreastFeedingAtEnrollment
 FROM [All_Staging_2016_2].[dbo].[stg_Patients] P
 INNER JOIN [All_Staging_2016_2].[dbo].[stg_ARTPatients] ART
 ON ART.PatientId=P.PatientId AND ART.SiteCode=P.SiteCode AND ART.PatientPK = P.PatientPK
 LEFT JOIN [All_Staging_2016_2].[dbo].PatientBaselines PB 
 ON  PB.PatientId=P.PatientId AND PB.SiteCode=P.SiteCode AND PB.PatientPK = P.PatientPK
 LEFT JOIN [All_Staging_2016_2].[dbo].[stg_PatientStatus]  S
  ON  S.PatientId=P.PatientId AND S.SiteCode=P.SiteCode AND S.PatientPK = P.PatientPK AND S.ExitReason='DIED'
  LEFT JOIN [All_Staging_2016_2].[dbo].[vw_PatientPregnancyStatusAtInitiation] Preg 
  ON  Preg.PatientId=P.PatientId AND Preg.SiteCode=P.SiteCode AND Preg.PatientPK = P.PatientPK  

  go 
PRINT 'Removing Facilities...'
GO


--Remove Facilities Not In Patient Profile 
DELETE cbsReportingFacility WHERE [FacilityCode] NOT IN (SELECT [ReportingFacilityCode] FROM [dbo].[cbsPatientProfile] GROUP BY [ReportingFacilityCode])

GO 

PRINT 'De-duplicating...'
GO

--- DE-DUPLICATE 
DELETE M 
FROM [cbsPatientProfile] M
INNER JOIN 
(
SELECT 
ROW_NUMBER () OVER (PARTITION BY Sex, BirthDate,CCCNumber,ReportingFacilityCode ORDER BY ReportingFacilityCode,CCCNumber, sex, BirthDate) AS RowId ,
 Sex, BirthDate,CCCNumber,ReportingFacilityCode, PatientUID
 FROM [cbsPatientProfile]) A 
 ON A.CCCNumber= M.CCCNumber AND A.BirthDate=M.BirthDate AND A.Sex=M.Sex 
 AND A.ReportingFacilityCode=M.ReportingFacilityCode anD A.PatientUID=M.PatientUID
 WHERE  A.RowId > 1

 Go 

 PRINT 'Patient Mapping Table...'
GO


--select * from [dbo].[cbsPatientProfile]
--SELECT * FROM [MPI].[dbo].[PatientMatchingTable]

 ---PatientUID Matching Table 

INSERT  [MPI].[dbo].[PatientMatchingTable] (PatientUID, SiteCode, CCCNumber)
SELECT P.PatientUID, P.ReportingFacilityCode, P.CCCNumber FROM [dbo].[cbsPatientProfile] P
LEFT JOIN [MPI].[dbo].[PatientMatchingTable] M ON M.SiteCode = P.ReportingFacilityCode AND M.CCCNumber=P.CCCNumber WHERE M.PatientUID IS NULL

 GO 

 -----------------------
PRINT 'CD4 Events'
go
INSERT  [dbo].[cbsCD4CountEvent] (PatientUID, PatientCD4CountDate, PatientCD4Count, ReportingFacilityCode)
select DISTINCT T.PatientUID,v.OrderedbyDate,CAST(v.TestResult AS Float),v.SiteCode from [All_Staging_2016_2].dbo.vw_GetCD4Counts v
INNER JOIN MPI.[dbo].[PatientMatchingTable] t ON V.PatientID = t.CCCNumber AND v.SiteCode=T.SiteCode 
INNER JOIN [dbo].[cbsReportingFacility] f ON f.FacilityCode=v.SiteCode
WHERE TestResult IS NOT NULL
Order by t.PatientUID, OrderedbyDate ASC


Print 'CD4PercentEvent'
go 
INSERT  [dbo].[cbsCD4PercentEvent](PatientUID, PatientCD4PercentDate, [PatientCD4Percent], ReportingFacilityCode)
select DISTINCT T.PatientUID,v.OrderedbyDate,CAST(v.TestResult AS Float),v.SiteCode from [All_Staging_2016_2].dbo.vw_GetCD4Percent v
INNER JOIN MPI.[dbo].[PatientMatchingTable] t ON V.PatientID = t.CCCNumber AND v.SiteCode=T.SiteCode 
INNER JOIN [dbo].[cbsReportingFacility] f ON f.FacilityCode=v.SiteCode
WHERE TestResult IS NOT NULL
Order by t.PatientUID, OrderedbyDate ASC


Print 'Viral Loads'
go
INSERT [dbo].[cbsViralLoadEvent] (PatientUID, VLDate, VLSampleResult, VLPatientResultSuppressed, ReportingFacilityCode)
SELECT DISTINCT  T.PatientUID,v.OrderedbyDate,TestResult,
CASE WHEN ISNUMERIC (TestResult)=1 THEN 
	 CASE WHEN CAST(TestResult AS FLOAT)<=1000 THEN 'YES' ELSE 'NO' END 
	 ELSE 'YES'  END AS VLSuppressed, v.SiteCode  FROM [All_Staging_2016_2].dbo.[vw_GetViralLoads] V

INNER JOIN MPI.[dbo].[PatientMatchingTable] t ON V.PatientID = t.CCCNumber AND v.SiteCode=T.SiteCode 
INNER JOIN [dbo].[cbsReportingFacility] f ON f.FacilityCode=v.SiteCode
WHERE TestResult IS NOT NULL AND TestResult <> 'Rejected'
Order by t.PatientUID, OrderedbyDate ASC

go

--SELECT * FROM [All_Staging_2016_2].dbo.[vw_GetViralLoads] V

---
Print 'Regimen Events'
go
INSERT [dbo].[cbsARTRegimenEvent](
 [PatientUID],[ARTRegimenStartDate],[ARTRegimen],[ReportingFacilityCode],[ARTRegimenEndDate])

SELECT PatientUID,Dispensedate,Drug,SiteCode,PrevDispenseDate 
FROM (
	SELECT ROW_NUMBER() OVER( PARTITION BY PatientUID,Dispensedate,Drug ORDER BY PatientUID,Dispensedate ASC) as num
	,PatientUID,Dispensedate,Drug, A.SiteCode,PrevDispenseDate   FROM 
	(SELECT  t.PatientUID,Dispensedate,Drug, A.SiteCode, /*Num
	,LAG(Drug) OVER (PARTITION BY PatientId, PatientPK, A.SiteCode, DRUG ORDER BY PatientId,num  ) AS  PrevDrug */
	LAG(Dispensedate) OVER (PARTITION BY PatientId, PatientPK, A.SiteCode, Drug ORDER BY PatientId, NUM  ) AS  PrevDispenseDate
	FROM 
	(
	SELECT 
	ROW_NUMBER() OVER (PARTITION BY PatientId, PatientPK, SiteCode, Drug ORDER BY PatientId, PatientPK, SiteCode, Dispensedate) AS NUM , 
	PatientId, PatientPK, SiteCode, Drug,Dispensedate 
	 FROM [All_Staging_2016_2].dbo.Stg_PatientPharmacy WHERE TreatmentType IN ('ARV', 'ART','PMTCT') 
	 ) A 

	INNER JOIN MPI.[dbo].[PatientMatchingTable] t ON A.PatientID = t.CCCNumber AND A.SiteCode=T.SiteCode 
		) as  A
) AS B WHERE Num=1

--SELECT DISTINCT * FROM 
--(SELECT TOP 100 PERCENT t.PatientUID,Dispensedate,Drug, A.SiteCode, /*Num
--,LAG(Drug) OVER (PARTITION BY PatientId, PatientPK, A.SiteCode, DRUG ORDER BY PatientId,num  ) AS  PrevDrug */
--LAG(Dispensedate) OVER (PARTITION BY PatientId, PatientPK, A.SiteCode, Drug ORDER BY PatientId, NUM  ) AS  PrevDispenseDate
--FROM 
--(
--SELECT 
--ROW_NUMBER() OVER (PARTITION BY PatientId, PatientPK, SiteCode, Drug ORDER BY PatientId, PatientPK, SiteCode, Dispensedate) AS NUM , 
--PatientId, PatientPK, SiteCode, Drug,Dispensedate 
-- FROM [All_Staging_2016_2].dbo.Stg_PatientPharmacy WHERE TreatmentType IN ('ARV', 'ART','PMTCT') 
-- ) A 

--INNER JOIN MPI.[dbo].[PatientMatchingTable] t ON A.PatientID = t.CCCNumber AND A.SiteCode=T.SiteCode 
--ORDER BY PatientID,DispenseDate ASC

--) A

--go



--SELECT Num, PatientId, PatientPK, SiteCode, Drug,Dispensedate,
--LAG(Drug) OVER (PARTITION BY PatientId, PatientPK, SiteCode, DRUG ORDER BY PatientId,num  ) AS  PrevDrug 
--,LAG(Dispensedate) OVER (PARTITION BY PatientId, PatientPK, SiteCode, Drug ORDER BY PatientId, NUM  ) AS  PrevDispenseDate
--FROM 
--(
--SELECT 
--ROW_NUMBER() OVER (PARTITION BY PatientId, PatientPK, SiteCode, Drug ORDER BY PatientId, PatientPK, SiteCode, Dispensedate) AS NUM , 
--PatientId, PatientPK, SiteCode, Drug,Dispensedate, R.Target_Regimen 
-- FROM [All_Staging_2016_2].dbo.Stg_PatientPharmacy P 
-- LEFT JOIN [All_Staging_2016_2].[dbo].[lkp_RegimenLineMap] R ON R.Source_Regimen = P.Drug
-- WHERE TreatmentType IN ('ARV', 'ART','PMTCT') 
-- ) A 
-- --ORDER BY PatientId, PatientPK, SiteCode,  Dispensedate 


--select * from [dbo].[lkp_RegimenLineMapping]

INSERT  [dbo].[cbsPatientEncounters] (PatientUID, EncounterDate, EncounterType, ReportingFacilityCode)
SELECT DISTINCT T.PatientUID,V.VisitDate,ISNULL(v.VisitType,'Not Specified'), v.SiteCode FROM All_Staging_2016_2.dbo.stg_PatientVisits V
INNER JOIN MPI.[dbo].[PatientMatchingTable] t ON V.PatientID = t.CCCNumber AND v.SiteCode=T.SiteCode 
INNER JOIN [dbo].[cbsReportingFacility] f ON f.FacilityCode=v.SiteCode

---** MG Mapping of Visit Types  **

  UPDATE 
  M SET M.FacilityEMR= P.Emr
  FROM [CBS_Staging].[dbo].[cbsReportingFacility] M
  INNER JOIN 
  (select DISTINCT SiteCode, EMR FROM All_Staging_2016_2.dbo.stg_Patients) P
  ON P.SiteCode = M.FacilityCode
