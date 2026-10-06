CREATE TABLE [dbo].[dimension_city] (

	[CityKey] int NULL, 
	[WWICityID] int NULL, 
	[City] varchar(8000) NULL, 
	[StateProvince] varchar(8000) NULL, 
	[Country] varchar(8000) NULL, 
	[Continent] varchar(8000) NULL, 
	[SalesTerritory] varchar(8000) NULL, 
	[Region] varchar(8000) NULL, 
	[Subregion] varchar(8000) NULL, 
	[Location] varchar(8000) NULL, 
	[LatestRecordedPopulation] bigint NULL, 
	[ValidFrom] datetime2(6) NULL, 
	[ValidTo] datetime2(6) NULL, 
	[LineageKey] int NULL
);