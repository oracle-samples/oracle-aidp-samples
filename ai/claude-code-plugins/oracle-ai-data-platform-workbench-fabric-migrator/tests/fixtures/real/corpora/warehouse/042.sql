CREATE TABLE [dbo].[dimension_customer] (

	[CustomerKey] int NULL, 
	[WWICustomerID] int NULL, 
	[Customer] varchar(8000) NULL, 
	[BillToCustomer] varchar(8000) NULL, 
	[Category] varchar(8000) NULL, 
	[BuyingGroup] varchar(8000) NULL, 
	[PrimaryContact] varchar(8000) NULL, 
	[PostalCode] varchar(8000) NULL, 
	[ValidFrom] datetime2(6) NULL, 
	[ValidTo] datetime2(6) NULL, 
	[LineageKey] int NULL
);