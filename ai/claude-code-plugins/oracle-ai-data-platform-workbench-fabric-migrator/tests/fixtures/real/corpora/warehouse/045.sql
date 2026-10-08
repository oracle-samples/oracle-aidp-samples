CREATE TABLE [dbo].[synthetic_orders] (

	[order_id] bigint NULL, 
	[customer_id] bigint NULL, 
	[order_ts] datetime2(6) NULL, 
	[amount] float NULL, 
	[region] varchar(8000) NULL
);