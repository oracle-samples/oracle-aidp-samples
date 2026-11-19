[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "95c6b3e4-e916-4fc6-aa76-29951df56f24"]}[Data]{[lakehouseId = "0434ed91-8c6e-4d2a-b4ab-ee1127dc9b6d"]}[Data];
shared SupplierCategories = let
    Source = Snowflake.Databases("dlhdzca-bab11165.snowflakecomputing.com","READONLY"),
    FABRIKAM_SUPPLIERS_Database = Source{[Name="FABRIKAM_SUPPLIERS",Kind="Database"]}[Data],
    PUBLIC_Schema = FABRIKAM_SUPPLIERS_Database{[Name="PUBLIC",Kind="Schema"]}[Data],
    SUPPLIER_CATEGORIES_Table = PUBLIC_Schema{[Name="SUPPLIER_CATEGORIES",Kind="Table"]}[Data],
    #"Removed Duplicates" = Table.Distinct(SUPPLIER_CATEGORIES_Table, {"SupplierCategoryID"})
in
    #"Removed Duplicates";
[BindToDefaultDestination = true]
shared Suppliers = let
    Source = Snowflake.Databases("dlhdzca-bab11165.snowflakecomputing.com","READONLY"),
    FABRIKAM_SUPPLIERS_Database = Source{[Name="FABRIKAM_SUPPLIERS",Kind="Database"]}[Data],
    PUBLIC_Schema = FABRIKAM_SUPPLIERS_Database{[Name="PUBLIC",Kind="Schema"]}[Data],
    SUPPLIERS_Table = PUBLIC_Schema{[Name="SUPPLIERS",Kind="Table"]}[Data],
    #"Changed Type" = Table.TransformColumnTypes(SUPPLIERS_Table,{{"SupplierCategoryID", Int64.Type}})
in
    #"Changed Type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Supplier_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared Supplier = let
  Source = Table.NestedJoin(Suppliers, {"SupplierCategoryID"}, SupplierCategories, {"SupplierCategoryID"}, "STG_SUPPLIER_CATEGORIES", JoinKind.LeftOuter),
  #"Expanded STG_SUPPLIER_CATEGORIES" = Table.ExpandTableColumn(Source, "STG_SUPPLIER_CATEGORIES", {"SupplierCategoryName"}, {"SupplierCategoryName"}),
    #"Removed Other Columns" = Table.SelectColumns(#"Expanded STG_SUPPLIER_CATEGORIES",{"SupplierID", "SupplierName", "SupplierCategoryID", "WebsiteURL", "SupplierCategoryName"}),
    #"Changed Type" = Table.TransformColumnTypes(#"Removed Other Columns",{{"SupplierID", Int64.Type}})
in
    #"Changed Type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "PO_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared PO = let
    Source = Snowflake.Databases("dlhdzca-bab11165.snowflakecomputing.com","READONLY"),
    FABRIKAM_SUPPLIERS_Database = Source{[Name="FABRIKAM_SUPPLIERS",Kind="Database"]}[Data],
    PUBLIC_Schema = FABRIKAM_SUPPLIERS_Database{[Name="PUBLIC",Kind="Schema"]}[Data],
    ORDERS_Table = PUBLIC_Schema{[Name="ORDERS",Kind="Table"]}[Data],
    #"Merged Queries" = Table.NestedJoin(ORDERS_Table, {"PurchaseOrderID"}, POLineItems, {"PurchaseOrderID"}, "PO Line Items", JoinKind.Inner),
    #"Expanded PO Line Items" = Table.ExpandTableColumn(#"Merged Queries", "PO Line Items", {"PurchaseOrderLineID", "StockItemID", "OrderedOuters", "ReceivedOuters", "ExpectedUnitPricePerOuter"}, {"PurchaseOrderLineID", "StockItemID", "OrderedOuters", "ReceivedOuters", "ExpectedUnitPricePerOuter"}),
    #"Changed Type" = Table.TransformColumnTypes(#"Expanded PO Line Items",{{"ContactPersonID", Int64.Type}, {"SupplierID", Int64.Type}, {"PurchaseOrderID", Int64.Type}, {"PurchaseOrderLineID", Int64.Type}, {"StockItemID", Int64.Type}}),
    #"Removed Other Columns" = Table.SelectColumns(#"Changed Type",{"PurchaseOrderID", "OrderDate", "ContactPersonID", "ExpectedDeliveryDate", "PurchaseOrderLineID", "StockItemID", "OrderedOuters", "ExpectedUnitPricePerOuter"})
in
    #"Removed Other Columns";
[BindToDefaultDestination = true]
shared POLineItems = let
    Source = Snowflake.Databases("dlhdzca-bab11165.snowflakecomputing.com","READONLY"),
    FABRIKAM_SUPPLIERS_Database = Source{[Name="FABRIKAM_SUPPLIERS",Kind="Database"]}[Data],
    PUBLIC_Schema = FABRIKAM_SUPPLIERS_Database{[Name="PUBLIC",Kind="Schema"]}[Data],
    ORDERLINES_Table = PUBLIC_Schema{[Name="ORDERLINES",Kind="Table"]}[Data]
in
    ORDERLINES_Table;
shared Supplier_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "95c6b3e4-e916-4fc6-aa76-29951df56f24"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "0434ed91-8c6e-4d2a-b4ab-ee1127dc9b6d"]}[Data],
  TableNavigation = Navigation_2{[Id = "Supplier", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared PO_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "95c6b3e4-e916-4fc6-aa76-29951df56f24"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "0434ed91-8c6e-4d2a-b4ab-ee1127dc9b6d"]}[Data],
  TableNavigation = Navigation_2{[Id = "PO", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
