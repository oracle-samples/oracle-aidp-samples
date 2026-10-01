[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "a8cbda3d-903e-4154-97d9-9a91c95abb42"]}[Data]{[lakehouseId = "7d0dfe91-9b90-42c4-9424-07dbd443a54c"]}[Data];
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dimension_customer_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "CustomerKey", DestinationColumnName = "CustomerKey"], [SourceColumnName = "WWICustomerID", DestinationColumnName = "WWICustomerID"], [SourceColumnName = "Customer", DestinationColumnName = "Customer"], [SourceColumnName = "BillToCustomer", DestinationColumnName = "BillToCustomer"], [SourceColumnName = "Category", DestinationColumnName = "Category"], [SourceColumnName = "BuyingGroup", DestinationColumnName = "BuyingGroup"], [SourceColumnName = "PrimaryContact", DestinationColumnName = "PrimaryContact"], [SourceColumnName = "PostalCode", DestinationColumnName = "PostalCode"], [SourceColumnName = "ValidFrom", DestinationColumnName = "ValidFrom"], [SourceColumnName = "ValidTo", DestinationColumnName = "ValidTo"], [SourceColumnName = "LineageKey", DestinationColumnName = "LineageKey"]}], DynamicSchema = true, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared dimension_customer = let
  Source = Csv.Document(Web.Contents("https://mngenvmcap372892-my.sharepoint.com/personal/admin_mngenvmcap372892_onmicrosoft_com/Documents/Apps/Microsoft Power Query/Uploaded Files/dimension_customer 9.csv"), [Delimiter = ",", Columns = 11, Encoding = 65001, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"CustomerKey", Int64.Type}, {"WWICustomerID", Int64.Type}, {"Customer", type text}, {"BillToCustomer", type text}, {"Category", type text}, {"BuyingGroup", type text}, {"PrimaryContact", type text}, {"PostalCode", type text}, {"ValidFrom", type datetime}, {"ValidTo", type datetime}, {"LineageKey", Int64.Type}})
in
  #"Changed column type";
shared dimension_customer_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "a8cbda3d-903e-4154-97d9-9a91c95abb42"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "7d0dfe91-9b90-42c4-9424-07dbd443a54c"]}[Data],
  TableNavigation = Navigation_2{[Id = "dimension_customer", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
