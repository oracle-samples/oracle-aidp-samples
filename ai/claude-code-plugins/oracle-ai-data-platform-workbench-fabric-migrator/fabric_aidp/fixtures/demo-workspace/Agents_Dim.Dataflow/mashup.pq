section Section1;

[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_agent_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared dim_agent = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "11111111-1111-1111-1111-111111111111"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "22222222-2222-2222-2222-222222222222"]}[Data],
  TableNavigation = Navigation_2{[Id = "agent", ItemKind = "Table"]}?[Data]?,
  #"Choose columns" = Table.SelectColumns(TableNavigation, {"agent_id", "first_name", "last_name", "region", "hired_on"}),
  #"Added full name" = Table.AddColumn(#"Choose columns", "full_name", each [first_name] & " " & [last_name]),
  #"Added hire year" = Table.AddColumn(#"Added full name", "hire_year", each Date.Year([hired_on])),
  #"Changed column type" = Table.TransformColumnTypes(#"Added hire year", {{"full_name", type text}, {"hire_year", Int64.Type}}),
  #"Filtered rows" = Table.SelectRows(#"Changed column type", each [region] <> null)
in
  #"Filtered rows";

shared dim_agent_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "11111111-1111-1111-1111-111111111111"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "22222222-2222-2222-2222-222222222222"]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_agent", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;

shared load_cutoff = #date(2026, 1, 1);

[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "ext_rates_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic"]]}]
shared ext_rates = let
  Source = Excel.Workbook(Web.Contents("https://example.invalid/rates.xlsx"), null, true),
  #"Promoted headers" = Table.PromoteHeaders(Source)
in
  #"Promoted headers";
