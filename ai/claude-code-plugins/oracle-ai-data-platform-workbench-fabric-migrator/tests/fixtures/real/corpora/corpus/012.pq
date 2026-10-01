[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "95c6b3e4-e916-4fc6-aa76-29951df56f24"]}[Data]{[lakehouseId = "0434ed91-8c6e-4d2a-b4ab-ee1127dc9b6d"]}[Data];
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "People_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared People = let
    Source = SharePoint.Files("https://faiad2024.sharepoint.com/sites/Fabrikam_HR", [ApiVersion = 15]),
    #"dim_People xlsx_https://faiad2024 sharepoint com/sites/Fabrikam_HR/Shared Documents/General/" = Source{[Name="dim_People.xlsx",#"Folder Path"="https://faiad2024.sharepoint.com/sites/Fabrikam_HR/Shared Documents/General/"]}[Content],
    #"Imported Excel Workbook" = Excel.Workbook(#"dim_People xlsx_https://faiad2024 sharepoint com/sites/Fabrikam_HR/Shared Documents/General/"),
    #"Expanded Data" = Table.ExpandTableColumn(#"Imported Excel Workbook", "Data", {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16", "Column17", "Column18"}, {"Data.Column1", "Data.Column2", "Data.Column3", "Data.Column4", "Data.Column5", "Data.Column6", "Data.Column7", "Data.Column8", "Data.Column9", "Data.Column10", "Data.Column11", "Data.Column12", "Data.Column13", "Data.Column14", "Data.Column15", "Data.Column16", "Data.Column17", "Data.Column18"}),
    #"Removed Columns" = Table.RemoveColumns(#"Expanded Data",{"Name"}),
    #"Promoted Headers" = Table.PromoteHeaders(#"Removed Columns", [PromoteAllScalars=true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted Headers", {{"PersonID", Int64.Type}, {"FullName", type text}, {"PreferredName", type text}, {"SearchName", type text}, {"IsPermittedToLogon", type logical}, {"LogonName", type text}, {"IsExternalLogonProvider", type logical}, {"IsSystemUser", type logical}, {"IsSalesperson", type logical}, {"UserPreferences", type text}, {"PhoneNumber", type text}, {"FaxNumber", type text}, {"EmailAddress", type text}, {"CustomFields", type text}, {"OtherLanguages", type text}, {"LastEditedBy", Int64.Type}, {"ValidFrom", type datetime}, {"ValidTo", type datetime}}),
    #"Removed Other Columns" = Table.SelectColumns(#"Changed column type",{"PersonID", "FullName", "PreferredName", "IsSalesperson", "EmailAddress"})
in
    #"Removed Other Columns";
shared People_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "95c6b3e4-e916-4fc6-aa76-29951df56f24"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "0434ed91-8c6e-4d2a-b4ab-ee1127dc9b6d"]}[Data],
  TableNavigation = Navigation_2{[Id = "People", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
