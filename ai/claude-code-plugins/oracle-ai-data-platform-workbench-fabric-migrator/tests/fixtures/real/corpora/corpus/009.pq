[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "squirrel-data csv_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "Area_Name", DestinationColumnName = "Area_Name"], [SourceColumnName = "Area_ID", DestinationColumnName = "Area_ID"], [SourceColumnName = "Park_Name", DestinationColumnName = "Park_Name"], [SourceColumnName = "Park_ID", DestinationColumnName = "Park_ID"], [SourceColumnName = "Squirrel_ID", DestinationColumnName = "Squirrel_ID"], [SourceColumnName = "Primary_Fur_Color", DestinationColumnName = "Primary_Fur_Color"], [SourceColumnName = "Highlights_in_Fur_Color", DestinationColumnName = "Highlights_in_Fur_Color"], [SourceColumnName = "Color_Notes", DestinationColumnName = "Color_Notes"], [SourceColumnName = "Location", DestinationColumnName = "Location"], [SourceColumnName = "Specific_Location", DestinationColumnName = "Specific_Location"], [SourceColumnName = "Activities", DestinationColumnName = "Activities"], [SourceColumnName = "Interactions_with_Humans", DestinationColumnName = "Interactions_with_Humans"], [SourceColumnName = "Other_Notes_or_Observations", DestinationColumnName = "Other_Notes_or_Observations"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared #"squirrel-data csv" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "a3d19f1f-5072-4e01-b263-bf9a0df97c6d"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "332e965d-d3c4-4186-8011-6ea11e54b9f1"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "squirrel-data.csv"]}[Content],
  #"Imported CSV" = Csv.Document(#"Navigation 3", [Delimiter = ",", Columns = 13, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"Area_Name", type text}, {"Area_ID", type text}, {"Park_Name", type text}, {"Park_ID", Int64.Type}, {"Squirrel_ID", type text}, {"Primary_Fur_Color", type text}, {"Highlights_in_Fur_Color", type text}, {"Color_Notes", type text}, {"Location", type text}, {"Specific_Location", type text}, {"Activities", type text}, {"Interactions_with_Humans", type text}, {"Other_Notes_or_Observations", type text}})
in
  #"Changed column type";
shared #"squirrel-data csv_DataDestination" = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "a3d19f1f-5072-4e01-b263-bf9a0df97c6d"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "893d5431-c636-4e26-8cf9-17be7c377a61"]}[Data],
  TableNavigation = Navigation_2{[Item = "squirrel_data", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
