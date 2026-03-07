// Power Query M Code for Excel - Experiment Results Import
// Use: Get Data > From File > From Text/CSV > Select experiment_results.csv
// Then: Home > Advanced Editor > Replace with this code (adapt path as needed)

let
    // Step 1: Load CSV - use full path or relative to workbook
    Source = Csv.Document(File.Contents("C:\Users\gayat\-E-COMMERCE-STREAMING-ANALYTICS-PLATFORM\output\analysis\experiment_results.csv"), [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.None]),
    PromotedHeaders = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    
    // Step 2: Change types
    Typed = Table.TransformColumnTypes(PromotedHeaders, {
        {"Experiment", type text},
        {"Experiment_ID", type text},
        {"Variant", type text},
        {"Users", Int64.Type},
        {"Conversions", Int64.Type},
        {"Conv_Rate", type number},
        {"Revenue", type number},
        {"AOV", type number}
    }),
    
    // Step 3: Add Conv_Rate_Pct (Conv_Rate is 0-100, e.g. 3.2 for 3.2%)
    AddedConvPct = Table.AddColumn(Typed, "Conv_Rate_Pct", each [Conv_Rate] / 100, Percentage.Type)
in
    AddedConvPct
