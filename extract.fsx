#r "nuget: FSharp.Data"

open System.IO
open FSharp.Data

type DeansListEntry =
    JsonProvider<"""[{"quarter":"Autumn 2024","first_name":"Gabriel","middle_name":"Fortier","last_name":"Aal","college":"COLLEGE OF ARTS AND SCIENCES"}]""">

let quarterNames = [ "autumn"; "summer"; "spring"; "winter" ]

let years = [ 2021..2024 ]

let api quarter year =
    $"https://apps.asais.uw.edu/apis/public/public-quarterly-deans/public_deans_list_%s{quarter}%d{year}.json"

let path (quarter: string) (year: int) =

    if Path.Exists "output/raw" |> not then
        Directory.CreateDirectory "output/raw" |> ignore

    $"output/raw/deans-list-{(string year)[2..3]}{quarter[0..1]}.csv"

for year in years do
    for quarter in quarterNames do
        printfn "Processing %s %d" quarter year
        use file = path quarter year |> File.CreateText

        fprintfn file "LastName|FirstName|MiddleName|College"

        for entry in api quarter year |> DeansListEntry.Load do
            fprintfn file $"{entry.LastName}|{entry.FirstName}|{entry.MiddleName}|{entry.College}"
