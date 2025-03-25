(* Binary Data Format

Big Endian format

[HEADER format | binary 32 bit]
[4 byte magic number "DUBS" 0x44554253]
[4 byte version number 0x00000001]
[4 byte count of records]
[4 byte record size (all records are the same size)]
[4 byte first year]

[record 1]
[record 2]
...
[record n]

// if a name exists in multiple colleges, it will be in separate records
RECORD FORMAT
[record i format | UTF8] {ALL CAPS, separated by '|', no spaces}
[LAST NAME]|[FIRST NAME]|[MIDDLE NAME]|[COLLEGE CODE]|[Quarter Codes][Year Codes]|

QUARTER CODE / YEAR CODE FORMAT
[0000_0000_0000_0000_0000_0000_0000_0000][0000_0000]
[32 quarters, WI SP SU AU][8 years]
*)

#load "colleges.fsx"

let contextYear = 2020


open System.Buffers.Binary
open System.Text
open System.IO
open System

let files = Directory.GetFiles "output/raw"

let headerSize = 32 // must match above description

let extractLine (line: string) =
    // assuming line is valid

    let parts = line.Split '|'

    let code = Map.tryFind parts[3] Colleges.codeMap

    match code with
    | None -> 
        failwithf "Unknown college code: %s" parts[3]
    | Some c ->
        $"{parts[0]}|{parts[1]}|{parts[2]}|{c}|".ToUpper()

type Quarter =
    | Winter
    | Spring
    | Summer
    | Autumn

type DeansList =
    | Quarter of year : int * quarter : Quarter
    | Year of year : int

let proc (filePath: string) =
    // extract year and quarter codes from file name
    // example file names: deans-list-21.csv, deans-list-21au.csv

    let name = 
        Path.GetFileNameWithoutExtension filePath
        |> _.Split('-')
        |> Seq.last

    let deansListType =
        match name.Length with
        | 2 -> 
            let year = int name
            
            if year - contextYear > 7 then
                failwithf "Year %d is too far in the future" year
            else if year - contextYear < 0 then
                failwithf "Year %d is in the past" year

            Year year
        | 4 ->
            let year = name[0..1] |> int
            let quarter = name[2 .. 3]
            match quarter.ToUpper() with
            | "WI" -> Quarter(year, Winter)
            | "SP" -> Quarter(year, Spring)
            | "SU" -> Quarter(year, Summer)
            | "AU" -> Quarter(year, Autumn)
            | _ -> failwithf "Unknown quarter code: %s" quarter
        | _ -> failwith $"unknown file name {filePath}"

    let lines =
        File.ReadLines filePath
        |> Seq.skip 1
        |> Seq.map extractLine

    {| People = lines; Date = deansListType |}

let buildPerson (person: string) (lists: DeansList seq) =
    
    let years = 
        Seq.choose (function Year y -> Some y | _ -> None) lists
        |> Seq.map (fun y -> y - contextYear)
        |> Seq.map ((<<<) 1u)
        |> Seq.fold (|||) 0u
        |> byte

    let quarters =
        Seq.choose (function | Quarter(y, q) -> Some (y - contextYear, q) | _ -> None) lists
        |> Seq.map (fun (y, q) -> y * 4 +  match q with Winter -> 0 | Spring -> 1 | Summer -> 2 | Autumn -> 3)
        |> Seq.map ((<<<) 1u)
        |> Seq.fold (|||) 0u
    
    let buff = Encoding.UTF8.GetByteCount person + 5 + 1 |> Array.zeroCreate<byte>
    // copy person over
    Encoding.UTF8.GetBytes(person, buff) |> ignore

    // write the years as 4 chars and years as 1 char
    BinaryPrimitives.WriteUInt32BigEndian(buff.AsSpan().Slice(buff.Length - 6), quarters)
    buff[buff.Length - 2] <- years
    buff[buff.Length - 1] <- byte '|'
    buff

let records = 
    Seq.map proc files
    |> Seq.collect (fun x -> x.People |> Seq.map (fun y -> y, x.Date))
    |> Seq.groupBy fst
    |> Seq.map (fun (name, values) -> values |> Seq.map snd |> buildPerson name)
    |> Seq.toArray

let maxLength = records |> Seq.map (fun x -> x.Length) |> Seq.max

// now we can write the records to a file

if Directory.Exists "output/binary" |> not then
    Directory.CreateDirectory "output/binary" |> ignore

do
    use output = File.Create "output/binary/deans-list.bin"

    // skip over the header
    output.Seek(headerSize, SeekOrigin.Begin) |> ignore

    for record in records do
        output.Write(record, 0, record.Length) |> ignore
        // seek to match max length
        output.Seek(maxLength - record.Length |> int64, SeekOrigin.Current) |> ignore

    output.Seek(0, SeekOrigin.Begin) |> ignore
    let h = Array.zeroCreate<byte> headerSize
    let header = h.AsSpan() 

    [|0x44uy; 0x55uy; 0x42uy; 0x53uy|].CopyTo header // DUBS
    BinaryPrimitives.WriteInt32BigEndian(header.Slice 4, 1) // version
    BinaryPrimitives.WriteInt32BigEndian(header.Slice 8, records.Length) // count
    BinaryPrimitives.WriteInt32BigEndian(header.Slice 12, maxLength) // length
    BinaryPrimitives.WriteInt32BigEndian(header.Slice 16, contextYear) // first year

    output.Write(h, 0, header.Length) |> ignore