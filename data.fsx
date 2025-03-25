#load "colleges.fsx"

open System.IO
open System
open System.Buffers.Binary


let source = "./output/binary/deans-list.bin"

let headerSize = 32

type Header =
    { Version: int
      YearStart: int
      Count: int
      Size: int }

type Record =
    { FirstName: string
      MiddleName: string
      LastName: string
      College: string
      Quarters: string list
      Years: int list }

let readHeader (input: FileStream) =
    let buffer = Array.zeroCreate<byte> headerSize
    input.Read(buffer, 0, headerSize) |> ignore
    let buffer = buffer.AsSpan()

    let magic = BinaryPrimitives.ReadUInt32BigEndian buffer

    if magic <> 0x44554253u then
        failwith "Invalid magic number"

    let version = BinaryPrimitives.ReadInt32BigEndian(buffer.Slice 4)
    let count = BinaryPrimitives.ReadInt32BigEndian(buffer.Slice 8)
    let size = BinaryPrimitives.ReadInt32BigEndian(buffer.Slice 12)
    let yearStart = BinaryPrimitives.ReadInt32BigEndian(buffer.Slice 16)

    { Version = version
      YearStart = yearStart
      Count = count
      Size = size }

let pull (indexes: int array) =
    // open a file stream and start reading
    // setup with sequential scan
    use fs = new FileStream(source, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, FileOptions.SequentialScan)

    let header = readHeader fs

    let locations = 
        Seq.where ((>) header.Count) indexes
        |> Seq.sort
        // find corresponding locations in file
        |> Seq.map (fun x -> headerSize + x * header.Size)

    [|
        for location in locations do
            fs.Seek(location, SeekOrigin.Begin) |> ignore
            let rawBuffer = Array.zeroCreate<byte> header.Size
            fs.Read(rawBuffer, 0, header.Size) |> ignore

            // count the '|' characters in the buffer
            let asString = System.Text.Encoding.UTF8.GetString(rawBuffer).Split('|')

            let lastName = asString[0]
            let firstName = asString[1]
            let middleName = asString[2]
            let college = asString[3][0]

            let lastSep = rawBuffer.AsSpan().LastIndexOf(byte '|')

            if lastSep = -1 then
                failwith "Invalid record format"

            let quarterMask =
                BinaryPrimitives.ReadUInt32BigEndian(rawBuffer.AsSpan().Slice(lastSep - 5, 4))

            let yearMask = rawBuffer[lastSep - 1]

            let quarters =
                [ for j in 0..31 do
                    if quarterMask &&& (1u <<< j) <> 0u then
                        j ]
                |> List.map (fun j -> j / 4, j % 4)
                |> List.map (fun (y, q) ->
                    header.YearStart + y,
                    match q with
                    | 0 -> "WI"
                    | 1 -> "SP"
                    | 2 -> "SU"
                    | _ -> "AU")
                |> List.map (fun (y, q) -> sprintf "%d%s" (y%100) q)

            let years =
                [ for j in 0..7 do
                    if yearMask &&& (1uy <<< j) <> 0uy then
                        j ]
                |> List.map ((+) header.YearStart)

            { FirstName = firstName
              MiddleName = middleName
              LastName = lastName
              College = Colleges.collegeMap.TryFind college |> Option.defaultValue "Unknown"
              Quarters = quarters
              Years = years }
    |]