#r "nuget: SIMDIntCompression, 1.0.0-alpha2"

open System.IO
open System
open System.Buffers.Binary
open SIMDIntCompression
open System.Collections.Generic
open      System.Runtime.InteropServices 

let source = "./output/binary/deans-list.bin"



let headerSize = 32

type Header =
    { Version: int
      YearStart: int
      Count: int
      Size: int }

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

type IndexField<'TKey> =
    { Name: string
      Index: Dictionary<'TKey, int ResizeArray> }

module IndexField =
    let create name = { Name = name; Index = Dictionary<'TKey, 'TValue ResizeArray>() }
    let add field key value =
        match field.Index.TryGetValue key with
        | true, v -> v.Add value
        | false, _ -> field.Index.Add(key, ResizeArray [ value ])
    
    let get field key = field.Index[key]

    let list field key =
        field.Index[key];

    let compressedList field key =
        let buffer = Array.zeroCreate<byte> 50_000
        let w = BinaryPacking128.Encode(CollectionsMarshal.AsSpan(field.Index[key]), buffer.AsSpan())
        buffer[..w]

    let dump field (outStream: System.IO.Stream) =
        // 2 bytes for field name size
        let name = System.Text.Encoding.UTF8.GetBytes(field.Name)
        outStream.Write(BitConverter.GetBytes(uint16 name.Length), 0, 2) |> ignore
        // write the name itself
        outStream.Write(name, 0, name.Length) |> ignore

        // write the number of keys in 4 bytes
        outStream.Write(BitConverter.GetBytes(uint32 field.Index.Count), 0, 4) |> ignore

        let mutable written = 2  + name.Length + 4

        let buffer = Array.zeroCreate<byte> 50_000

        for name in field.Index.Keys do
            let w = BinaryPacking128.Encode(CollectionsMarshal.AsSpan(field.Index[name]), buffer.AsSpan())
            // 2 bytes for field name size
            let name = System.Text.Encoding.UTF8.GetBytes(string name)
            outStream.Write(BitConverter.GetBytes(uint16 name.Length), 0, 2) |> ignore
            // write the name itself
            outStream.Write(name, 0, name.Length) |> ignore
            // write the number of bytes of compressed data in 4 bytes
            outStream.Write(BitConverter.GetBytes(uint32 w), 0, 4) |> ignore
            // write the compressed data
            outStream.Write(buffer, 0, w) |> ignore
            written <- written + 2 + name.Length + 4 + w

    let readStr (inStream: System.IO.Stream) =
        // first read the name
        let nameSize = Array.zeroCreate<byte> 2
        inStream.Read(nameSize, 0, 2) |> ignore
        let nameSize = BitConverter.ToUInt16(nameSize, 0) |> int
        let name = Array.zeroCreate<byte> nameSize
        inStream.Read(name, 0, nameSize) |> ignore
        let name = System.Text.Encoding.UTF8.GetString(name)

        // read the number of keys
        let keyCount = Array.zeroCreate<byte> 4
        inStream.Read(keyCount, 0, 4) |> ignore
        let keyCount = BitConverter.ToUInt32(keyCount, 0)

        let field = create name

        for i in 0 .. int keyCount - 1 do
            // read the key
            let keySize = Array.zeroCreate<byte> 2
            inStream.Read(keySize, 0, 2) |> ignore
            let keySize = BitConverter.ToUInt16(keySize, 0) |> int
            let key = Array.zeroCreate<byte> keySize
            inStream.Read(key, 0, keySize) |> ignore
            let key = System.Text.Encoding.UTF8.GetString(key)

            // read the compressed data size
            let dataSize = Array.zeroCreate<byte> 4
            inStream.Read(dataSize, 0, 4) |> ignore
            let dataSize = BitConverter.ToUInt32(dataSize, 0) |> int

            let buffer = Array.zeroCreate<byte> dataSize
            inStream.Read(buffer, 0, dataSize) |> ignore

            let nValues = BinaryPacking128.GetDecompressedLength(buffer.AsSpan(0, dataSize))

            let values = Array.zeroCreate<int> nValues
            
            BinaryPacking128.Decode(buffer.AsSpan(0, dataSize), values.AsSpan()) |> ignore

            field.Index.Add(key, ResizeArray values)

        field

    let readInt (inStream: System.IO.Stream) =
        // first read the name
        let nameSize = Array.zeroCreate<byte> 2
        inStream.Read(nameSize, 0, 2) |> ignore
        let nameSize = BitConverter.ToUInt16(nameSize, 0) |> int
        let name = Array.zeroCreate<byte> nameSize
        inStream.Read(name, 0, nameSize) |> ignore
        let name = System.Text.Encoding.UTF8.GetString(name)

        // read the number of keys
        let keyCount = Array.zeroCreate<byte> 4
        inStream.Read(keyCount, 0, 4) |> ignore
        let keyCount = BitConverter.ToUInt32(keyCount, 0)

        let field = create name

        for i in 0 .. int keyCount - 1 do
            // read the key
            let keySize = Array.zeroCreate<byte> 2
            inStream.Read(keySize, 0, 2) |> ignore
            let keySize = BitConverter.ToUInt16(keySize, 0) |> int
            let key = Array.zeroCreate<byte> keySize
            inStream.Read(key, 0, keySize) |> ignore
            let key = System.Text.Encoding.UTF8.GetString(key) |> int

            // read the compressed data size
            let dataSize = Array.zeroCreate<byte> 4
            inStream.Read(dataSize, 0, 4) |> ignore
            let dataSize = BitConverter.ToUInt32(dataSize, 0) |> int

            let buffer = Array.zeroCreate<byte> dataSize
            inStream.Read(buffer, 0, dataSize) |> ignore

            let nValues = BinaryPacking128.GetDecompressedLength(buffer.AsSpan(0, dataSize))

            let values = Array.zeroCreate<int> nValues
            
            BinaryPacking128.Decode(buffer.AsSpan(0, dataSize), values.AsSpan()) |> ignore

            field.Index.Add(key, ResizeArray values)

        field

    let readAll () =

        let compressedIndexFile = File.OpenRead "./output/binary/index-compressed.bin"

        {|
            FirstNames = readStr compressedIndexFile
            MiddleNames = readStr compressedIndexFile
            LastNames = readStr compressedIndexFile
            Colleges = readInt compressedIndexFile
            EntriesQuarter = readInt compressedIndexFile
            EntriesYear = readInt compressedIndexFile
        |}


// only execute if this is the script being run
if File.Exists source |> not then
    let fs = File.OpenRead source
    printfn "Indexing..."

    let firstNames: string IndexField = IndexField.create "FirstName"
    let middleNames = IndexField.create "MiddleName"
    let lastNames = IndexField.create "LastName"
    let colleges = IndexField.create "College"
    let entriesQuarter = IndexField.create "Quarter"
    let entriesYear = IndexField.create "Year"

    let header = readHeader fs

    for i in 0 .. header.Count - 1 do
        let rawBuffer = Array.zeroCreate<byte> header.Size
        fs.Read(rawBuffer, 0, header.Size) |> ignore

        // count the '|' characters in the buffer
        let asString = System.Text.Encoding.UTF8.GetString(rawBuffer).Split('|')

        IndexField.add lastNames  asString[0] i
        IndexField.add firstNames  asString[1] i
        IndexField.add middleNames  asString[2] i
        IndexField.add colleges (asString[3][0] |> int) i

        let lastSep = rawBuffer.AsSpan().LastIndexOf(byte '|')
        if lastSep = -1 then
            failwith "Invalid record format"

        let quarterMask = BinaryPrimitives.ReadUInt32BigEndian(rawBuffer.AsSpan().Slice(lastSep - 5, 4))
        let yearMask = rawBuffer[lastSep - 1]

        

        for j in 0 .. 31 do
            if quarterMask &&& (1u <<< j) <> 0u then
                IndexField.add entriesQuarter j i
        
        for j in 0 .. 7 do
            if yearMask &&& (1uy <<< j) <> 0uy then
                IndexField.add entriesYear j i

    // write compressed index

    let compressedIndexFile = File.Create "./output/binary/index-compressed.bin" // compressed

    // go through all our fields

    IndexField.dump firstNames compressedIndexFile
    IndexField.dump middleNames compressedIndexFile
    IndexField.dump lastNames compressedIndexFile
    IndexField.dump colleges compressedIndexFile
    IndexField.dump entriesQuarter compressedIndexFile
    IndexField.dump entriesYear compressedIndexFile

    compressedIndexFile.Close()