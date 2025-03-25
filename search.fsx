#load "colleges.fsx"
#load "data.fsx"
#load "index.fsx"

open Colleges
open Data
open Index

open System

let index = IndexField.readAll ()

let start = DateTime.Now

let a = IndexField.get index.FirstNames "DANIEL" |> set
let d = IndexField.get index.LastNames "ZHANG" |> set

let ad = Set.intersect a d

let res = 
    Seq.toArray ad 
    |> pull

let end' = DateTime.Now
printfn "Elapsed: %A" (end' - start)