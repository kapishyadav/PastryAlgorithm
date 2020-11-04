#time "on"
#r "nuget: Akka.FSharp"
#r "nuget: Akka.TestKit"

open System
open Akka.FSharp
open Akka.Actor
open Akka.Configuration
open System.Threading

let system =
    System.create "system" (Configuration.defaultConfig ())

type command =
    | CreateNetwork of string * int
    | Route of string * string * int
    | AddToNetwork of string * int
    | UpdateTable of string []
    | Initiate of int * int

let mutable actorDict:Map <String, IActorRef> = Map.empty
let mutable hopsDict:Map <String, double[]>  = Map.empty
let mutable num_nodes = 0
let mutable num_requests = 0
let mutable num_digits = 0

let node (mailbox: Actor<_>) =
    let mutable id = ""
    let mutable num_rows = 0
    let mutable num_cols = 16
    let mutable routing_table: string[,] = Array2D.zeroCreate 0 0
    let mutable leaf_set = Set.empty
    let mutable common_prefix_len = 0
    let mutable onRow = 0

    let rec loop () =
        actor {
            let! message = mailbox.Receive()

            match message with
            | CreateNetwork (x, d) ->
                id <- x
                //printfn "In ConstructD ID: %s" id
                num_rows <- d
                routing_table<-Array2D.zeroCreate num_rows num_cols
                let mutable counter = 0
                let number = Int32.Parse(x, Globalization.NumberStyles.HexNumber)
                let mutable leftCtr = number
                let mutable rightCtr = number
                while counter < 8 do
                    if leftCtr = 0 then leftCtr <- actorDict.Count - 1
                    leaf_set <- leaf_set.Add(string leftCtr)
                    counter <- counter + 1
                    leftCtr <- leftCtr - 1
                while counter < 16 do
                    if rightCtr = actorDict.Count - 1 then rightCtr <- 0
                    leaf_set <- leaf_set.Add(string rightCtr)
                    counter <- counter + 1
                    rightCtr <- rightCtr + 1
            | AddToNetwork (key, currentIndex) ->
                let mutable x = 0
                let mutable y = 0
                let mutable z = currentIndex
                while key.[x] = id.[x] do
                    x <- x + 1
                common_prefix_len <- x
                let mutable routing_row = Array.zeroCreate 0
                while z <= common_prefix_len do
                    routing_row <- routing_table.[z, *]
                    let idx1 = Int32.Parse(id.[common_prefix_len].ToString(), Globalization.NumberStyles.HexNumber)
                    routing_row.[idx1] <- id
                    actorDict.[key] <! UpdateTable(routing_row)
                    z <- z + 1
                let mutable rt_row = common_prefix_len

                let mutable rt_col =
                    Int32.Parse(key.[common_prefix_len].ToString(), Globalization.NumberStyles.HexNumber)

                if routing_table.[rt_row, rt_col] = null then
                    routing_table.[rt_row, rt_col] <- key
                else
                    actorDict.[routing_table.[rt_row, rt_col]]
                    <! AddToNetwork(key, z)
            | UpdateTable (r) ->
                routing_table.[onRow, *] <- r
                onRow <- onRow + 1
            | Route (key, source, hops) ->
                if key = id then
                    if hopsDict.ContainsKey(source) then
                        let mutable actor_hops = hopsDict.[source]
                        let sum = double actor_hops.[1]
                        let average_hops = double actor_hops.[0]
                        actor_hops.[0] <- ((average_hops * sum) + double hops) / (sum + 1.0)
                        actor_hops.[1] <- sum + 1.0
                        hopsDict <- hopsDict.Add(source, actor_hops)
                    else
                        let temp_array = [| double hops; 1.0 |]
                        hopsDict <- hopsDict.Add(source, temp_array)
                elif leaf_set.Contains(key) then
                    actorDict.[key] <! Route(key, source, hops + 1)
                else
                    let mutable x = 0
                    let mutable y = 0
                    while key.[x] = id.[x] do
                        x <- x + 1
                    common_prefix_len <- x
               
                    let mutable rt_row = common_prefix_len

                    let mutable rt_col =
                        Int32.Parse(key.[common_prefix_len].ToString(), Globalization.NumberStyles.HexNumber)

                    if routing_table.[rt_row, rt_col] = null then rt_col <- 0
                    actorDict.[routing_table.[rt_row, rt_col]]
                    <! Route(key, source, hops + 1)
            | _ -> ()

            return! loop ()
        }

    loop ()

let parent (mailbox: Actor<_>) =
    let rec loop () =
        actor {
            let! message = mailbox.Receive()

            match message with
            | Initiate (nodes, requests) ->
                num_nodes <- nodes
                num_requests <- requests
                num_digits <- int (ceil (Math.Log(double num_nodes) / Math.Log(double 16)))
                printfn "Network Construction Initiated."
                let mutable node_id = ""
                let mutable hex_number = ""
                let mutable length = 0
                node_id <- String.replicate num_digits "0"
              
                let mutable actor =
                    spawn system (sprintf "actor%s" node_id) node

                actorDict <- actorDict.Add(node_id, actor)

                actor <! CreateNetwork(node_id, num_digits)
                for x = 2 to num_nodes do
                    if x = num_nodes / 4
                    then printfn "25 percent of network is constructed."
                    elif x = num_nodes / 2
                    then printfn "50 percent of network is constructed."
                    elif x = num_nodes * 3 / 4
                    then printfn "75 percent of network is constructed."

                    hex_number <- x.ToString("X")
                    length <- hex_number.Length
                    node_id <- (String.replicate (num_digits - length) "0") + hex_number

                    actor <- spawn system (sprintf "actor%s" node_id) node
                    actor <! CreateNetwork(node_id, num_digits)
                    actorDict <- actorDict.Add(node_id, actor)
                    actorDict.[String.replicate num_digits "0"] <! AddToNetwork(node_id, 0)
                    Thread.Sleep(5)
                let mutable flag = false
                Thread.Sleep(1000)
                printfn "Network has been built."
                let mutable array_of_actors = Array.empty
                for element in actorDict do
                    let t = [| element.Key |]
                    array_of_actors <- Array.append array_of_actors t
                printfn "Processing Requests"
                let mutable z = 1
                let mutable target_addr = ""
                let mutable counter = 0
                while z <= num_requests do
                    for source_addr in array_of_actors do
                        counter <- counter + 1
                        target_addr <- source_addr
                        while target_addr = source_addr do
                            target_addr <- array_of_actors.[Random().Next(0, num_nodes)]
                        actorDict.[source_addr]
                        <! Route(target_addr, source_addr, 0)
                        Thread.Sleep(5)
                    printfn "Each node has performed %x requests" z
                    z <- z + 1
                Thread.Sleep 1000    
                printfn "Requests processed"
                let mutable total_hops = double 0
                printfn "Computing average hop size"
                printfn "Hop Map %A" hopsDict
                for element in hopsDict do
                    total_hops <- total_hops + double element.Value.[0]
                printfn "Avg Hop Size: %f" (total_hops / double hopsDict.Count)
            | _ -> ()

            return! loop ()
        }

    loop ()

num_nodes <- int fsi.CommandLineArgs.[1]
num_requests <- int fsi.CommandLineArgs.[2]



let parentId = spawn system "parent" parent
parentId <! Initiate(num_nodes, num_requests)
Console.ReadLine()