// Peer to peer system - Chord Protocol simulation using F# and actor model
// Kavya Gopal (UFID: 6581 2209)
// Rema Veranna Gowda (UFID: 94628005)

#r "nuget: Akka.FSharp"
#r "nuget: Akka.TestKit"

open System
open Akka.Actor
open Akka.FSharp
open System.Security.Cryptography

// Finger table of size m.
// For each node, we create a finger table and keep node ID and references of some nodes in the chord
type FingerTableInfo = {
  mutable Initiate:int;
  mutable NodeIDKey:int;
  mutable RefToNode:IActorRef
}

type ActionType =
    | JoinChord of IActorRef 
    | UpdateChordRing of IActorRef
    | Stabilize
    | FindSucessor of int*String*IActorRef
    | ImmediateSucessor of IActorRef*String*Boolean
    | FindCurrentPredecessor
    | FindPrecedessor of IActorRef*Boolean
    | UpdateFingerTable
    | ShowFingerTable
    | InitiateMessaging
    | MessageInHop of int*int*IActorRef
    

type FindNode =
    | Initiate
    | Recieved
    | MessageRecieved of int
    | PrintAndStartMsg

let system = ActorSystem.Create("System")

// Create a random string.
// This string will be hashed to get Node ID for all nodes in chord
let ranStr n = 
        let r = Random()
        let chars = Array.concat([[|'a' .. 'z'|];[|'A' .. 'Z'|];[|'0' .. '9'|]])
        let sz = Array.length chars in
        String(Array.init n (fun _ -> chars.[r.Next sz]))

// Generate random node ID's in with m bit
// We are creating a nodeID with 10 digits
let getRandomNodeID (m:int)= 
        let randString = ranStr(32)
        let nodeID =  Math.Abs(BitConverter.ToInt32(((new SHA256Managed()).ComputeHash(System.Text.Encoding.ASCII.GetBytes ("rveerannagowda;"+randString))), 0))
        // printfn "%i" nodeID
        nodeID


//keeps track of hop count,nodes joining,neighbour count,messages delivered
type Counter(nodes: IActorRef [], numNodes: int, numofReq: int) =
    inherit Actor()
    let mutable joinCounter = 0
    let mutable peerCounter = 0
    let mutable numOfHops = 0
    let mutable numOfMsgDelivered = 0
    override x.OnReceive(msg) =
        match msg :?> FindNode with
            // Initiaates adding nodes to chord network
            | Initiate -> 
                nodes.[0]<! JoinChord(nodes.[0])
            
            // When message reaches desitnation, count the number of hops and the number of messages.
            // If all messages reached destination, print average hops for the messages to be sent and terminate.
            | MessageRecieved hopCount-> 
                numOfMsgDelivered <- numOfMsgDelivered + 1
                numOfHops <- hopCount + numOfHops
                if numOfMsgDelivered = (numNodes * numofReq) then
                    printfn "Total hops in Chord: %d" numOfHops
                    let averageHops =  ((numOfHops |> float)/ ((numNodes |> float) * (numofReq |> float)))
                    printfn "Average Hops: %A " averageHops
                    Environment.Exit(0)

            //Start sending message - finding node for the key
            | PrintAndStartMsg ->
                peerCounter<-peerCounter+1
                if peerCounter< nodes.Length then
                    nodes.[peerCounter] <! ShowFingerTable
                if peerCounter=nodes.Length then
                    for i in 1..nodes.Length do
                        nodes.[i-1]<!InitiateMessaging

            | Recieved ->
                // Join node to chord network till we reach specified number of nodes
                joinCounter<-joinCounter+1
                if joinCounter< nodes.Length then
                    nodes.[joinCounter]<! JoinChord(nodes.[joinCounter-1])
                
                if joinCounter= nodes.Length then
                    system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(100.0),nodes.[0],ShowFingerTable,x.Self);

            

type Node(nid: int , m: int)=
    inherit Actor()

    // Initialize predecessor, successor and reference to current actor/node.
    let mutable nodeReference:IActorRef = null
    let mutable predecessor:IActorRef = null
    let mutable successor:IActorRef = null

    // Create finger table for each node
    let createFingerTable =
        let mutable ftableArray=Array.empty
        for i in 1..m do
            ftableArray <-[|{  Initiate=int (nid + pown 2 i-1)%(pown 2 m);NodeIDKey=Int32.MaxValue;RefToNode=null }|] |>Array.append ftableArray
        ftableArray
    let mutable fingerTable = createFingerTable

    // Checks if the node is between begin and end
    let rec checkRange (id:int) (beginning:int) (ending:int)=
        if beginning < ending then
            id > beginning && id < ending
        elif beginning > ending then
            not (checkRange id ending beginning) && (id <> beginning) && (id<>ending)
        else
            (id <> beginning) && (id <> ending)

    // check if the key we are searching for is between the node(beginning) and its successor(ending)   
    let rec belongs (id:int) (beginning:int) (ending:int)=
            if beginning < ending then
                id > beginning && id<=ending
            else if beginning > ending then
                not (belongs id ending beginning)
            else 
                true

    // Returns the precceding ndoe
    let immediatePrecedingNode (id:int)=
        let mutable value=null
        let mutable flag = false
        let mutable count = m
        while not flag && count >=1 do
            let isTrue:bool = checkRange fingerTable.[count-1].NodeIDKey nid id
            if isTrue then
                value <- fingerTable.[count-1].RefToNode
                flag <- true
            count <- count-1
        value

    // Loop for all the messagages
    override x.OnReceive(msg) =
         match msg :?> ActionType with


            // Generate a key using same hash function.
            // Search the node that will have the key in the chord network
            | InitiateMessaging ->
                let msgID = getRandomNodeID m
                let initialHop = 0
                for i in 1..10 do
                    system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(float 100.0),x.Self,MessageInHop(msgID,initialHop,x.Self),x.Self);
            
            //  For a new node, find the location in chord it should be a part of and update the finger table and predecessor and successor accordingly
            | JoinChord ref -> 
                nodeReference<-x.Sender
                if ref.Path.Name.Equals(x.Self.Path.Name) then
                    predecessor <- null
                    successor <- x.Self
                    // Updaate finger table for the new chord
                    for i in 1..m do
                        fingerTable.[i-1].NodeIDKey <- nid
                        fingerTable.[i-1].RefToNode <- x.Self
                    // Join the network
                    nodeReference <! Recieved
                    system.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromMilliseconds(50.0),TimeSpan.FromMilliseconds(80.0),x.Self,Stabilize,x.Self);
                    system.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromMilliseconds(60.0),TimeSpan.FromMilliseconds(90.0),x.Self,UpdateFingerTable,x.Self);
                else
                    predecessor <- null
                    ref <! FindSucessor(nid,"join",x.Self)

            // maintain chord ntewrok whenever there ia an update(node entering or leaving)
            | Stabilize ->
                successor <! FindCurrentPredecessor
            
            // find predecessor for the current node
            | FindCurrentPredecessor ->
                if  isNull predecessor then
                    let pre = x.Self
                    let valid = false
                    x.Sender <! FindPrecedessor(pre,valid)
                else
                    let pre = predecessor
                    let valid = true
                    x.Sender <! FindPrecedessor(pre,valid)
            | FindPrecedessor(pred:IActorRef,valid:bool) ->
                if valid then
                    let preId = int pred.Path.Name
                    let sucId = int successor.Path.Name
                    if (checkRange preId nid sucId) then
                        successor <- pred
                successor <! UpdateChordRing(x.Self)

            // find immediate successor and add to network and mark as received
            | ImmediateSucessor(succ:IActorRef,reason:String,valid:Boolean)->
                if (reason = "join") then
                    if valid then
                        successor<-succ
                    nodeReference<! Recieved
                    system.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromMilliseconds(50.0),TimeSpan.FromMilliseconds(80.0),x.Self,Stabilize,x.Self);
                    system.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromMilliseconds(60.0),TimeSpan.FromMilliseconds(90.0),x.Self,Stabilize,x.Self);

                else
                    let index = int (reason.Split (".")).[2]
                    if valid then
                        fingerTable.[index].RefToNode <- succ
                        fingerTable.[index].NodeIDKey <-int succ.Path.Name

            // Find successor of node
            | FindSucessor(id,taskType,ref) ->
       
                    let succNid = int successor.Path.Name
                    if belongs id nid succNid then

                        if isNull successor then
                            let sucessorType=x.Self
                            let valid=false
                            ref<! ImmediateSucessor(sucessorType,taskType,valid)
                        else
                            let returnSuccessor=successor
                            let valid=true
                            ref<! ImmediateSucessor(returnSuccessor,taskType,valid)
                    else
                        let nextHop = immediatePrecedingNode(id)
                        if not (isNull nextHop) then
                            nextHop <! FindSucessor(id,taskType,ref)
                        else
                            successor <! FindSucessor(id,taskType,ref)
            
            // Calling the print and starting message 
            | ShowFingerTable ->  
                let mutable predName:string=null
                if not (isNull predecessor) then
                    predName<-predecessor.Path.Name

                x.Sender<! PrintAndStartMsg

            // Here updating the finger table
            | UpdateFingerTable -> 
                let i = Random().Next(0, m)
                x.Self <! FindSucessor(fingerTable.[i].Initiate,"finger.update."+(string i),x.Self) 

            // when a new node enters ensure to add to chord and update finger table
            | UpdateChordRing(ref:IActorRef) ->
                if isNull predecessor then
                    predecessor <- ref
                else
                    let preId = int predecessor.Path.Name
                    let refId = int ref.Path.Name
                    if (checkRange refId preId nid) then
                        predecessor <-ref            

            // Keep a count of hops to find key in the chord network
            | MessageInHop(msgID:int,hopCount:int,ref:IActorRef) ->
                let visitCount = hopCount + 1
                let maximum = int (pown 2 m)
                if (visitCount = maximum) then
                    nodeReference <! MessageRecieved(visitCount)
                else
                    let sucId = int successor.Path.Name
                    if belongs msgID nid sucId then    
                        nodeReference<!MessageRecieved(visitCount)
                    else
                        let closestHop = immediatePrecedingNode msgID
                        if (not (isNull closestHop)) then 
                            closestHop <! MessageInHop(msgID,visitCount,ref) 
                        else 
                            successor <! MessageInHop(msgID,visitCount,ref)

// Getting command line arguments
let arguments : string array = fsi.CommandLineArgs |> Array.tail
let numOfNodes =  arguments.[0] |> int
let numOfRequests = arguments.[1] |> int

// m is the number of entries that we have in finger table
let m = int(ceil (Math.Log(float numOfNodes)/Math.Log(2.0)))
// List of node numbers 
let mutable nodeID= []
let mutable id = -1
for i in 1..numOfNodes do
    id <- getRandomNodeID m
    // printfn "%i" id
    while(not (List.contains id nodeID)) do 
        id <- getRandomNodeID m
        nodeID<- [id] |>List.append nodeID

// Create an array of actors
let mutable nodeArrayOfActors = Array.empty
for i in nodeID do
    nodeArrayOfActors<-[|system.ActorOf(Props.Create(typeof<Node>, i, m),""+ string (i))|] |>Array.append nodeArrayOfActors 

let ReftoNode = system.ActorOf(Props.Create(typeof<Counter>, nodeArrayOfActors,numOfNodes,numOfRequests), "nodeReference")

ReftoNode<!Initiate

Console.ReadLine() |> ignore