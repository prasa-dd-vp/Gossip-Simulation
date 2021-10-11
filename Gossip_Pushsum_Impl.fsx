#time "on"
#r "nuget: Akka.FSharp"
#r "nuget: Akka.TestKit"
open System
open Akka.Actor
open Akka.FSharp

let system = ActorSystem.Create("Gossip")


// Input from Command Line
let mutable nodesCount = fsi.CommandLineArgs.[1] |> int
let topology = fsi.CommandLineArgs.[2]
let algorithm = fsi.CommandLineArgs.[3]
let random = Random(nodesCount)
let timer = System.Diagnostics.Stopwatch()

type TopologyCommands = 
    | BuildTopology of string * list<IActorRef>
    | LineTopology of list<IActorRef>
    | FullTopology of list<IActorRef>
    | ThreeDTopology of list<IActorRef>
    | ConstructionDone

type AlgoCommands = 
    | SetNeighbours of IActorRef * list<IActorRef>
    | ComputePushSum of float * float
    | SpreadGossip
    | InformNeighbours of string
    | LimitReached of string
    
type ActorCommands = 
    | Create
    
type InitiatorCommands = 
    | Init
    | ActorsCreated of list<IActorRef>
    | InitializeTopology of list<IActorRef>
    | TopologyBuilt
    | TerminateNode  
    
let getCubeRoot number = (number|>float)**(1.0/3.0) |> System.Convert.ToInt32

let Topology(mailbox:Actor<_>) = 
    let mutable nodescompleted=0
    let mutable initiatorRef = null
    let mutable neighbors = []
    let rec loop() =actor{
        let! message = mailbox.Receive()
        match message with
        | BuildTopology(topology,actorsList)    ->  printfn "In build topology"
                                                    initiatorRef<-mailbox.Sender()
                                                    if(topology="line") then
                                                        mailbox.Self<!LineTopology(actorsList)
                                                    elif(topology="full") then
                                                        mailbox.Self<!FullTopology(actorsList)
                                                    elif topology="3D" || topology="imp3D" then
                                                        mailbox.Self<!ThreeDTopology(actorsList)
                                                        
        | LineTopology(actorsList)              ->  for i in 0 .. nodesCount-1 do
                                                        neighbors <- []
                                                        if i <> 0 then
                                                            //TODO
                                                            neighbors <- List.append neighbors [actorsList.Item(i-1)] 
                                                        if i <> nodesCount-1 then
                                                            neighbors <- List.append neighbors [actorsList.Item(i+1)]

                                                        actorsList.Item(i)<!SetNeighbours(initiatorRef,neighbors)   

        | FullTopology(actorsList)              ->  for i in 0 .. actorsList.Length-1 do
                                                        neighbors <- actorsList |> List.filter (fun actor -> actor.Path.Name <> actorsList.Item(i).Path.Name) 
                                                        actorsList.Item(i) <! SetNeighbours(initiatorRef, neighbors)  
                                                        
        | ThreeDTopology(actorsList)            ->  //printfn "%d" nodesCount
                                                    let dimension = getCubeRoot nodesCount
                                                    // printfn "%d" dimension
                                                    let planeSize = dimension * dimension
                                                    let mutable idx = 0
                                                    for k in 0 .. dimension-1 do
                                                        for i in 0 .. dimension-1 do
                                                            for j in 0 .. dimension-1 do
                                                                // printfn "Finding neighbors for %d" ((i*dimension) + j + (k*planeSize))
                                                                neighbors <- []

                                                                if j > 0 then
                                                                    idx <- (i*dimension) + (j-1) + (k*planeSize)
                                                                    neighbors <- [actorsList.Item(idx)]

                                                                if j < dimension-1 then
                                                                    idx <- (i*dimension) + (j+1) + (k*planeSize)
                                                                    neighbors <- List.append neighbors [actorsList.Item(idx)]

                                                                if i > 0 then
                                                                    idx <- ((i-1)*dimension) + j + k*planeSize
                                                                    neighbors <- List.append neighbors [actorsList.Item(idx)]

                                                                if i < dimension-1 then
                                                                    idx <- ((i+1)*dimension) + j + k*planeSize
                                                                    neighbors <- List.append neighbors [actorsList.Item(idx)]

                                                                if k > 0 then
                                                                    idx <- (i*dimension) + j + ((k-1)*planeSize)
                                                                    neighbors <- List.append neighbors [actorsList.Item(idx)]

                                                                if k < dimension-1 then
                                                                    idx <- (i*dimension) + j + ((k+1)*planeSize)
                                                                    neighbors <- List.append neighbors [actorsList.Item(idx)]

                                                                if topology = "imp3D" then
                                                                    let mutable randInt = random.Next() 
                                                                    while List.contains (actorsList.Item(randInt % nodesCount)) neighbors && actorsList.Item(randInt % nodesCount) <> actorsList.Item((i*dimension) + j + (k*planeSize)) do
                                                                        randInt<-random.Next()
                                                                    neighbors <- List.append neighbors [actorsList.Item(randInt % nodesCount)]

                                                                idx <- (i*dimension) + j + (k*planeSize)
                                                                // printfn "Neighbors of %d are" idx 
                                                                // for i in 0 .. neighbors.Length-1 do
                                                                    // printfn "%s" (neighbors.Item(i).Path.Name |> string)
                                                                    // printfn ""
                                                                actorsList.Item(idx) <! SetNeighbours(initiatorRef, neighbors)
        
        | ConstructionDone                      ->  if nodescompleted=nodesCount-1 then
                                                        printfn "construction done"
                                                        initiatorRef<!TopologyBuilt
                                                    nodescompleted<-nodescompleted+1
                        
        return! loop()
    }
    loop()
let topologyRef = spawn system "Topology" Topology

let GossipPushsum(mailbox:Actor<_>)=
    let mutable initiatorRef = null
    let mutable neighborList=[]
    let gossipReceiveLimit = 10
    let mutable gossipReceiveCount = 0
    let mutable sum = (mailbox.Self.Path.Name.Split '-').[1] |> float
    let mutable weight = 1.0
    let mutable exitCount = 0

    let rec loop() =actor{
        let! message = mailbox.Receive()
        match message with
        | SetNeighbours(initiator,nodelist)         ->  //printfn "Neighbours received"
                                                        neighborList<-nodelist
                                                        initiatorRef<-initiator
                                                        mailbox.Sender()<!ConstructionDone

        | SpreadGossip                              -> 
                                                        //printfn "Received Gossip by %s" mailbox.Self.Path.Name
                                                        if gossipReceiveCount < gossipReceiveLimit then
                                                            gossipReceiveCount <- gossipReceiveCount + 1
                                                            let mutable next = random.Next()
                                                            if neighborList.Length <> 0 then
                                                                neighborList.Item(next % neighborList.Length) <! SpreadGossip
                                                            
                                                        else
                                                            // isNodeInactive <- true
                                                            initiatorRef <! TerminateNode
                                                            mailbox.Self <! InformNeighbours (mailbox.Self.Path.Name.Split '-').[1]

        | ComputePushSum(recSum,recWeight)          ->  //printfn "Compute push sum %s " mailbox.Self.Path.Name
                                                        let curSum = sum + recSum
                                                        let curWeight = weight + recWeight
                                                        if abs((curSum/curWeight)-(sum/weight))<0.0000000001 then
                                                            exitCount <- exitCount + 1
                                                        else
                                                            exitCount <- 0
                                                            
                                                        if exitCount = 3 then 
                                                            mailbox.Self <! ComputePushSum(recSum,recWeight)
                                                            initiatorRef <! TerminateNode
                                                            mailbox.Self <! InformNeighbours (mailbox.Self.Path.Name.Split '-').[1]
                                                        else
                                                            sum <- curSum
                                                            weight <- curWeight
                                                            if neighborList.Length <> 0 then
                                                                neighborList.Item(random.Next()%neighborList.Length) <! ComputePushSum(sum/2.0,weight/2.0)
        
        | InformNeighbours(inActiveNodeId)        ->    for i in 0 .. neighborList.Length-1 do
                                                            neighborList.Item(i) <! LimitReached(inActiveNodeId)

        | LimitReached(inActiveNodeId)            ->    neighborList <- neighborList |> List.filter (fun (actor) -> ((actor.Path.Name.Split '-').[1]) <> inActiveNodeId) 

                                                        // for i in 0 .. neighborList.Length-1 do
                                                            // printfn "%s" (neighborList.Item(i).Path.Name |> string)
                                                        
                                                        if neighborList.Length = 0 then
                                                            // isNodeInactive <- true
                                                            initiatorRef <! TerminateNode
                                                        else
                                                            if algorithm = "gossip" then
                                                                mailbox.Self <! SpreadGossip
                                                            else if algorithm = "pushsum" then
                                                                mailbox.Self <! ComputePushSum(sum, weight)
        
        return! loop()
    }
    loop()

let Actor (mailbox:Actor<_>) = 
    let rec loop() = actor {
        let! message = mailbox.Receive()
        match message with
        | Create -> printfn("In creation")
                    let mutable actors = []
                    actors <- [for i in 0 .. nodesCount-1 do yield(spawn system ("Actor-" + (string i)) GossipPushsum)]
                    mailbox.Sender() <! ActorsCreated(actors)

        return! loop()
    }
    loop()
let actorRef = spawn system "Actor" Actor  


let Initiator (mailbox:Actor<_>) = 
    let mutable nodeGossipedCount = 0
    let mutable actorList = []
    let rec loop () = actor {
        let! message = mailbox.Receive()
        match message with 
        | Init                          ->  if topology = "3D" || topology = "imp3D" then
                                                let cbrt = getCubeRoot nodesCount
                                                nodesCount <- cbrt*cbrt*cbrt
                                                printfn "%d" nodesCount
                                            actorRef <! Create

        | ActorsCreated(actorRefList)   ->  printfn("Actor created")
                                            actorList <- actorRefList
                                            mailbox.Self <! InitializeTopology(actorList)
        
        | InitializeTopology(actorList) ->  printfn("Initializing topology")
                                            topologyRef <! BuildTopology(topology, actorList)
        
        | TopologyBuilt                 ->  printfn("Topology Constructed")
                                            if algorithm = "gossip" then
                                                actorList.Item(random.Next() % nodesCount) <! SpreadGossip
                                                timer.Start()
                                            elif algorithm = "pushsum" then
                                                let ind = random.Next() % nodesCount
                                                actorList.Item(ind) <! ComputePushSum((float) ind,1.0)
                                                timer.Start()      

        | TerminateNode                 ->  let mutable sender = mailbox.Sender().Path.Name
                                            // printfn "Terminated %s" sender
                                            nodeGossipedCount <- nodeGossipedCount + 1
                                            if nodeGossipedCount = nodesCount then 
                                                mailbox.Context.System.Terminate() |> ignore
                                                printfn "%s,%s,%i,%i" algorithm topology nodesCount timer.ElapsedMilliseconds
        
        return! loop()
    }
    loop()

// Start of the algorithm - spawn Boss, the delgator
let intiator = spawn system "Initialize" Initiator
intiator <! Init
system.WhenTerminated.Wait()