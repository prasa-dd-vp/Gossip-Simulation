#time "on"
#r "nuget: Akka.FSharp"
#r "nuget: Akka.TestKit"
open System
open Akka.Actor
open Akka.FSharp
open Akka.Configuration

let system = ActorSystem.Create("Gossip")


// Input from Command Line
let mutable nodesCount = fsi.CommandLineArgs.[1] |> int
let topology = fsi.CommandLineArgs.[2]
let algorithm = fsi.CommandLineArgs.[3]
let random = Random(nodesCount)
let timer = System.Diagnostics.Stopwatch()
//TODO
"""
if topology = "imp2D" || topology = "2D" then
    nodes <- roundOffNodes nodes
"""
type TopologyCommands = 
    | BuildTopology of string * list<IActorRef>
    | LineTopology of list<IActorRef>
    | FullTopology of list<IActorRef>
    | ThreeDTopology of list<IActorRef>
    | ConstructionDone

type GossipCommands = 
    | SetNeighbours of IActorRef * list<IActorRef>
    | ReceiveGossip
    | SendGossip
    | InformNeighbours
    | LimitReached
    
type ActorCommands = 
    | Create
    
type InitiatorCommands = 
    | Init
    | ActorsCreated of list<IActorRef>
    | InitializeTopology of list<IActorRef>
    | TopologyBuilt
    | TerminateNode  
    

let Topology(mailbox:Actor<_>) = 
    let mutable nodescompleted=0
    let mutable initiatorRef = null
    let mutable neighbours = []
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
                                                        neighbours <- []
                                                        if i <> 0 then
                                                            neighbours <- List.append neighbours [actorsList.Item(i-1)] 
                                                        if i <> nodesCount-1 then
                                                            neighbours <- List.append neighbours [actorsList.Item(i+1)]  
                                                        actorsList.Item(i)<!SetNeighbours(initiatorRef,neighbours)   

        | FullTopology(actorsList)              ->  let mutable actorName = ""
                                                    let mutable actorId = 0
                                                    for i in 0 .. actorsList.Length-1 do
                                                        actorName <- actorsList.Item(i).Path.Name
                                                        actorId  <- (actorName.Split '-').[1] |> int
                                                        neighbours <- actorsList 
                                                                        |> List.indexed 
                                                                        |> List.filter (fun (i, _) -> i <> actorId) 
                                                                        |> List.map snd

                                                        actorsList.Item(i) <! SetNeighbours(initiatorRef,neighbours)  
                                                        
        | ThreeDTopology(actorsList)            ->  let n= sqrt(nodesCount|>float) |> int
                                                    nodesCount<-n*n
                                                    for i in 0 .. nodesCount-1 do
                                                        neighbours <- []
                                                        if i%n <> 0 then
                                                            neighbours <- List.append neighbours [actorsList.Item(i-1)]
                                                        if i%n <> n-1 then
                                                            neighbours <- List.append neighbours [actorsList.Item(i+1)]
                                                        if i/n <> 0 then 
                                                            neighbours <- List.append neighbours [actorsList.Item(i-n)]
                                                        if i/n <> n-1 then 
                                                            neighbours <- List.append neighbours [actorsList.Item(i+n)]
                                                        if topology = "imp2D" then 
                                                            // let mutable next = rnd.Next()
                                                            // while temp.ContainsKey(next%nodecount) do
                                                            //     next <- rnd.Next()
                                                            neighbours <- List.append neighbours [actorsList.Item(nodesCount-1-i)]
                                                        actorsList.Item(i)<!SetNeighbours(initiatorRef,neighbours) 
        
        | ConstructionDone                      ->  if nodescompleted=nodesCount-1 then
                                                        initiatorRef<!TopologyBuilt
                                                    nodescompleted<-nodescompleted+1
                        
        return! loop()
    }
    loop()
let topologyRef = spawn system "Topology" Topology

let Gossip(mailbox:Actor<_>)=
    let mutable initiatorRef = null
    let mutable neigbhours=[]
    let gossipReceiveLimit = 1000
    let gossipSendLimit = 10
    let mutable gossipReceiveCount = 0
    let mutable gossipSentCount = 0
    let mutable isNodeInactive = false
    let mutable inactiveNeighbours = 0
    // let id = mailbox.Self.Path.Name |> int

    let rec loop() =actor{
        let! message = mailbox.Receive()
        // printfn "%A %i" message id
        match message with
        | SetNeighbours(initiator,nodelist)         ->  printfn "Neighbours received"
                                                        neigbhours<-nodelist
                                                        initiatorRef<-initiator
                                                        mailbox.Sender()<!ConstructionDone
        
        | ReceiveGossip                             ->  if not isNodeInactive then
                                                            printfn "Received Gossip by %s" mailbox.Self.Path.Name
                                                            if gossipReceiveCount < gossipReceiveLimit then
                                                                gossipReceiveCount <- gossipReceiveCount + 1
                                                                mailbox.Self <! SendGossip
                                                                
                                                            else if not isNodeInactive then
                                                                isNodeInactive <- true
                                                                initiatorRef <! TerminateNode
                                                                mailbox.Self <! InformNeighbours

        | SendGossip                                ->  if not isNodeInactive then
                                                            printfn "Sent Gossip by %s" mailbox.Self.Path.Name
                                                            if gossipSentCount < gossipSendLimit then
                                                                gossipSentCount <- gossipSentCount + 1
                                                                //TODO
                                                                let mutable next = random.Next()
                                                                neigbhours.Item(next % neigbhours.Length) <! ReceiveGossip
                                                            
                                                            else if not isNodeInactive then
                                                                isNodeInactive <- true
                                                                initiatorRef <! TerminateNode
                                                                mailbox.Self <! InformNeighbours

        | InformNeighbours                          ->  for i in 0 .. neigbhours.Length-1 do
                                                            neigbhours.Item(i) <! LimitReached

        | LimitReached                              ->  inactiveNeighbours <- inactiveNeighbours + 1
                                                        if inactiveNeighbours = neigbhours.Length then
                                                            isNodeInactive <- true
                                                            initiatorRef <! TerminateNode
                                                        else
                                                            mailbox.Self <! SendGossip
        
        return! loop()
    }
    loop()


let Actor (mailbox:Actor<_>) = 
    let rec loop() = actor {
        let! message = mailbox.Receive()
        match message with
        | Create -> printfn("In creation")
                    let mutable actors = []
                    if algorithm = "gossip"  then
                        actors <- [for i in 0 .. nodesCount do yield(spawn system ("Actor-" + (string i)) Gossip)]
                    elif algorithm = "pushsum" then
                    //TODO
                        actors <- [for i in 0 .. nodesCount do yield(spawn system ("Actasdor_" + (string i)) Gossip)]
                    else
                        printfn "Exception: Unknown algorithm!!!"
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
        | Init                          ->  actorRef <! Create

        | ActorsCreated(actorRefList)   ->  printfn("Actor created")
                                            actorList <- actorRefList
                                            """
                                            for i in 0 .. 3 do
                                                let mutable str = ""
                                                str <- actorList.Item(i).Path.Name
                                                printfn "actor name is %s" str
                                            """
                                            mailbox.Self <! InitializeTopology(actorList)
        
        | InitializeTopology(actorList) ->  printfn("Initializing topology")
                                            topologyRef <! BuildTopology(topology, actorList)
        
        | TopologyBuilt                 ->  printfn("Topology Constructed")
                                            if algorithm = "gossip" then
                                                actorList.Item(random.Next() % nodesCount) <! ReceiveGossip
                                                timer.Start()
                                            """
                                            else
                                                let ind = random.Next()%nodecount |> float
                                                Nodelist.Item(rnd.Next()%nodecount)<!Receive(ind,1.0)
                                                timer.Start()      
                                            """

        | TerminateNode                 ->  let mutable sender = mailbox.Sender().Path.Name
                                            printfn "Terminated %s" sender
                                            nodeGossipedCount <- nodeGossipedCount + 1
                                            if nodeGossipedCount = nodesCount then 
                                                printfn "asdasdasd"
                                                mailbox.Context.System.Terminate() |> ignore
                                                printfn "%s,%s,%i,%i" algorithm topology nodesCount timer.ElapsedMilliseconds
        
        | _                             -> ()

        return! loop()
    }
    loop()

// Start of the algorithm - spawn Boss, the delgator
let intiator = spawn system "Initialize" Initiator
intiator <! Init
system.WhenTerminated.Wait()