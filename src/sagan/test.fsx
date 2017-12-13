#I "bin/Debug"

#r "Newtonsoft.Json"
#r "Microsoft.Azure.Documents.Client"
#r "FSharp.Control.AsyncSeq"

#load "Prelude.fs"
#load "Reactor.fs"
#load "ChangefeedProcessor.fs"


open System
open Microsoft.Azure.Documents
open Sagan.ChangefeedProcessor


let handler (docs:Document[]) = async {
  return sprintf "Processed %d documents" (Array.length docs)
}

let prog (msgs, changefeedPosition) = async {
  printfn "---Progress Tracker---Handler called %d times---" (Seq.length msgs)
  printfn "~~~~Changefeed Position~~~~\n%A\n~~~~" changefeedPosition
}

let endpoint : CosmosEndpoint = {
  uri = Uri "https://[YOUR ACCOUNT].documents.azure.com:443/"
  authKey = "[YOUR AUTH KEY]"
  databaseName = "[DB NAME]"
  collectionName = "[COLLECTION NAME]"
}

let config : Config = {
  BatchSize = 1000
  ProgressInterval = TimeSpan.FromSeconds 10.
  StartingPosition = Beginning
  StoppingPosition = None
}

go endpoint config handler prog
|> Async.RunSynchronously
