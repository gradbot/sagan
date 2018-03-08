module Sagan.ChangefeedProcessor

open System
open System.Linq

open Microsoft.Azure.Documents
open Microsoft.Azure.Documents.Client

open FSharp.Control


type CosmosEndpoint = {
  uri : Uri
  authKey : string
  databaseName : string
  collectionName : string
}

type RangePosition = {
  RangeMin : int64
  RangeMax : int64
  LastLSN : int64
}

type ChangefeedPosition = RangePosition[]

/// ChangefeedProcessor configuration
type Config = {
  /// MaxItemCount fetched from a partition per batch
  BatchSize : int

  /// Interval between invocations of `progressHandler`
  ProgressInterval : TimeSpan

  /// Position in the Changefeed to begin processing from
  StartingPosition : StartingPosition

  /// Position in the Changefeed to stop processing at
  StoppingPosition : ChangefeedPosition option
}

/// ChangefeedProcessor starting position in the DocDB changefeed
and StartingPosition =
  | Beginning
  | ChangefeedPosition of ChangefeedPosition


type private State = {
  /// The client used to communicate with DocumentDB
  client : DocumentClient

  /// URI of the collection being processed
  collectionUri : Uri
}



[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module RangePosition =
  /// Returns true if the range, r, covers the semi-open interval [min, max)
  let inline rangeCoversMinMax (r:RangePosition) min max =
    (r.RangeMin <= min) && (r.RangeMax >= max)

  /// Returns true if the range of the second argument, y, is fully contained within the first one.
  let inline fstCoversSnd (x:RangePosition) (y:RangePosition) =
    rangeCoversMinMax x y.RangeMin y.RangeMax

  /// Converts a cosmos range string from hex to int64
  let rangeToInt64 str =
    match Int64.TryParse(str, Globalization.NumberStyles.HexNumber, Globalization.CultureInfo.InvariantCulture) with
    | true, 255L when str.ToLower() = "ff" -> Int64.MaxValue    // max for last partition
    | true, i -> i
    | false, _ when str = "" -> 0L
    | false, _ -> 0L  // NOTE: I am not sure if this should be the default or if we should return an option instead

  /// Converts a cosmos logical sequence number (LSN) from string to int64
  let lsnToInt64 str =
    match Int64.TryParse str with
    | true, i -> i
    | false, _ -> 0L



[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module ChangefeedPosition =
  let fstSucceedsSnd (cfp1:ChangefeedPosition) (cfp2:ChangefeedPosition) =
    let successionViolationExists =
      seq { // test range covers
        for x in cfp1 do
          yield cfp2 |> Seq.exists (fun y -> (RangePosition.fstCoversSnd x y) && (x.LastLSN > y.LastLSN))
      }
      |> Seq.exists id // find any succession violations
    not successionViolationExists

  let tryPickLatest (cfp1:ChangefeedPosition) (cfp2:ChangefeedPosition) =
    if fstSucceedsSnd cfp1 cfp2 then Some cfp1
    elif fstSucceedsSnd cfp2 cfp1 then Some cfp2
    else None

  let tryGetPartitionByRange min max (cfp:ChangefeedPosition) =
    cfp |> Array.tryFind (fun rp -> RangePosition.rangeCoversMinMax rp min max)






/// Returns the current set of partitions in docdb changefeed
let private getPartitions (st:State) = async {
  // TODO: set FeedOptions properly. Needed for resuming from a changefeed position
  let! response = st.client.ReadPartitionKeyRangeFeedAsync st.collectionUri |> Async.AwaitTaskCorrect
  return response.ToArray()
}


/// Reads a partition of the changefeed and returns an AsyncSeq<Document[] * RangePosition>
///   - Document[] is a batch of documents read from changefeed
///   - RangePosition is a record containing the partition's key range and the last logical sequence number in the batch.
/// This function attempts to read all documents in the semi-closed LSN range [start, end).
let rec private readPartition (config:Config) (st:State) (pkr:PartitionKeyRange) =
  let rangeMin = pkr.GetPropertyValue "minInclusive" |> RangePosition.rangeToInt64
  let rangeMax = pkr.GetPropertyValue "maxExclusive" |> RangePosition.rangeToInt64

  let continuationToken : string =
    match config.StartingPosition with
    | Beginning -> null
    | ChangefeedPosition cfp ->
      cfp
      |> ChangefeedPosition.tryGetPartitionByRange rangeMin rangeMax
      |> Option.map (fun rp -> string rp.LastLSN)
      |> Option.getValueOr null   // docdb starts at the the beginning of a partition if null
  let stoppingPosition =
    config.StoppingPosition
    |> Option.bind (ChangefeedPosition.tryGetPartitionByRange rangeMin rangeMax)
    |> Option.map (fun rp -> rp.LastLSN)
  let cfo =
    ChangeFeedOptions(
      PartitionKeyRangeId = pkr.Id,
      MaxItemCount = Nullable config.BatchSize,
      StartFromBeginning = true,   // TODO: double check that this is ignored by docdb if RequestContinuation is set
      RequestContinuation = continuationToken)
  let query = st.client.CreateDocumentChangeFeedQuery(st.collectionUri, cfo)

  let rec readPartition (query:Linq.IDocumentQuery<Document>) (pkr:PartitionKeyRange) = asyncSeq {
    let! response = query.ExecuteNextAsync<Document>() |> Async.AwaitTask

    let rp : RangePosition = {
      RangeMin = rangeMin
      RangeMax = rangeMax
      LastLSN = response.ResponseContinuation.Replace("\"", "") |> RangePosition.lsnToInt64
    }
    if response <> null then
      if response.Count > 0 then
        yield (response.ToArray(), rp)
      if query.HasMoreResults then
        match stoppingPosition with
        | Some stopLSN when rp.LastLSN >= stopLSN -> ()   // TODO: this can stop after the stop position, but this is ok for now. Fix later.
        | _ -> yield! readPartition query pkr
      else
        yield! readPartition query pkr
  }

  let startLSN =
    if continuationToken = null then Int64.MinValue
    else int64 continuationToken

  match stoppingPosition with
  | Some stopLSN when startLSN >= stopLSN -> AsyncSeq.empty
  | _ -> readPartition query pkr


/// Returns an async computation that runs a concurrent (per-docdb-partition) changefeed processor.
/// - `handle`: is an asynchronous function that takes a batch of documents and returns a result
/// - `progressHandler`: is an asynchronous function ('a list * ChangefeedPosition) -> Async<unit>
///    that is called periodically with a list of outputs that were produced by the handle function since the
///    last invocation and the current position of the changefeedprocessor.
let go (cosmos:CosmosEndpoint) (config:Config) handle progressHandler = async {
  use client = new DocumentClient(cosmos.uri, cosmos.authKey)
  let state = {
    client = client
    collectionUri = UriFactory.CreateDocumentCollectionUri(cosmos.databaseName, cosmos.collectionName)
  }

  // updates the given partition position in the given changefeed position
  let updateChangefeedPosition (cfp:ChangefeedPosition) (rp:RangePosition) =
    let index = cfp |> Array.tryFindIndex (function x -> x.RangeMin=rp.RangeMin && x.RangeMax=rp.RangeMax)
    match index with
    | Some i ->
      let newCfp = Array.copy cfp
      newCfp.[i] <- rp
      newCfp
    | None ->
      let newCfp = Array.zeroCreate (cfp.Length+1)
      for i in 0..cfp.Length-1 do
        newCfp.[i] <- cfp.[i]
      newCfp.[cfp.Length] <- rp
      newCfp

  // updates changefeed position and add the new element to the list of outputs
  let accumPartitionsPositions (outputs:'a list, cfp:ChangefeedPosition) (handlerOutput: 'a, pp:RangePosition) =
    (handlerOutput::outputs) , (updateChangefeedPosition cfp pp)

  // converts a buffered list of handler output to a flat list of outputs and an updated changefeed position
  let flatten (x: ('a list * ChangefeedPosition) []) : 'a list * ChangefeedPosition =
    let flattenOutputs os =
      Seq.collect id os
      |> Seq.toList
      |> List.rev
    let flattenPartitionPositions (cfps:ChangefeedPosition[]) = cfps |> Array.tryLast |> Option.getValueOr [||]
    x
    |> Array.unzip
    |> mapPair flattenOutputs flattenPartitionPositions


  // used to accumulate the output of all the user handle functions
  let progressReactor = Reactor.mk

  // wrap the document handler so that it takes and passes out partition position
  let handle ((_doc, pp) as input) = async {
    let! ret = handle input
    (ret, pp) |> Reactor.send progressReactor
  }
  let! progressTracker =
    progressReactor
    |> Reactor.recv
    |> AsyncSeq.scan accumPartitionsPositions ([],[||])
    |> AsyncSeq.bufferByTime (int config.ProgressInterval.TotalMilliseconds)
    |> AsyncSeq.iterAsync (flatten >> progressHandler)
    |> Async.StartChild


  let! partitions = getPartitions state
  let workers =
    partitions
    |> Array.map ((fun pkr -> readPartition config state pkr) >> AsyncSeq.iterAsync handle)
    |> Async.Parallel
    |> Async.Ignore

  return! Async.choose progressTracker workers
}


/// Periodically queries DocDB for the latest positions of all partitions in its changefeed.
/// The `handler` function will be called periodically, once per `interval`, with an updated ChangefeedPosition.
let trackTailPosition (cosmos:CosmosEndpoint) (interval:TimeSpan) (handler:DateTime*ChangefeedPosition -> Async<unit>) = async {
  use client = new DocumentClient(cosmos.uri, cosmos.authKey)
  let state = {
    client = client
    collectionUri = UriFactory.CreateDocumentCollectionUri(cosmos.databaseName, cosmos.collectionName)
  }

  let getRecentPosition (pkr:PartitionKeyRange) = async {
    let cfo = ChangeFeedOptions(PartitionKeyRangeId = pkr.Id, StartTime = Nullable DateTime.Now)
    let query = client.CreateDocumentChangeFeedQuery(state.collectionUri, cfo)
    let! response = query.ExecuteNextAsync<Document>() |> Async.AwaitTask
    let rp : RangePosition = {
      RangeMin = pkr.GetPropertyValue "minInclusive" |> RangePosition.rangeToInt64
      RangeMax = pkr.GetPropertyValue "maxExclusive" |> RangePosition.rangeToInt64
      LastLSN = response.ResponseContinuation.Replace("\"", "") |> RangePosition.lsnToInt64
    }
    return rp
  }

  let! partitions = getPartitions state   // NOTE: partitions will only be fetched once. Consider moving this inside the query function in case of a docdb partition split.
  let queryPartitions dateTime = async {
    let! changefeedPosition =
      partitions
      |> Array.map getRecentPosition
      |> Async.Parallel
    return (dateTime, changefeedPosition)
  }

  return!
    AsyncSeq.intervalMs (int interval.TotalMilliseconds)
    |> AsyncSeq.mapAsyncParallel queryPartitions
    |> AsyncSeq.iterAsync handler
}