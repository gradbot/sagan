[<AutoOpen>]
module Sagan.Prelude

open System
open System.Threading
open System.Threading.Tasks

/// Maps over individual items of a pair.
let inline mapPair f g (a,b) = (f a, g b)


module Option =
  /// Given a default value and an option, returns the option value if there else the default value.
  let inline getValueOr defaultValue = function Some v -> v | None -> defaultValue


module Array =
  let tryLast arr =
    let len = Array.length arr
    if len > 0 then Some arr.[len-1]
    else None


module List =
  /// Prepend element to list.
  let inline cons x xs = x::xs



type Mb<'a> = MailboxProcessor<'a>

/// Operations on unbounded FIFO mailboxes.
module Mb =

  /// Creates a new unbounded mailbox.
  let create () = Mb.Start (fun _ -> async.Return())

  /// Puts a message into a mailbox, no waiting.
  let inline put (a:'a) (mb:Mb<'a>) = mb.Post a

  /// Creates an async computation that completes when a message is available in a mailbox.
  let inline take (mb:Mb<'a>) = mb.Receive ()



type Async with
  static member inline bind (f:'a -> Async<'b>) (a:Async<'a>) : Async<'b> = async.Bind(a, f)

  /// Asynchronously await supplied task with the following variations:
  ///     *) Task cancellations are propagated as exceptions
  ///     *) Singleton AggregateExceptions are unwrapped and the offending exception passed to cancellation continuation
  static member inline AwaitTaskCorrect (task:Task<'a>) : Async<'a> =
    Async.FromContinuations <| fun (ok,err,cnc) ->
      task.ContinueWith (fun (t:Task<'a>) ->
        if t.IsFaulted then
            let e = t.Exception
            if e.InnerExceptions.Count = 1 then err e.InnerExceptions.[0]
            else err e
        elif t.IsCanceled then err (TaskCanceledException("Task wrapped with Async has been cancelled."))
        elif t.IsCompleted then ok t.Result
        else err(Exception "invalid Task state!")
      )
      |> ignore

  /// Like Async.StartWithContinuations but starts the computation on a ThreadPool thread.
  static member StartThreadPoolWithContinuations (a:Async<'a>, ok:'a -> unit, err:exn -> unit, cnc:OperationCanceledException -> unit, ?ct:CancellationToken) =
    let a = Async.SwitchToThreadPool () |> Async.bind (fun _ -> a)
    Async.StartWithContinuations (a, ok, err, cnc, defaultArg ct CancellationToken.None)

  /// Creates an async computation which completes when any of the argument computations completes.
  /// The other argument computation is cancelled.
  static member choose (a:Async<'a>) (b:Async<'a>) : Async<'a> =
    Async.FromContinuations <| fun (ok,err,cnc) ->
      let state = ref 0
      let cts = new CancellationTokenSource()
      let inline cancel () =
        cts.Cancel()
        cts.Dispose()
      let inline ok a =
        if (Interlocked.CompareExchange(state, 1, 0) = 0) then
          cancel ()
          ok a
      let inline err (ex:exn) =
        if (Interlocked.CompareExchange(state, 1, 0) = 0) then
          cancel ()
          err ex
      let inline cnc ex =
        if (Interlocked.CompareExchange(state, 1, 0) = 0) then
          cancel ()
          cnc ex
      Async.StartThreadPoolWithContinuations (a, ok, err, cnc, cts.Token)
      Async.StartThreadPoolWithContinuations (b, ok, err, cnc, cts.Token)