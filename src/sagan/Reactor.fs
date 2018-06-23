namespace Sagan

open FSharp.Control

type internal Reactor<'a> = private {
  send : 'a -> unit
  events : AsyncSeq<'a>
}


[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module internal Reactor =

  let mk<'a> : Reactor<'a> =
    let mb = Mb.create ()
    let s = AsyncSeq.replicateInfiniteAsync (Mb.take mb)
    { send = mb.Post ; events = s }

  let send (r:Reactor<'a>) (a:'a) = r.send a

  let recv (r:Reactor<'a>) : AsyncSeq<'a> = r.events

  let map (f:'a -> 'b) (g:'b -> 'a) (r:Reactor<'a>) : Reactor<'b> =
    { send = (fun b -> r.send (g b)) ; events = r.events |> AsyncSeq.map f }


type internal ChangefeedPositionTracker<'a> = 
  private {
    channel :  Reactor<'a>
  }


[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module internal ChangefeedPositionTracker =

  let start<'a> () = async {
    let reactor = Reactor.mk<'a>
    let proc =
      reactor
      |> Reactor.recv
      |> AsyncSeq.iterAsync (fun p -> async { return () })
    return { channel = reactor }, proc
  }
