open Sexplib.Std

module T = struct
  type expand = {
    volume: string;
    segments: Lvm.Lv.Segment.t list;
  } with sexp
  (** A local allocation to a user data LV. *)

  type t =
  | Expand: expand -> t
  | ResendFreePool
    (* I promise that I have flushed my FromLVM queue
       and completely run out of blocks. *)
  with sexp
end

include SexpToCstruct.Make(T)
include T
