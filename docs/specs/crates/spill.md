# `spill` Crate Specification

**Status:** Living crate contract

## Purpose

`spill` provides query-local memory accounting, a versioned ephemeral binary
codec for common rows and values, rewindable tapes backed by anonymous temporary
files, and stable external sorting. It depends only on `common` and `tempfile` so
executor operators and future storage/index builders can reuse it.

## Contract

- `SpillConfig::for_operator` creates an independent soft memory budget. Internal
  structures for one physical operator share that budget; separate operators do
  not. Direct configurations are clamped to a 4KiB framework minimum so run
  metadata can always make progress; the server's SQL `work_mem` minimum is
  stricter.
- Retained allocations, including framework-owned vector capacity, are charged.
  One oversized record and the constant number of merge heads may exceed the
  limit so progress remains possible. Retained row data never grows with total
  input; binary run consolidation retains at most logarithmic small run metadata
  and anonymous file handles.
- Tapes migrate atomically from memory to anonymous files. Readers have
  independent logical positions and may be interleaved. Files are query-local,
  are never fsynced or WAL-logged, and disappear when their handles are dropped.
- Spill files use `SGSP`, version `1`, length-framed records, and explicit binary
  encodings that preserve every `Value`, including non-finite floats and signed
  zero. The format is not durable across server versions.
- External sort is stable, cancellation-aware, and consolidates runs through
  binary merges. I/O or codec failures return structured `IoError` values; a
  sorter that loses run state during a failed merge is poisoned against reuse.
