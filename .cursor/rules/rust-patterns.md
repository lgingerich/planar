# Priorities (in order): idiomatic Rust > simplicity/clarity > performance.
# Default to the standard library; add dependencies only when clearly justified.

You are writing Rust for a codebase that values:
- Idiomatic Rust (ownership/borrowing, Result/Option, iterators, pattern matching, modules)
- Simplicity (readability, small focused functions, minimal cleverness)
- Performance (avoid needless allocs/clones, prefer streaming/iterators, correct complexity)

GENERAL
- Prefer clear, direct code over “smart” abstractions.
- Keep functions small and single-purpose. Extract helpers when logic branches grow.
- Use early returns and `?` for error propagation.
- Prefer `match` when it improves clarity over nested `if let`.
- Avoid unnecessary macros; only use when they reduce repetition meaningfully.

FORMATTING & LINTS
- Code must be `rustfmt`-clean.
- Code must pass `clippy` with no new warnings; follow clippy suggestions unless they harm readability or performance.
- Use descriptive names; avoid single-letter names except for short closures or indices.

TYPES, OWNERSHIP, BORROWING
- Prefer borrowing over owning:
  - Take `&str` instead of `String` when the function doesn’t need ownership.
  - Take `&[T]` instead of `Vec<T>` when the function doesn’t need ownership.
- Return owned values when the caller should own them; otherwise return references with correct lifetimes.
- Avoid `.clone()` and `.to_string()` unless necessary; justify clones in comments if non-obvious.
- Prefer `Cow<'a, str>` when you want “borrow when possible, allocate when needed”.
- Minimize lifetime annotations; only add explicit lifetimes when required by the compiler.

ERROR HANDLING
- Prefer `Result<T, E>` with a meaningful error type.
- Use `thiserror` only if the project already uses it or error enums are getting unwieldy.
- Avoid `unwrap()` / `expect()` in library code and production paths.
  - `unwrap()` is acceptable in tests and unreachable-by-construction internal code, with a short comment if needed.
- Provide context for errors where useful (e.g., include identifiers, paths, operation).

API DESIGN
- Prefer simple signatures:
  - Use generics when they reduce duplication without obscuring intent.
  - Prefer `impl Trait` in arguments/returns for readability when appropriate.
- Prefer `AsRef<Path>` for path-like inputs and `IntoIterator` for iterable inputs when it helps ergonomics.
- Prefer returning iterators only when it’s genuinely useful; otherwise return collections or slices.
- Prefer passing a small backend enum (for example, `DbKind`) into a single selector function for DB-specific limits/config, rather than creating many backend-specific tiny helper functions.
- Prefer concrete option types over JSON for core APIs; keep JSON only at persistence boundaries.
- For known constrained domains (formats, states, operation kinds), prefer enums over strings.
- Keep read/write/stream APIs aligned across formats so sync/async behavior matches at the core layer.
- Align helper naming across format implementations (e.g., `apply_read_options`).
- Use consistent path handling (validate UTF-8 once, share helper), avoid silent lossy conversions.
- Keep small helpers inline unless it prevents substantial duplication.
- Reject permissive JSON shapes in core metadata/spec inputs; require object shape where keys are expected.

ITERATORS & COLLECTIONS
- Prefer iterators over indexing when possible.
- Avoid repeated passes over data unless it improves clarity and is not performance-critical.
- Use `HashMap`/`HashSet` when needed; pre-allocate with `with_capacity` when size is known or can be estimated.
- Prefer `Vec::with_capacity` when building vectors incrementally with a known approximate size.
- Avoid intermediate allocations:
  - Prefer `push_str`, `write!`/`fmt::Write`, and `String::with_capacity` for string building.
  - Prefer `.extend(...)` over repeated `.push(...)` when convenient.

PERFORMANCE GUIDELINES
- Don’t micro-optimize by default. Optimize only obvious hotspots:
  - Remove unnecessary allocations, clones, and temporary collections.
  - Avoid `format!` in tight loops; prefer `write!` into a preallocated `String`.
  - Choose appropriate algorithmic complexity (avoid O(n^2) when n can grow).
- Use `&str` slicing carefully; avoid repeated `.chars().nth()` (O(n)).
- Prefer `bytes()` for ASCII/byte-level work; use `chars()` only for Unicode scalar value needs.

CONCURRENCY & ASYNC (ONLY IF REQUESTED/RELEVANT)
- Prefer simple synchronous code unless async is required.
- If async is used, avoid blocking calls in async contexts.
- Use channels and tasks judiciously; keep critical sections small.

DOCUMENTATION
- Add rustdoc comments for public items:
  - Brief summary + key invariants + examples when helpful.
- Document invariants and safety assumptions, especially around `unsafe` (which should be avoided unless necessary).

TESTING
- Add unit tests for non-trivial logic and edge cases.
- Prefer table-driven tests for many cases.
- Use `#[cfg(test)]` modules and keep tests readable.
- Avoid relying on timing/flaky behavior.

UNSAFE
- Avoid `unsafe` unless there is a measurable need.
- If `unsafe` is required:
  - Keep the unsafe block minimal.
  - Explain invariants that make it safe.
  - Add tests covering boundary conditions.

CODE REVIEW CHECKLIST (APPLY BEFORE FINAL OUTPUT)
- Is the ownership/borrowing model minimal and correct?
- Any unnecessary clones/allocations?
- Any panics in non-test code?
- Is the API ergonomic (borrows where possible, owns where appropriate)?
- Would `clippy` complain? If so, address it or justify it.
- Is the simplest correct solution implemented first?
