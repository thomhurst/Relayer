; Unshipped analyzer release

### New Rules

Rule ID | Category | Severity | Notes
--------|----------|----------|-------
RLYH001 | Relayer | Error | Handler message type must be accessible to generated dispatch
RLY001 | Relayer | Warning | Message type needs a stable wire identity
RLY002 | Relayer | Error | Message name and version must be unique
RLY003 | Relayer | Warning | Handler must be registered
RLY004 | Relayer | Warning | Consumer needs a handler
RLYM001 | Relayer | Error | Message declaration must not have unbound type parameters
RLYM002 | Relayer | Error | Message type must be accessible to generated registration and JSON metadata
RLYM004 | Relayer | Error | Message type must not be static
