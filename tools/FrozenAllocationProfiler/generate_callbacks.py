"""Reuse official MIT-licensed sample no-op callbacks, with their real SDK ABI."""
import pathlib
import re
import sys

source = pathlib.Path(sys.argv[1]).read_text()
callbacks = re.findall(r"HRESULT STDMETHODCALLTYPE (\w+)\([^;{}]*?\) override \{ return S_OK; \}", source)
if len(callbacks) < 70 or "ObjectAllocated" not in callbacks:
    raise ValueError("Unexpected pinned sample callback scaffolding")
methods = re.findall(r"HRESULT STDMETHODCALLTYPE \w+\([^;{}]*?\) override \{ return S_OK; \}", source)
methods = [m for m in methods if not m.startswith("HRESULT STDMETHODCALLTYPE ObjectAllocated(")]
pathlib.Path(sys.argv[2]).write_text(
    "// Generated from dotnet/samples (MIT); see its retained LICENSE.\n"
    "#pragma once\n#include <atomic>\n#include \"profilercommon.h\"\n"
    "class Callbacks : public ICorProfilerCallback10 {\npublic:\n"
    + "\n".join(methods) + "\n};\n")
