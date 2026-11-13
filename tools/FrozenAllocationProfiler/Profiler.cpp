// Investigation only. Allocation callbacks change allocation fast paths and scheduling.
// Exact counts describe this instrumented execution, never the historical gate.
#include "Callbacks.h"
#include "ClassFactory.h"
#include <cstdlib>
#include <cstring>
#include <vector>
#include <fstream>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <tuple>

namespace {
constexpr GUID ProfilerId = {0x32b7ef07,0x25e8,0x4ac7,{0xb0,0x11,0x9a,0x43,0x21,0xd7,0x02,0xc8}};
struct Count { unsigned long long objects = 0, bytes = 0; std::string name; };
std::mutex gate;
bool initialized = false, active = false, begun = false;
unsigned long long failures = 0;
std::map<ClassID, Count> counts;
struct Window { long long measured; std::map<ClassID, Count> counts; };
std::vector<Window> windows;
bool windowsCompleted = false;
std::map<ClassID, Count> previousCounts;
ICorProfilerInfo12* info = nullptr;
// Optional synchronous CLR snapshots, taken on the allocating thread. Native
// memory only; no managed instrumentation or cross-thread suspension is used.
std::string stackPath;
using StackKey = std::tuple<size_t, ThreadID, ClassID, HRESULT, std::vector<FunctionID>>;
std::map<StackKey, Count> stackCounts;
std::map<FunctionID, std::string> frameNames;
unsigned long long stackFailures = 0;
// Native BDN events are delivered synchronously on the emitting thread by the
// profiler EventPipe API (pinned dotnet/samples EventPipeProfiler.cpp).
bool nativeTail = false, benchmarkStarted = false, benchmarkStopped = false, iterationOpen = false;
ThreadID engineThread = 0;
long long pendingOperations = 0, nativeOperations = 0;
unsigned iterations = 0;
EVENTPIPE_SESSION session = 0;
struct EngineEvent { DWORD id; ThreadID thread; long long operations; };
std::vector<EngineEvent> engineEvents;
constexpr const char* Benchmark = "Relayer.Benchmarks.EndToEndBenchmarks.ConcurrentRoundTrip";

std::string selectedType, selectedMethod;
unsigned long long methodEntries = 0, methodLeaves = 0;
FunctionID selectedFunction = 0;
int writeReport();
std::string narrow(const WCHAR* value);
std::string definitionName(IMetaDataImport*, mdTypeDef, unsigned depth = 0);

HRESULT STDMETHODCALLTYPE snapshotFrame(FunctionID function, UINT_PTR, COR_PRF_FRAME_INFO,
    ULONG32, BYTE[], void* state)
{
    static_cast<std::vector<FunctionID>*>(state)->push_back(function);
    return S_OK;
}

std::string functionName(FunctionID function)
{
    if (!function) return "<native-transition>";
    ClassID type; ModuleID module; mdToken token;
    if (FAILED(info->GetFunctionInfo(function, &type, &module, &token))) return "<unresolved-function>";
    IMetaDataImport* metadata = nullptr;
    if (FAILED(info->GetModuleMetaData(module, ofRead, IID_IMetaDataImport,
        reinterpret_cast<IUnknown**>(&metadata)))) return "<unresolved-function>";
    mdTypeDef owner; WCHAR name[2048]; ULONG length; DWORD flags, implFlags;
    PCCOR_SIGNATURE signature; ULONG signatureLength, rva;
    auto hr = metadata->GetMethodProps(token, &owner, name, 2048, &length, &flags,
        &signature, &signatureLength, &rva, &implFlags);
    auto result = SUCCEEDED(hr) && length <= 2048
        ? definitionName(metadata, owner) + "." + narrow(name) : "<unresolved-function>";
    metadata->Release();
    return result;
}

// Slowpath hooks use the runtime's ABI-preserving stubs, as in dotnet/runtime's
// native SlowPathELTProfiler. Do not substitute the fastpath hooks with C++ bodies.
void STDMETHODCALLTYPE methodEnter(FunctionIDOrClientID, COR_PRF_ELT_INFO)
{
    std::lock_guard<std::mutex> lock(gate);
    ++methodEntries;
    if (begun) { ++failures; return; }
    begun = true; active = true;
}
void STDMETHODCALLTYPE methodLeave(FunctionIDOrClientID, COR_PRF_ELT_INFO)
{
    std::lock_guard<std::mutex> lock(gate);
    ++methodLeaves;
    if (!active) ++failures;
    active = false;
}
void STDMETHODCALLTYPE methodTailcall(FunctionIDOrClientID, COR_PRF_ELT_INFO)
{
    std::lock_guard<std::mutex> lock(gate);
    ++failures; active = false;
}

UINT_PTR STDMETHODCALLTYPE mapFunction(FunctionID function, void*, BOOL* hook)
{
    *hook = FALSE;
    ClassID type; ModuleID module; mdToken token;
    if (FAILED(info->GetFunctionInfo(function, &type, &module, &token))) return function;
    IMetaDataImport* metadata = nullptr;
    if (FAILED(info->GetModuleMetaData(module, ofRead, IID_IMetaDataImport,
                                     reinterpret_cast<IUnknown**>(&metadata)))) return function;
    mdTypeDef owner; WCHAR name[2048]; ULONG length; DWORD flags, implFlags;
    PCCOR_SIGNATURE signature; ULONG signatureLength; ULONG rva;
    auto hr = metadata->GetMethodProps(token, &owner, name, 2048, &length, &flags,
                                      &signature, &signatureLength, &rva, &implFlags);
    // Only the explicitly selected non-inlined method can begin a region. Name
    // collisions/overloads cause multiple entries or target functions and fail closed.
    if (SUCCEEDED(hr) && length <= 2048 && narrow(name) == selectedMethod) {
        std::lock_guard<std::mutex> lock(gate);
        if (definitionName(metadata, owner) == selectedType) {
            if (!(implFlags & miNoInlining)) ++failures;
            if (selectedFunction && selectedFunction != function) ++failures;
            selectedFunction = function;
            *hook = TRUE;
        }
    }
    metadata->Release();
    return function;
}

std::string narrow(const WCHAR* value)
{
    // Metadata can be Unicode. Preserve code units as JSON escapes rather than lossy conversion.
    std::ostringstream result;
    for (; *value; ++value) {
        unsigned c = static_cast<unsigned short>(*value);
        if (c >= 32 && c < 127 && c != '\\' && c != '"') result << static_cast<char>(c);
        else { char escaped[7]; std::snprintf(escaped, sizeof(escaped), "\\u%04x", c); result << escaped; }
    }
    return result.str();
}

std::string definitionName(IMetaDataImport* metadata, mdTypeDef token, unsigned depth)
{
    if (depth > 12) { ++failures; return "<type-depth-exceeded>"; }
    WCHAR name[2048]; ULONG length; DWORD flags; mdToken base;
    auto hr = metadata->GetTypeDefProps(token, name, 2048, &length, &flags, &base);
    if (FAILED(hr) || length > 2048) { ++failures; return "<unresolved-name>"; }
    mdTypeDef enclosing;
    if (IsTdNested(flags)) {
        if (FAILED(metadata->GetNestedClassProps(token, &enclosing))) { ++failures; return "<unresolved-parent>"; }
        return definitionName(metadata, enclosing, depth + 1) + "+" + narrow(name);
    }
    return narrow(name);
}

std::string typeName(ClassID id, unsigned depth = 0)
{
    if (depth > 12) { ++failures; return "<type-depth-exceeded>"; }
    CorElementType element; ClassID elementId; ULONG rank;
    if (info->IsArrayClass(id, &element, &elementId, &rank) == S_OK)
        return (elementId ? typeName(elementId, depth + 1) : "<primitive:" + std::to_string(element) + ">") + "[rank=" + std::to_string(rank) + "]";
    ModuleID module; mdTypeDef token; ClassID parent; ULONG actual;
    ClassID arguments[32];
    if (FAILED(info->GetClassIDInfo2(id, &module, &token, &parent, 32, &actual, arguments)) || actual > 32) {
        ++failures; return "<unresolved-class>";
    }
    IMetaDataImport* metadata = nullptr;
    auto moduleResult = info->GetModuleMetaData(module, ofRead, IID_IMetaDataImport, reinterpret_cast<IUnknown**>(&metadata));
    if (FAILED(moduleResult)) {
        std::fprintf(stderr, "GetModuleMetaData failed: 0x%x, module %llu\n", static_cast<unsigned>(moduleResult), static_cast<unsigned long long>(module));
        ++failures; return "<unresolved-module>";
    }
    std::string result = definitionName(metadata, token);
    metadata->Release();
    if (actual) {
        result += "[";
        for (ULONG i = 0; i < actual; ++i) { if (i) result += ","; result += typeName(arguments[i], depth + 1); }
        result += "]";
    }
    return result;
}

class Profiler final : public Callbacks {
    std::atomic<ULONG> references{0};
public:
    HRESULT STDMETHODCALLTYPE Initialize(IUnknown* unknown) override {
        auto hr = unknown->QueryInterface(__uuidof(ICorProfilerInfo12), reinterpret_cast<void**>(&info));
        if (FAILED(hr)) return hr;
        nativeTail = std::getenv("RELAYER_ALLOCATION_BDN_TAIL") != nullptr;
        DWORD mask = COR_PRF_MONITOR_OBJECT_ALLOCATED | COR_PRF_ENABLE_OBJECT_ALLOCATED;
        if (const char* path = std::getenv("RELAYER_ALLOCATION_STACKS")) {
            if (!*path) return E_INVALIDARG;
            stackPath = path;
            mask |= COR_PRF_ENABLE_STACK_SNAPSHOT;
        }
        const char* type = std::getenv("RELAYER_ALLOCATION_TYPE");
        const char* method = std::getenv("RELAYER_ALLOCATION_METHOD");
        if ((type != nullptr) != (method != nullptr)) return E_INVALIDARG;
        if (nativeTail && type) return E_INVALIDARG;
        if (type && method) {
            if (!*type || !*method) return E_INVALIDARG;
            selectedType = type; selectedMethod = method;
            // FRAME_INFO selects slowpath runtime stubs (ordinary C++ callback ABI).
            mask |= COR_PRF_MONITOR_ENTERLEAVE | COR_PRF_ENABLE_FRAME_INFO | COR_PRF_MONITOR_EXCEPTIONS;
        }
        hr = info->SetEventMask2(mask, nativeTail ? COR_PRF_HIGH_MONITOR_EVENT_PIPE : 0);
        if (SUCCEEDED(hr) && !selectedMethod.empty()) {
            hr = info->SetFunctionIDMapper2(mapFunction, nullptr);
            if (SUCCEEDED(hr)) hr = info->SetEnterLeaveFunctionHooks3WithInfo(methodEnter, methodLeave, methodTailcall);
        }
        initialized = SUCCEEDED(hr);
        if (FAILED(hr)) std::fprintf(stderr, "Profiler initialization failed: 0x%x\n", static_cast<unsigned>(hr));
        if (FAILED(hr)) return hr;
        if (nativeTail) {
            COR_PRF_EVENTPIPE_PROVIDER_CONFIG provider = {WCHAR("BenchmarkDotNet.EngineEventSource"), 0xffffffffffffffffULL, 4, nullptr};
            hr = info->EventPipeStartSession(1, &provider, false, &session);
            if (FAILED(hr)) initialized = false;
        }
        return hr;
    }
    HRESULT STDMETHODCALLTYPE Shutdown() override {
        if (session) info->EventPipeStopSession(session);
        std::lock_guard<std::mutex> lock(gate);
        active = false;
        if (nativeTail) {
            if (!benchmarkStopped || iterationOpen || !iterations) ++failures;
            if (!writeReport()) std::fprintf(stderr, "Native tail report invalid or could not be written.\n");
        }
        if (!selectedMethod.empty()) {
            if (!selectedFunction || methodEntries != 1 || methodLeaves != 1) ++failures;
            if (!writeReport()) std::fprintf(stderr, "Exact method allocation report failed\n");
        }
        initialized = false;
        if (info) { info->Release(); info = nullptr; }
        return S_OK;
    }
    HRESULT STDMETHODCALLTYPE ObjectAllocated(ObjectID object, ClassID type) override {
        std::lock_guard<std::mutex> lock(gate);
        if (!active) return S_OK;
        SIZE_T size;
        if (FAILED(info->GetObjectSize2(object, &size))) { ++failures; return S_OK; }
        auto& count = counts[type];
        // GetModuleMetaData requires a profiler callback context. Resolve once here,
        // never asynchronously from the P/Invoke boundary that writes the report.
        if (count.name.empty()) count.name = typeName(type);
        ++count.objects; count.bytes += size;
        if (!stackPath.empty()) {
            ThreadID thread = 0;
            std::vector<FunctionID> frames;
            auto hr = info->GetCurrentThreadID(&thread);
            if (SUCCEEDED(hr)) hr = info->DoStackSnapshot(0, snapshotFrame, COR_PRF_SNAPSHOT_DEFAULT,
                &frames, nullptr, 0);
            if (FAILED(hr) || frames.empty() || !thread) ++stackFailures;
            for (const auto function : frames)
                if (frameNames.find(function) == frameNames.end()) frameNames.emplace(function, functionName(function));
            auto& stack = stackCounts[StackKey{windows.size(), thread, type, hr, std::move(frames)}];
            ++stack.objects; stack.bytes += size;
        }
        return S_OK;
    }
    HRESULT STDMETHODCALLTYPE ExceptionUnwindFunctionEnter(FunctionID function) override {
        std::lock_guard<std::mutex> lock(gate);
        if (function == selectedFunction && active) { ++failures; active = false; }
        return S_OK;
    }
    HRESULT STDMETHODCALLTYPE EventPipeEventDelivered(EVENTPIPE_PROVIDER provider, DWORD id, DWORD version, ULONG, LPCBYTE,
        ULONG length, LPCBYTE data, LPCGUID, LPCGUID, ThreadID thread, ULONG, UINT_PTR[]) override {
        WCHAR name[256]; ULONG actual;
        if (!nativeTail) return S_OK;
        auto hr = info->EventPipeGetProviderInfo(provider, 256, &actual, name);
        std::lock_guard<std::mutex> lock(gate);
        if (FAILED(hr) || actual > 256) { ++failures; active = false; return S_OK; }
        if (narrow(name) != "BenchmarkDotNet.EngineEventSource") return S_OK;
        // eventThread may be null for an event emitted by the current thread.
        // Verify synchronous delivery and use the CLR's current managed ThreadID.
        ThreadID current = 0;
        auto threadResult = info->GetCurrentThreadID(&current);
        long long operations = 0;
        bool valid = version == 0 && SUCCEEDED(threadResult) && current != 0 && (!thread || thread == current) && data;
        thread = current;
        if (id == 1 || id == 2) {
            const auto expected = std::strlen(Benchmark);
            valid = valid && length == (expected + 1) * 2;
            for (size_t i = 0; valid && i <= expected; ++i)
                valid = data[i * 2] == static_cast<unsigned char>(Benchmark[i]) && data[i * 2 + 1] == 0;
        } else {
            valid = valid && id >= 3 && id <= 16 && length == sizeof(operations);
            if (valid) std::memcpy(&operations, data, sizeof(operations));
            valid = valid && operations > 0 && operations % 8 == 0;
        }
        engineEvents.push_back({id, thread, operations});
        if (!valid || benchmarkStopped || (engineThread && engineThread != thread)) {
            ++failures; active = false; return S_OK;
        }
        engineThread = thread;
        if (id == 1) {
            if (benchmarkStarted) ++failures;
            benchmarkStarted = true;
        } else if (!benchmarkStarted) {
            if (id < 3 || id > 6) ++failures; // EngineFactory jitting precedes BenchmarkStart.
        } else if (id == 15) {
            if (iterationOpen) ++failures;
            iterationOpen = true; pendingOperations = operations; active = false;
        } else if (id == 16) {
            if (!iterationOpen || pendingOperations != operations) ++failures;
            iterationOpen = false; nativeOperations = operations; ++iterations;
            // The final stop is not known until BenchmarkStop. Discard each earlier
            // tail, without ever discarding a coverage/metadata failure.
            counts.clear(); active = failures == 0;
        } else if (id == 2) {
            if (iterationOpen || !iterations) ++failures;
            benchmarkStopped = true; active = false;
        } else if (iterationOpen || iterations) {
            ++failures; active = false;
        }
        return S_OK;
    }
    HRESULT STDMETHODCALLTYPE EventPipeProviderCreated(EVENTPIPE_PROVIDER) override { return S_OK; }
    HRESULT STDMETHODCALLTYPE QueryInterface(REFIID id, void** result) override {
        if (!result) return E_POINTER;
        if (id == IID_IUnknown || id == __uuidof(ICorProfilerCallback) || id == __uuidof(ICorProfilerCallback2) ||
            id == __uuidof(ICorProfilerCallback3) || id == __uuidof(ICorProfilerCallback4) ||
            id == __uuidof(ICorProfilerCallback5) || id == __uuidof(ICorProfilerCallback6) ||
            id == __uuidof(ICorProfilerCallback7) || id == __uuidof(ICorProfilerCallback8) ||
            id == __uuidof(ICorProfilerCallback9) || id == __uuidof(ICorProfilerCallback10)) {
            *result = this; AddRef(); return S_OK;
        }
        *result = nullptr; return E_NOINTERFACE;
    }
    ULONG STDMETHODCALLTYPE AddRef() override { return ++references; }
    ULONG STDMETHODCALLTYPE Release() override { auto left = --references; if (!left) delete this; return left; }
};
}

extern "C" __attribute__((visibility("default"))) HRESULT STDMETHODCALLTYPE DllGetClassObject(REFCLSID id, REFIID iid, void** result)
{
    if (!result) return E_POINTER;
    *result = nullptr;
    if (id != ProfilerId) return E_FAIL;
    auto factory = new ClassFactory<Profiler>();
    auto hr = factory->QueryInterface(iid, result);
    if (FAILED(hr)) delete factory;
    return hr;
}

extern "C" __attribute__((visibility("default"))) int BeginAllocationRegion()
{
    std::lock_guard<std::mutex> lock(gate);
    if (!initialized || nativeTail || begun || !selectedMethod.empty()) return 0;
    begun = true; active = true;
    return 1;
}

namespace {
int writeReport()
{
    const char* path = std::getenv("RELAYER_ALLOCATION_OUTPUT");
    if (!path) return 0;
    std::ofstream output(path, std::ios::out | std::ios::trunc);
    if (!output) return 0;
    bool stacksWritten = true;
    if (!stackPath.empty()) {
        std::ofstream stacks(stackPath, std::ios::out | std::ios::trunc);
        for (const auto& frame : frameNames)
            stacks << "{\"kind\":\"frame\",\"functionId\":" << frame.first << ",\"name\":\"" << frame.second << "\"}\n";
        unsigned long long objects = 0, bytes = 0;
        for (const auto& entry : stackCounts) {
            const auto& [index, thread, type, hr, frames] = entry.first;
            stacks << "{\"kind\":\"allocation-stack\",\"index\":" << index << ",\"thread\":" << thread
                   << ",\"classId\":" << type << ",\"snapshotResult\":" << static_cast<long long>(hr)
                   << ",\"objects\":" << entry.second.objects << ",\"bytes\":" << entry.second.bytes << ",\"frames\":[";
            for (size_t i = 0; i < frames.size(); ++i) { if (i) stacks << ','; stacks << frames[i]; }
            stacks << "]}\n";
            objects += entry.second.objects; bytes += entry.second.bytes;
        }
        stacks << "{\"kind\":\"stack-coverage\",\"objects\":" << objects << ",\"bytes\":" << bytes
               << ",\"failures\":" << stackFailures
               << ",\"limits\":\"Managed frames on the allocating thread in this new instrumented execution. Inlining, native frames and unresolved names limit attribution. Window envelopes include unrelated parallel work; object bytes are not thread-counter bytes.\"}\n";
        stacks.flush();
        stacksWritten = stacks.good() && !stackFailures;
    }
    if (nativeTail) {
        for (const auto& event : engineEvents)
            output << "{\"kind\":\"engine-event\",\"id\":" << event.id << ",\"thread\":" << event.thread
                   << ",\"operations\":" << event.operations << "}\n";
        output << "{\"kind\":\"native-engine-tail\",\"nativeOperations\":" << nativeOperations
               << ",\"iterations\":" << iterations << ",\"stopped\":" << (benchmarkStopped ? "true" : "false") << "}\n";
    }
    for (size_t index = 0; index < windows.size(); ++index) {
        const auto& window = windows[index];
        unsigned long long objects = 0, bytes = 0;
        for (const auto& entry : window.counts) {
            objects += entry.second.objects; bytes += entry.second.bytes;
            output << "{\"kind\":\"window-type\",\"index\":" << index
                   << ",\"classId\":" << entry.first << ",\"type\":\"" << entry.second.name
                   << "\",\"objects\":" << entry.second.objects << ",\"bytes\":" << entry.second.bytes << "}\n";
        }
        output << "{\"kind\":\"window\",\"index\":" << index << ",\"mode\":" << index % 3
               << ",\"measured\":" << window.measured << ",\"objects\":" << objects << ",\"bytes\":" << bytes << "}\n";
    }
    unsigned long long objects = 0, bytes = 0;
    for (const auto& entry : counts) {
        objects += entry.second.objects; bytes += entry.second.bytes;
        output << "{\"kind\":\"type\",\"classId\":" << entry.first << ",\"type\":\"" << entry.second.name
               << "\",\"objects\":" << entry.second.objects << ",\"bytes\":" << entry.second.bytes << "}\n";
    }
    output << "{\"kind\":\"coverage\",\"objects\":" << objects << ",\"bytes\":" << bytes
           << ",\"failures\":" << failures
           << ",\"windows\":" << windows.size()
           << ",\"methodEntries\":" << methodEntries << ",\"methodLeaves\":" << methodLeaves
           << ",\"boundary\":\"" << (selectedMethod.empty() ? "external-marker" : "method-enter-leave") << "\""
           << ",\"limits\":\"Exact CLR ObjectAllocated callbacks in this separate instrumented region; callbacks change fast paths and scheduling. Native-tail mode includes final timed cleanup/logging and extra-stat setup/work/cleanup, not exact MeasureWithGc. "
           << (stackPath.empty() ? "No stacks. " : "Managed stacks are in the separate opt-in report. ")
           << "No historical replay or cross-control subtraction.\"}\n";
    output.flush();
    return output.good() && stacksWritten && !failures && objects ? 1 : 0;
}
}

extern "C" __attribute__((visibility("default"))) int EndAllocationRegion()
{
    std::lock_guard<std::mutex> lock(gate);
    if (!initialized || nativeTail || (!active && !windowsCompleted) || !selectedMethod.empty()) return 0;
    active = false;
    if (!windows.empty() && !windowsCompleted) ++failures;
    windowsCompleted = false;
    if (!windows.empty()) {
        if (windows.size() % 3 != 0) ++failures;
        // The final checkpoint stops capture atomically. Every observed allocation
        // must therefore belong to a checkpoint.
        for (const auto& entry : counts) {
            const auto found = previousCounts.find(entry.first);
            if (found == previousCounts.end() || found->second.objects != entry.second.objects
                || found->second.bytes != entry.second.bytes) ++failures;
        }
    }
    return writeReport();
}

// External probe markers partition this instrumented process envelope, not the
// fixture's thread-byte counters. Native lock order assigns racing allocations.
extern "C" __attribute__((visibility("default"))) int CheckpointAllocationRegion(int index, long long measured, int final)
{
    std::lock_guard<std::mutex> lock(gate);
    if (!initialized || nativeTail || !active || !selectedMethod.empty()
        || index < 0 || static_cast<size_t>(index) != windows.size() || measured < 0
        || (final != 0 && final != 1) || (final && index % 3 != 2)) {
        ++failures;
        return 0;
    }
    Window window{measured, {}};
    for (const auto& entry : counts) {
        auto previous = previousCounts.find(entry.first);
        const auto objects = entry.second.objects - (previous == previousCounts.end() ? 0 : previous->second.objects);
        const auto bytes = entry.second.bytes - (previous == previousCounts.end() ? 0 : previous->second.bytes);
        if (objects) window.counts.emplace(entry.first, Count{objects, bytes, entry.second.name});
    }
    previousCounts = counts;
    windows.push_back(std::move(window));
    if (final) { active = false; windowsCompleted = true; }
    return 1;
}
