// Investigation only. Allocation callbacks change allocation fast paths and scheduling.
// Exact counts describe this instrumented execution, never the historical gate.
#include "Callbacks.h"
#include "ClassFactory.h"
#include <cstdlib>
#include <fstream>
#include <map>
#include <mutex>
#include <sstream>
#include <string>

namespace {
constexpr GUID ProfilerId = {0x32b7ef07,0x25e8,0x4ac7,{0xb0,0x11,0x9a,0x43,0x21,0xd7,0x02,0xc8}};
struct Count { unsigned long long objects = 0, bytes = 0; std::string name; };
std::mutex gate;
bool initialized = false, active = false, begun = false;
unsigned long long failures = 0;
std::map<ClassID, Count> counts;
ICorProfilerInfo12* info = nullptr;

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

std::string definitionName(IMetaDataImport* metadata, mdTypeDef token, unsigned depth = 0)
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
        hr = info->SetEventMask2(COR_PRF_MONITOR_OBJECT_ALLOCATED | COR_PRF_ENABLE_OBJECT_ALLOCATED, 0);
        initialized = SUCCEEDED(hr);
        return hr;
    }
    HRESULT STDMETHODCALLTYPE Shutdown() override {
        std::lock_guard<std::mutex> lock(gate);
        active = false; initialized = false;
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
        return S_OK;
    }
    HRESULT STDMETHODCALLTYPE EventPipeEventDelivered(EVENTPIPE_PROVIDER, DWORD, DWORD, ULONG, LPCBYTE,
        ULONG, LPCBYTE, LPCGUID, LPCGUID, ThreadID, ULONG, UINT_PTR[]) override { return S_OK; }
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
    if (!initialized || begun) return 0;
    begun = true; active = true;
    return 1;
}

extern "C" __attribute__((visibility("default"))) int EndAllocationRegion()
{
    std::lock_guard<std::mutex> lock(gate);
    if (!initialized || !active) return 0;
    active = false;
    const char* path = std::getenv("RELAYER_ALLOCATION_OUTPUT");
    if (!path) return 0;
    std::ofstream output(path, std::ios::out | std::ios::trunc);
    if (!output) return 0;
    unsigned long long objects = 0, bytes = 0;
    for (const auto& entry : counts) {
        objects += entry.second.objects; bytes += entry.second.bytes;
        output << "{\"kind\":\"type\",\"classId\":" << entry.first << ",\"type\":\"" << entry.second.name
               << "\",\"objects\":" << entry.second.objects << ",\"bytes\":" << entry.second.bytes << "}\n";
    }
    output << "{\"kind\":\"coverage\",\"objects\":" << objects << ",\"bytes\":" << bytes
           << ",\"failures\":" << failures << ",\"limits\":\"Exact CLR ObjectAllocated callbacks in this separate instrumented region; callbacks change fast paths and scheduling. No stacks, historical replay or cross-control subtraction.\"}\n";
    output.flush();
    return output.good() && !failures && objects ? 1 : 0;
}
