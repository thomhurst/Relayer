cmake_minimum_required(VERSION 3.20)
project(FrozenAllocationProfiler LANGUAGES CXX)
# Pinned official SDK/scaffolding; the workflow retains these inputs too.
set(PROFILER_SAMPLES "" CACHE PATH "dotnet/samples checkout at 3810558800760fc39282cc939bac645735514209")
find_package(Python3 REQUIRED COMPONENTS Interpreter)
execute_process(COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/generate_callbacks.py
    ${PROFILER_SAMPLES}/core/profiling/eventpipe/src/EventPipeProfiler.h
    ${CMAKE_CURRENT_BINARY_DIR}/Callbacks.h COMMAND_ERROR_IS_FATAL ANY)
add_library(FrozenAllocationProfiler SHARED Profiler.cpp
    ${PROFILER_SAMPLES}/core/profiling/common/guids.cpp)
target_include_directories(FrozenAllocationProfiler PRIVATE ${CMAKE_CURRENT_BINARY_DIR}
    ${PROFILER_SAMPLES}/core/profiling/common
    ${PROFILER_SAMPLES}/core/profiling/common/sdk
    ${PROFILER_SAMPLES}/core/profiling/common/unix)
target_compile_features(FrozenAllocationProfiler PRIVATE cxx_std_17)
target_compile_options(FrozenAllocationProfiler PRIVATE -fms-extensions -fPIC
    -Wno-pragma-pack -Wno-ignored-attributes -Wno-invalid-noreturn)
target_compile_definitions(FrozenAllocationProfiler PRIVATE BIT64 PAL_STDCPP_COMPAT PLATFORM_UNIX HOST_64BIT)
target_link_libraries(FrozenAllocationProfiler PRIVATE pthread)
