cmake_minimum_required(VERSION 3.14)
project(HashTableBenchmark CXX)  

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Для Windows убираем конфликтующие флаги
if(MSVC)
    set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>")
    
    # Убираем RTC флаги
    foreach(config Debug Release RelWithDebInfo MinSizeRel)
        string(REPLACE "/RTC1" "" CMAKE_CXX_FLAGS_${config} "${CMAKE_CXX_FLAGS_${config}}")
        string(REPLACE "/RTCsu" "" CMAKE_CXX_FLAGS_${config} "${CMAKE_CXX_FLAGS_${config}}")
    endforeach()
endif()

# Настройки флагов
set(CMAKE_CXX_FLAGS_DEBUG "/MDd /Zi /Ob0 /Od" CACHE STRING "Debug flags" FORCE)
set(CMAKE_CXX_FLAGS_RELEASE "/MD /O2 /Ob2 /DNDEBUG" CACHE STRING "Release flags" FORCE)
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "/MD /Zi /O2 /Ob1 /DNDEBUG" CACHE STRING "RelWithDebInfo flags" FORCE)
set(CMAKE_CXX_FLAGS_MINSIZEREL "/MD /O1 /Ob1 /DNDEBUG" CACHE STRING "MinSizeRel flags" FORCE)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(Threads REQUIRED)

include(FetchContent)

# Отключаем поиск system-wide benchmark, всегда используем FetchContent
set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
set(BENCHMARK_ENABLE_GTEST_TESTS OFF CACHE BOOL "" FORCE)

FetchContent_Declare(
    benchmark
    GIT_REPOSITORY https://github.com/google/benchmark.git
    GIT_TAG v1.8.3
)

FetchContent_MakeAvailable(benchmark)

include_directories(include)

set(SOURCES
    src/HashTable.cpp  
    src/Benchmark.cpp
)

set(PROJ_NAME benchmarks)

add_executable(${PROJ_NAME} ${SOURCES})

target_include_directories(${PROJ_NAME} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(${PROJ_NAME} PRIVATE 
    benchmark::benchmark 
    Threads::Threads
)

# Настройки компилятора
if(MSVC)
    target_compile_options(${PROJ_NAME} PRIVATE
        $<$<CONFIG:Debug>:/MDd /Zi /Od>
        $<$<CONFIG:Release>:/O2 /Ob2>
        $<$<CONFIG:RelWithDebInfo>:/O2 /Zi>
        $<$<CONFIG:MinSizeRel>:/O1 /Os>
    )
    
    target_compile_definitions(${PROJ_NAME} PRIVATE
        $<$<CONFIG:Debug>:_DEBUG>
        $<$<NOT:$<CONFIG:Debug>>:NDEBUG>
        _SILENCE_CXX17_ALLOCATOR_VOID_DEPRECATION_WARNING
        _SILENCE_CXX20_CISO646_REMOVED_WARNING
    )
else()
    target_compile_options(${PROJ_NAME} PRIVATE
        $<$<CONFIG:Debug>:-O0 -g>
        $<$<CONFIG:Release>:-O2>
        $<$<Config:RelWithDebInfo>:-O2 -g>
        $<$<CONFIG:MinSizeRel>:-Os>
    )
endif()