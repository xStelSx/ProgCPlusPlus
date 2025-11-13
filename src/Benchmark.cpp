#define _SILENCE_CXX17_ALLOCATOR_VOID_DEPRECATION_WARNING
#define _SILENCE_CXX20_CISO646_REMOVED_WARNING

#include <benchmark/benchmark.h>
#include <algorithm>
#include <chrono>
#include <fstream>
#include <iostream>
#include <numeric>
#include <random>
#include <string>
#include <vector>
#include <unordered_map>
#include <set>
#include <iomanip>

#include "HashTable.h"

// Data generation
std::vector<int> generate_data(int n, const std::string& scenario) {
    std::vector<int> data;

    if (scenario == "random") {
        data.resize(n);
        std::iota(data.begin(), data.end(), 0);
        std::mt19937 rng(42);
        std::shuffle(data.begin(), data.end(), rng);
    }
    else if (scenario == "ascending") {
        data.resize(n);
        std::iota(data.begin(), data.end(), 0);
    }
    else if (scenario == "clustered") {
        data.reserve(n);
        std::mt19937 rng(42);
        std::uniform_int_distribution<int> dist(0, 99);
        for (int i = 0; i < n; ++i) {
            data.push_back(dist(rng) * 10);
        }
    }
    else if (scenario == "high_collision") {
        data.reserve(n);
        std::mt19937 rng(42);
        std::uniform_int_distribution<int> dist(0, 9);
        for (int i = 0; i < n; ++i) {
            data.push_back(dist(rng));
        }
    }
    return data;
}

// Save latency data
void save_latency_to_csv(const std::string& filename,
    const std::vector<long long>& latencies_ns,
    const std::string& operation,
    const std::string& scenario,
    int n, int method) {

    std::ofstream out(filename, std::ios::app);
    for (size_t i = 0; i < latencies_ns.size(); ++i) {
        out << operation << "," << scenario << "," << n << "," << method << "," << latencies_ns[i] << "\n";
    }
    out.close();
}

// Бенчмарк вставки с сохранением задержек
static void BM_Insert_Scenarios(benchmark::State& state) {
    const int N = state.range(0);
    int scenario_type = state.range(1);
    int method_type = state.range(2);

    const std::string scenario = (scenario_type == 0) ? "random" :
        (scenario_type == 1) ? "ascending" :
        (scenario_type == 2) ? "clustered" : "high_collision";

    auto method = static_cast<ProbingMethod>(method_type);
    auto data = generate_data(N, scenario);

    std::vector<long long> iteration_latencies;

    for (auto _ : state) {
        HashTable table;
        table.set_probing_method(method);
        table.reset_collision_count();

        auto start = std::chrono::high_resolution_clock::now();

        for (int x : data) {
            table.insert(x, "value_" + std::to_string(x));
        }

        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);

        // Сохраняем суммарное время для всех операций вставки
        iteration_latencies.push_back(duration.count());

        state.counters["Collisions"] = table.collision_count();
        state.counters["LoadFactor"] = static_cast<double>(table.size()) / table.capacity();
        state.counters["UniqueKeys"] = table.size();
    }

    // Сохраняем задержки для этой конфигурации
    save_latency_to_csv("insert_latencies.csv", iteration_latencies, "insert", scenario, N, method_type);
}

// Бенчмарк поиска с сохранением задержек
static void BM_Find(benchmark::State& state) {
    const int N = state.range(0);
    auto method = static_cast<ProbingMethod>(state.range(1));

    auto data = generate_data(N, "random");

    std::vector<long long> iteration_latencies;

    for (auto _ : state) {
        state.PauseTiming();
        HashTable table;
        table.set_probing_method(method);
        for (int x : data) {
            table.insert(x, "value_" + std::to_string(x));
        }
        state.ResumeTiming();

        auto start = std::chrono::high_resolution_clock::now();

        // Тестируем поиск ВСЕХ элементов один раз за итерацию
        for (int i = 0; i < N; ++i) {
            benchmark::DoNotOptimize(table.find(data[i]));
        }

        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);

        // Сохраняем СРЕДНЕЕ время на одну операцию поиска
        iteration_latencies.push_back(duration.count() / N);
    }

    save_latency_to_csv("find_latencies.csv", iteration_latencies, "find", "random", N, state.range(1));
}

// Бенчмарк удаления с сохранением задержек
static void BM_Erase(benchmark::State& state) {
    const int N = state.range(0);
    auto method = static_cast<ProbingMethod>(state.range(1));

    auto data = generate_data(N, "random");

    std::vector<long long> iteration_latencies;

    for (auto _ : state) {
        state.PauseTiming();
        HashTable table;
        table.set_probing_method(method);
        for (int x : data) {
            table.insert(x, "value_" + std::to_string(x));
        }
        state.ResumeTiming();

        auto start = std::chrono::high_resolution_clock::now();

        // Тестируем удаление ВСЕХ элементов один раз за итерацию
        for (int i = 0; i < N; ++i) {
            table.erase(data[i]);
        }

        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);

        // Сохраняем СРЕДНЕЕ время на одну операцию удаления
        iteration_latencies.push_back(duration.count() / N);
    }

    save_latency_to_csv("erase_latencies.csv", iteration_latencies, "erase", "random", N, state.range(1));
}

// Бенчмарк поиска существующих элементов
static void BM_Find_Existing(benchmark::State& state) {
    const int N = state.range(0);
    auto method = static_cast<ProbingMethod>(state.range(1));

    auto data = generate_data(N, "random");

    std::vector<long long> iteration_latencies;

    for (auto _ : state) {
        state.PauseTiming();
        HashTable table;
        table.set_probing_method(method);
        for (int x : data) {
            table.insert(x, "value_" + std::to_string(x));
        }
        state.ResumeTiming();

        auto start = std::chrono::high_resolution_clock::now();

        // Поиск существующих элементов
        for (int i = 0; i < N; ++i) {
            benchmark::DoNotOptimize(table.find(data[i]));
        }

        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);

        // Среднее время на одну операцию
        iteration_latencies.push_back(duration.count() / N);
    }

    save_latency_to_csv("find_existing_latencies.csv", iteration_latencies, "find_existing", "random", N, state.range(1));
}

// Бенчмарк поиска отсутствующих элементов
static void BM_Find_Missing(benchmark::State& state) {
    const int N = state.range(0);
    auto method = static_cast<ProbingMethod>(state.range(1));

    auto data = generate_data(N, "random");
    std::vector<int> missing_data;
    for (int i = N; i < 2 * N; ++i) {
        missing_data.push_back(i);
    }

    std::vector<long long> iteration_latencies;

    for (auto _ : state) {
        state.PauseTiming();
        HashTable table;
        table.set_probing_method(method);
        for (int x : data) {
            table.insert(x, "value_" + std::to_string(x));
        }
        state.ResumeTiming();

        auto start = std::chrono::high_resolution_clock::now();

        // Поиск отсутствующих элементов
        for (int i = 0; i < N; ++i) {
            benchmark::DoNotOptimize(table.find(missing_data[i]));
        }

        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);

        // Среднее время на одну операцию
        iteration_latencies.push_back(duration.count() / N);
    }

    save_latency_to_csv("find_missing_latencies.csv", iteration_latencies, "find_missing", "random", N, state.range(1));
}

// Сравнение с STL с сохранением задержек
static void BM_STL_Compare(benchmark::State& state) {
    const int N = state.range(0);
    int scenario_type = state.range(1);
    const std::string scenario = (scenario_type == 0) ? "random" : "clustered";

    auto data = generate_data(N, scenario);

    std::vector<long long> iteration_latencies;

    for (auto _ : state) {
        state.PauseTiming();
        std::unordered_map<int, std::string> map;
        map.reserve(N);
        state.ResumeTiming();

        auto start = std::chrono::high_resolution_clock::now();

        for (int x : data) {
            map[x] = "value_" + std::to_string(x);
        }

        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
        iteration_latencies.push_back(duration.count());
    }

    save_latency_to_csv("stl_latencies.csv", iteration_latencies, "stl_insert", scenario, N, -1);
}

// Сравнение поиска с STL
static void BM_STL_Find(benchmark::State& state) {
    const int N = state.range(0);

    auto data = generate_data(N, "random");

    std::vector<long long> iteration_latencies;

    for (auto _ : state) {
        state.PauseTiming();
        std::unordered_map<int, std::string> map;
        for (int x : data) {
            map[x] = "value_" + std::to_string(x);
        }
        state.ResumeTiming();

        auto start = std::chrono::high_resolution_clock::now();

        // Поиск всех элементов
        for (int i = 0; i < N; ++i) {
            benchmark::DoNotOptimize(map.find(data[i]));
        }

        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);

        // Среднее время на одну операцию
        iteration_latencies.push_back(duration.count() / N);
    }

    save_latency_to_csv("stl_find_latencies.csv", iteration_latencies, "stl_find", "random", N, -1);
}

// Тест коллизий
static void BM_Collision_Test(benchmark::State& state) {
    for (auto _ : state) {
        HashTable table;
        table.set_probing_method(ProbingMethod::LINEAR);
        table.reset_collision_count();

        // Create guaranteed collisions
        std::vector<int> collision_keys;
        for (int i = 0; i < 8; ++i) {
            collision_keys.push_back(i * 16);
        }

        for (int key : collision_keys) {
            table.insert(key, "test");
        }

        state.counters["Collisions"] = table.collision_count();
        state.counters["LoadFactor"] = static_cast<double>(table.size()) / table.capacity();
        state.counters["TableSize"] = table.size();
    }
}

// Бенчмарк Upsert операций
static void BM_Upsert(benchmark::State& state) {
    const int N = state.range(0);
    auto method = static_cast<ProbingMethod>(state.range(1));

    auto data = generate_data(N, "random");

    std::vector<long long> iteration_latencies;

    for (auto _ : state) {
        HashTable table;
        table.set_probing_method(method);
        table.reset_collision_count();

        auto start = std::chrono::high_resolution_clock::now();

        for (int x : data) {
            table.upsert(x, "value_" + std::to_string(x));
        }

        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);

        iteration_latencies.push_back(duration.count());
        state.counters["Collisions"] = table.collision_count();
    }

    save_latency_to_csv("upsert_latencies.csv", iteration_latencies, "upsert", "random", N, state.range(1));
}

// ========== РЕГИСТРАЦИЯ БЕНЧМАРКОВ ==========

// Тест коллизий
BENCHMARK(BM_Collision_Test);

// Основные бенчмарки вставки
BENCHMARK(BM_Insert_Scenarios)
->Args({ 1024, 0, 0 })  // N=1024, random, double_hashing
->Args({ 1024, 0, 1 })  // N=1024, random, linear
->Args({ 1024, 0, 2 })  // N=1024, random, quadratic
->Args({ 1024, 1, 0 })  // N=1024, ascending, double_hashing
->Args({ 1024, 2, 0 })  // N=1024, clustered, double_hashing
->Args({ 1024, 3, 0 })  // N=1024, high_collision, double_hashing
->Unit(benchmark::kMicrosecond);

// Бенчмарки поиска
BENCHMARK(BM_Find)
->Args({ 1024, 0 })  // N=1024, double_hashing
->Args({ 1024, 1 })  // N=1024, linear
->Args({ 1024, 2 })  // N=1024, quadratic
->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_Find_Existing)
->Args({ 1024, 0 })  // N=1024, double_hashing
->Args({ 1024, 1 })  // N=1024, linear
->Args({ 1024, 2 })  // N=1024, quadratic
->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_Find_Missing)
->Args({ 1024, 0 })  // N=1024, double_hashing
->Args({ 1024, 1 })  // N=1024, linear
->Args({ 1024, 2 })  // N=1024, quadratic
->Unit(benchmark::kMicrosecond);

// Бенчмарки удаления
BENCHMARK(BM_Erase)
->Args({ 1024, 0 })  // N=1024, double_hashing
->Args({ 1024, 1 })  // N=1024, linear
->Args({ 1024, 2 })  // N=1024, quadratic
->Unit(benchmark::kMicrosecond);

// Бенчмарки Upsert
BENCHMARK(BM_Upsert)
->Args({ 1024, 0 })  // N=1024, double_hashing
->Args({ 1024, 1 })  // N=1024, linear
->Args({ 1024, 2 })  // N=1024, quadratic
->Unit(benchmark::kMicrosecond);

// Сравнение с STL
BENCHMARK(BM_STL_Compare)
->Args({ 1024, 0 })  // N=1024, random
->Args({ 1024, 1 })  // N=1024, clustered
->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_STL_Find)
->Args({ 1024, 0 })  // N=1024
->Unit(benchmark::kMicrosecond);

// Инициализация CSV файлов
void initialize_csv_files() {
    std::vector<std::string> files = {
        "insert_latencies.csv",
        "find_latencies.csv",
        "find_existing_latencies.csv",
        "find_missing_latencies.csv",
        "erase_latencies.csv",
        "upsert_latencies.csv",
        "stl_latencies.csv",
        "stl_find_latencies.csv"
    };

    for (const auto& file : files) {
        std::ofstream out(file);
        out << "operation,scenario,n,method,latency_ns\n";
        out.close();
    }
}

int main(int argc, char** argv) {
    initialize_csv_files();

    std::cout << "Running hash table benchmarks with CSV export..." << std::endl;
    std::cout << "Load factor threshold: 0.5" << std::endl;
    std::cout << "CSV files will be created with latency data" << std::endl;
    std::cout << "Note: Find/Erase operations measure AVERAGE time per operation" << std::endl;

    ::benchmark::Initialize(&argc, argv);
    ::benchmark::RunSpecifiedBenchmarks();
    ::benchmark::Shutdown();

    std::cout << "Benchmarks completed! CSV files created." << std::endl;
    return 0;
}