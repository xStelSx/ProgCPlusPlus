import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from matplotlib.backends.backend_pdf import PdfPages
import numpy as np
import os

# Настройка стиля графиков
sns.set(style="whitegrid")
plt.rcParams["figure.figsize"] = (12, 8)
plt.rcParams["font.size"] = 12

# Цвета для разных методов пробирования
METHOD_COLORS = {
    0: 'steelblue',  # Double Hashing
    1: 'coral',  # Linear Probing
    2: 'green',  # Quadratic Probing
    -1: 'purple'  # STL
}

METHOD_NAMES = {
    0: 'Double Hashing',
    1: 'Linear Probing',
    2: 'Quadratic Probing',
    -1: 'STL unordered_map'
}

SCENARIO_NAMES = {
    'random': 'Случайные ключи',
    'ascending': 'Возрастающая последовательность',
    'clustered': 'Кластеризованные ключи',
    'high_collision': 'Высокая коллизия'
}

OPERATION_NAMES = {
    'insert': 'Вставка',
    'find': 'Поиск',
    'erase': 'Удаление',
    'find_existing': 'Поиск (существ.)',
    'find_missing': 'Поиск (отсутств.)',
    'upsert': 'Upsert',
    'stl_insert': 'STL Вставка',
    'stl_find': 'STL Поиск'
}


def load_and_prepare_data(csv_directory):
    """Загрузка и подготовка данных из CSV файлов"""
    data_dir = Path(csv_directory)
    print(f" Ищем CSV файлы в: {data_dir.absolute()}")

    csv_files = list(data_dir.glob("*.csv"))

    if not csv_files:
        available_files = list(data_dir.glob("*"))
        print(f" Доступные файлы в директории:")
        for f in available_files:
            print(f"   - {f.name}")
        raise FileNotFoundError(f"CSV файлы не найдены в директории {data_dir}")

    print(f" Найдено CSV файлов: {len(csv_files)}")

    all_data = []
    for file in csv_files:
        try:
            df = pd.read_csv(file)
            print(f" Загружен: {file.name} ({len(df)} записей)")

            # Автоматическое определение единиц измерения
            if 'latency_ns' in df.columns:
                sample_values = df['latency_ns'].head(3).tolist()
                print(f"   Примеры latency_ns: {sample_values}")

                median_val = df['latency_ns'].median()
                print(f"   Медиана latency_ns: {median_val}")

                # Явная конвертация в микросекунды
                df['latency_us'] = df['latency_ns'] / 1000.0
                print(f"    Данные конвертированы из наносекунд в микросекунды")

            else:
                print(f"    Нет колонки latency_ns в файле")
                continue

            df['source_file'] = file.name
            all_data.append(df)

        except Exception as e:
            print(f" Ошибка загрузки {file.name}: {e}")
            continue

    if not all_data:
        raise ValueError("Не удалось загрузить ни одного CSV файла")

    combined_df = pd.concat(all_data, ignore_index=True)

    # Очистка данных
    combined_df = combined_df.dropna(subset=['latency_us'])
    combined_df['latency_us'] = pd.to_numeric(combined_df['latency_us'], errors='coerce')
    combined_df = combined_df.dropna(subset=['latency_us'])

    # Добавляем читаемые названия
    combined_df['method_name'] = combined_df['method'].map(METHOD_NAMES)
    combined_df['scenario_name'] = combined_df['scenario'].map(SCENARIO_NAMES)
    combined_df['operation_name'] = combined_df['operation'].map(OPERATION_NAMES)

    combined_df['method_name'] = combined_df['method_name'].fillna('Unknown')
    combined_df['scenario_name'] = combined_df['scenario_name'].fillna('Unknown')
    combined_df['operation_name'] = combined_df['operation_name'].fillna(combined_df['operation'])

    print(f"\n ФИНАЛЬНАЯ СТАТИСТИКА ДО ФИЛЬТРАЦИИ:")
    print(f"   Всего записей: {len(combined_df)}")
    print(f"   Задержки: {combined_df['latency_us'].min():.1f} - {combined_df['latency_us'].max():.1f} мкс")
    print(f"   Медиана: {combined_df['latency_us'].median():.1f} мкс")

    # Анализ операций перед фильтрацией
    print(f"\n АНАЛИЗ ОПЕРАЦИЙ ДО ФИЛЬТРАЦИИ:")
    for operation in combined_df['operation'].unique():
        op_data = combined_df[combined_df['operation'] == operation]
        print(f"   {operation}: {len(op_data)} записей, "
              f"медиана: {op_data['latency_us'].median():.2f} мкс")

    # ФИЛЬТРАЦИЯ ВЫБРОСОВ
    print(f"\n🗑️ ФИЛЬТРАЦИЯ ВЫБРОСОВ:")
    before_filter = len(combined_df)

    # Разные пределы для разных операций
    limits = {
        'insert': 500,  # 500 μs (суммарное время для N операций)
        'upsert': 500,  # 500 μs
        'stl_insert': 500,  # 500 μs
        'find': 20,  # 20 μs (нормализованное время на операцию)
        'erase': 20,  # 20 μs (нормализованное время на операцию)
        'find_existing': 20,  # 20 μs
        'find_missing': 30,  # 30 μs
        'stl_find': 5  # 5 μs
    }

    filtered_dfs = []
    for operation, limit in limits.items():
        op_data = combined_df[combined_df['operation'] == operation]
        if len(op_data) > 0:
            filtered = op_data[op_data['latency_us'] <= limit]
            removed = len(op_data) - len(filtered)
            if removed > 0:
                print(f"   {operation}: удалено {removed} выбросов (> {limit} мкс)")
            filtered_dfs.append(filtered)
        else:
            # Если операции нет в данных, добавляем пустой DataFrame
            filtered_dfs.append(pd.DataFrame(columns=combined_df.columns))

    combined_df = pd.concat(filtered_dfs, ignore_index=True)
    after_filter = len(combined_df)
    print(f"📊 Итог фильтрации: {before_filter} → {after_filter} записей")

    # Анализ после фильтрации
    print(f"\n СТАТИСТИКА ПОСЛЕ ФИЛЬТРАЦИИ:")
    for operation in combined_df['operation'].unique():
        op_data = combined_df[combined_df['operation'] == operation]
        if len(op_data) > 0:
            print(f"   {operation}: {len(op_data)} записей, "
                  f"медиана: {op_data['latency_us'].median():.2f} мкс")

    return combined_df


def create_latency_distributions(df, output_pdf="hash_table_performance_analysis.pdf"):
    """Создание графиков распределения задержек"""

    print(f"\n Создание графиков в {output_pdf}...")

    with PdfPages(output_pdf) as pdf:

        # 1. Сравнение методов пробирования для вставки (случайные данные)
        print(" График 1: Сравнение методов вставки")
        plt.figure(figsize=(14, 10))

        insert_random = df[(df['operation'] == 'insert') & (df['scenario'] == 'random')]
        print(f"   Данные для графика: {len(insert_random)} записей")

        has_data = False
        for method in [0, 1, 2]:
            method_data = insert_random[insert_random['method'] == method]
            if len(method_data) > 0:
                has_data = True
                # Используем разумное усечение для лучшей визуализации
                cutoff = np.percentile(method_data['latency_us'], 95)
                trimmed_data = method_data[method_data['latency_us'] <= cutoff]

                sns.histplot(
                    data=trimmed_data,
                    x='latency_us',
                    label=f"{METHOD_NAMES[method]} (n={len(trimmed_data)})",
                    color=METHOD_COLORS[method],
                    alpha=0.7,
                    bins=50,
                    kde=True,
                    stat='density'
                )

        if has_data:
            plt.title('Распределение задержек вставки: Сравнение методов пробирования\n(Случайные данные, N=1024)',
                      fontsize=16, fontweight='bold')
            plt.xlabel('Задержка (микросекунды)', fontsize=14)
            plt.ylabel('Плотность вероятности', fontsize=14)
            plt.legend(fontsize=12)
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            pdf.savefig()
            plt.close()
        else:
            print("    Нет данных для графика 1")

        # 2. Сравнение сценариев данных для Double Hashing
        print(" График 2: Сравнение сценариев данных")
        plt.figure(figsize=(14, 10))

        double_hashing_data = df[(df['operation'] == 'insert') & (df['method'] == 0)]
        print(f"   Данные для графика: {len(double_hashing_data)} записей")

        scenarios = ['random', 'ascending', 'clustered', 'high_collision']
        colors = ['steelblue', 'orange', 'red', 'purple']

        has_data = False
        for scenario, color in zip(scenarios, colors):
            scenario_data = double_hashing_data[double_hashing_data['scenario'] == scenario]
            if len(scenario_data) > 0:
                has_data = True
                cutoff = np.percentile(scenario_data['latency_us'], 95)
                trimmed_data = scenario_data[scenario_data['latency_us'] <= cutoff]

                sns.histplot(
                    data=trimmed_data,
                    x='latency_us',
                    label=f"{SCENARIO_NAMES[scenario]} (n={len(trimmed_data)})",
                    color=color,
                    alpha=0.7,
                    bins=50,
                    kde=True,
                    stat='density'
                )

        if has_data:
            plt.title('Распределение задержек вставки: Влияние распределения ключей\n(Double Hashing, N=1024)',
                      fontsize=16, fontweight='bold')
            plt.xlabel('Задержка (микросекунды)', fontsize=14)
            plt.ylabel('Плотность вероятности', fontsize=14)
            plt.legend(fontsize=12)
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            pdf.savefig()
            plt.close()
        else:
            print("   ️ Нет данных для графика 2")

        # 3. Сравнение операций поиска и удаления (нормализованное время)
        print("📈 График 3: Сравнение операций поиска и удаления")
        plt.figure(figsize=(14, 10))

        normalized_ops = df[(df['scenario'] == 'random') &
                            (df['method'] == 2) &  # Quadratic
                            (df['operation'].isin(['find', 'erase', 'find_existing', 'find_missing']))]
        print(f"   Данные для графика: {len(normalized_ops)} записей")

        operations = ['find', 'erase', 'find_existing', 'find_missing']
        op_names = ['Поиск (все)', 'Удаление', 'Поиск (существ.)', 'Поиск (отсутств.)']
        colors = ['green', 'red', 'blue', 'orange']

        has_data = False
        for op, op_name, color in zip(operations, op_names, colors):
            op_data = normalized_ops[normalized_ops['operation'] == op]
            if len(op_data) > 0:
                has_data = True
                # Более агрессивное усечение для нормализованных данных
                cutoff = np.percentile(op_data['latency_us'], 99)
                trimmed_data = op_data[op_data['latency_us'] <= cutoff]

                print(f"   {op_name}: {len(trimmed_data)} записей, "
                      f"медиана: {trimmed_data['latency_us'].median():.3f} мкс")

                sns.histplot(
                    data=trimmed_data,
                    x='latency_us',
                    label=f"{op_name} (n={len(trimmed_data)})",
                    color=color,
                    alpha=0.7,
                    bins=30,
                    kde=True,
                    stat='density'
                )

        if has_data:
            plt.title('Сравнение задержек операций поиска и удаления\n(Quadratic Probing, Случайные данные, N=1024)',
                      fontsize=16, fontweight='bold')
            plt.xlabel('Задержка на операцию (микросекунды)', fontsize=14)
            plt.ylabel('Плотность вероятности', fontsize=14)
            plt.legend(fontsize=12)
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            pdf.savefig()
            plt.close()
        else:
            print("   ️ Нет данных для графика 3")

        # 4. Сравнение с STL
        print("📈 График 4: Сравнение с STL")
        plt.figure(figsize=(14, 10))

        stl_comparison = df[((df['operation'] == 'insert') & (df['scenario'] == 'random')) |
                            (df['operation'] == 'stl_insert')]
        print(f"   Данные для графика: {len(stl_comparison)} записей")

        custom_hash = stl_comparison[(stl_comparison['operation'] == 'insert') &
                                     (stl_comparison['method'] == 2)]  # Quadratic
        stl_hash = stl_comparison[stl_comparison['operation'] == 'stl_insert']

        if len(custom_hash) > 0 and len(stl_hash) > 0:
            cutoff_custom = np.percentile(custom_hash['latency_us'], 95)
            cutoff_stl = np.percentile(stl_hash['latency_us'], 95)

            trimmed_custom = custom_hash[custom_hash['latency_us'] <= cutoff_custom]
            trimmed_stl = stl_hash[stl_hash['latency_us'] <= cutoff_stl]

            plt.hist(trimmed_custom['latency_us'], bins=50, alpha=0.7, color='blue',
                     label=f'Кастомная хэш-таблица (Quadratic) (n={len(trimmed_custom)})', density=True)
            plt.hist(trimmed_stl['latency_us'], bins=50, alpha=0.7, color='red',
                     label=f'STL unordered_map (n={len(trimmed_stl)})', density=True)

            plt.title('Сравнение производительности: Кастомная vs STL реализация\n(Вставка, Случайные данные, N=1024)',
                      fontsize=16, fontweight='bold')
            plt.xlabel('Задержка (микросекунды)', fontsize=14)
            plt.ylabel('Плотность вероятности', fontsize=14)
            plt.legend(fontsize=12)
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            pdf.savefig()
            plt.close()
        else:
            print("   ⚠ Недостаточно данных для сравнения с STL")

        # 5. Bar chart средних задержек вставки
        print(" График 5: Средние задержки вставки по методам")
        plt.figure(figsize=(12, 8))

        insert_random = df[(df['operation'] == 'insert') & (df['scenario'] == 'random')]

        methods = []
        medians = []

        for method in [0, 1, 2]:
            method_data = insert_random[insert_random['method'] == method]
            if len(method_data) > 0:
                methods.append(METHOD_NAMES[method])
                medians.append(method_data['latency_us'].median())

        if methods:
            x = np.arange(len(methods))

            bars = plt.bar(x, medians, alpha=0.7,
                           color=['steelblue', 'coral', 'green'],
                           width=0.6)

            plt.title('Сравнение задержек вставки по методам\n(Случайные данные, N=1024)',
                      fontsize=16, fontweight='bold')
            plt.xlabel('Метод пробирования', fontsize=14)
            plt.ylabel('Задержка (микросекунды)', fontsize=14)
            plt.xticks(x, methods)

            # Добавление значений на столбцы
            for i, (bar, median) in enumerate(zip(bars, medians)):
                height = bar.get_height()
                plt.text(bar.get_x() + bar.get_width() / 2., height + 1,
                         f'{median:.1f}', ha='center', va='bottom', fontsize=12)

            plt.grid(True, alpha=0.3, axis='y')
            plt.tight_layout()
            pdf.savefig()
            plt.close()
        else:
            print("   ️ Нет данных для bar chart")

        # 6. Сравнение STL поиска с кастомной реализацией
        print(" График 6: Сравнение поиска STL vs кастомная")
        plt.figure(figsize=(14, 10))

        search_comparison = df[((df['operation'] == 'find') & (df['scenario'] == 'random') & (df['method'] == 2)) |
                               (df['operation'] == 'stl_find')]
        print(f"   Данные для графика: {len(search_comparison)} записей")

        custom_search = search_comparison[search_comparison['operation'] == 'find']
        stl_search = search_comparison[search_comparison['operation'] == 'stl_find']

        if len(custom_search) > 0 and len(stl_search) > 0:
            cutoff_custom = np.percentile(custom_search['latency_us'], 95)
            cutoff_stl = np.percentile(stl_search['latency_us'], 95)

            trimmed_custom = custom_search[custom_search['latency_us'] <= cutoff_custom]
            trimmed_stl = stl_search[stl_search['latency_us'] <= cutoff_stl]

            plt.hist(trimmed_custom['latency_us'], bins=50, alpha=0.7, color='blue',
                     label=f'Кастомная (Quadratic) (n={len(trimmed_custom)})', density=True)
            plt.hist(trimmed_stl['latency_us'], bins=50, alpha=0.7, color='red',
                     label=f'STL unordered_map (n={len(trimmed_stl)})', density=True)

            plt.title('Сравнение производительности поиска: Кастомная vs STL\n(Случайные данные, N=1024)',
                      fontsize=16, fontweight='bold')
            plt.xlabel('Задержка на операцию поиска (микросекунды)', fontsize=14)
            plt.ylabel('Плотность вероятности', fontsize=14)
            plt.legend(fontsize=12)
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            pdf.savefig()
            plt.close()
        else:
            print("   ⚠️ Недостаточно данных для сравнения поиска с STL")


def create_statistical_summary(df):
    """Создание статистической сводки"""
    print("\n" + "=" * 60)
    print("СТАТИСТИЧЕСКАЯ СВОДКА ПРОИЗВОДИТЕЛЬНОСТИ")
    print("=" * 60)

    # Анализ для вставки со случайными данными
    insert_random = df[(df['operation'] == 'insert') & (df['scenario'] == 'random')]

    print(f"\n📊 ОБЩИЙ АНАЛИЗ ВСТАВКИ (случайные данные, N=1024):")
    print(f"Всего измерений: {len(insert_random)}")

    for method in [0, 1, 2]:
        method_data = insert_random[insert_random['method'] == method]
        if len(method_data) > 0:
            latencies = method_data['latency_us']
            print(f"\n{METHOD_NAMES[method]} (n={len(latencies)}):")
            print(f"   Медиана: {latencies.median():.2f} мкс")
            print(f"   Среднее: {latencies.mean():.2f} мкс")
            print(f"   STD:     {latencies.std():.2f} мкс")
            print(f"   P95:     {np.percentile(latencies, 95):.2f} мкс")
            print(f"   Min:     {latencies.min():.2f} мкс")
            print(f"   Max:     {latencies.max():.2f} мкс")

    # Анализ операций поиска и удаления
    print(f"\n АНАЛИЗ ОПЕРАЦИЙ ПОИСКА И УДАЛЕНИЯ (Quadratic Probing):")
    quadratic_data = df[(df['scenario'] == 'random') & (df['method'] == 2)]

    for op, op_name in [('find', 'Поиск'), ('erase', 'Удаление'),
                        ('find_existing', 'Поиск (существ.)'),
                        ('find_missing', 'Поиск (отсутств.)')]:
        op_data = quadratic_data[quadratic_data['operation'] == op]
        if len(op_data) > 0:
            latencies = op_data['latency_us']
            print(f"\n{op_name} (n={len(latencies)}):")
            print(f"   Медиана: {latencies.median():.3f} мкс")
            print(f"   Среднее: {latencies.mean():.3f} мкс")
            print(f"   STD:     {latencies.std():.3f} мкс")
            print(f"   P95:     {np.percentile(latencies, 95):.3f} мкс")

    # Сравнение с STL
    print(f"\n СРАВНЕНИЕ С STL:")
    stl_insert = df[df['operation'] == 'stl_insert']
    custom_insert = df[(df['operation'] == 'insert') & (df['method'] == 2)]

    if len(stl_insert) > 0 and len(custom_insert) > 0:
        stl_median = stl_insert['latency_us'].median()
        custom_median = custom_insert['latency_us'].median()
        ratio = custom_median / stl_median if stl_median > 0 else float('inf')

        print(f"  STL Вставка:     {stl_median:.2f} мкс")
        print(f"  Кастомная Вставка: {custom_median:.2f} мкс")
        print(f"  Отношение: {ratio:.2f}x")

    stl_find = df[df['operation'] == 'stl_find']
    custom_find = df[(df['operation'] == 'find') & (df['method'] == 2)]

    if len(stl_find) > 0 and len(custom_find) > 0:
        stl_median = stl_find['latency_us'].median()
        custom_median = custom_find['latency_us'].median()
        ratio = custom_median / stl_median if stl_median > 0 else float('inf')

        print(f"  STL Поиск:       {stl_median:.3f} мкс")
        print(f"  Кастомная Поиск: {custom_median:.3f} мкс")
        print(f"  Отношение: {ratio:.2f}x")


def main():
    """Основная функция"""
    try:
        print(" Анализ производительности хэш-таблицы")
        print("=" * 50)

        # Указываем путь к CSV файлам
        csv_directory = r"E:\ITMO\HighCPlusPlus\PlusLabOnev3\out\build\x64-Debug"

        if not os.path.exists(csv_directory):
            print(f"❌ Директория не существует: {csv_directory}")
            print("Пожалуйста, проверьте путь и запустите бенчмарки для создания CSV файлов")
            return

        print(f"📁 Рабочая директория: {csv_directory}")

        # Загружаем данные
        print("Загрузка данных...")
        df = load_and_prepare_data(csv_directory)

        print(f"\n Данные успешно загружены и обработаны!")
        print(f" Всего записей после фильтрации: {len(df)}")
        print(f" Операции: {list(df['operation'].unique())}")
        print(f" Сценарии: {list(df['scenario'].unique())}")
        print(f" Методы: {list(df['method_name'].unique())}")

        # Создаём графики
        output_file = "hash_table_performance_analysis.pdf"
        print(f"\n Создание графиков...")
        create_latency_distributions(df, output_file)

        # Создаём статистическую сводку
        create_statistical_summary(df)

        result_path = Path.cwd() / output_file
        print(f"\n Анализ завершён!")
        print(f" Отчёт сохранён: {result_path.absolute()}")
        print(f"\n Создано 6 ключевых графиков:")
        print("  1.  Сравнение методов пробирования (вставка)")
        print("  2.  Влияние распределения ключей")
        print("  3.  Сравнение операций поиска и удаления")
        print("  4.  Сравнение с STL (вставка)")
        print("  5.  Bar chart средних задержек вставки")
        print("  6.  Сравнение поиска с STL")

    except Exception as e:
        print(f"\n Ошибка: {e}")
        import traceback
        print(f"\n Детали ошибки:")
        traceback.print_exc()
        print(f"\n Возможные решения:")
        print("   • Убедитесь, что бенчмарки были запущены и создали CSV файлы")
        print("   • Проверьте путь к директории с CSV файлами")
        print("   • Убедитесь, что CSV файлы содержат колонку 'latency_ns'")


if __name__ == "__main__":
    main()