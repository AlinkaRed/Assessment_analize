import pandas as pd
import os
from datetime import datetime
import pandas as pd
import psycopg2

def main():
    # Загрузка данных
    df = pd.read_excel('df1.xlsx') # здесь задайте путь к исходному файлу
    df.columns = df.columns.str.strip()
    df_original = df.copy()
    
    columns_to_drop = ['ID оценочной сессии', 'Опубликование макета', 'Трудоемкость программы',
                       'Срок реализации программы', 'Целевой уровень развития компетенции',
                       'Дата регистрации', 'Дата начала прохождения оценки']
    df.drop(columns=columns_to_drop, inplace=True)
    
    def convert_to_years(year_str):
        start_year = '20' + year_str.split('/')[0]
        end_year = '20' + year_str.split('/')[1]
        return start_year, end_year
    df[['Год начала', 'Год окончания']] = df['Поток'].apply(lambda x: pd.Series(convert_to_years(str(x))))
    
    df['Дата начала'] = pd.to_datetime(df['Год начала'] + '-01-01')
    df['Дата окончания'] = pd.to_datetime(df['Год окончания'] + '-01-01')
    
    numeric_cols = ['Результат', 'Количество попыток', 'Время результирующей попытки',
                   'Итоговый уровень сформированности компетенций']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # --- ОБЩАЯ СТАТИСТИКА АССЕССМЕНТА ---
    # Общие метрики
    num_participants = df['ID пользователя'].nunique()
    num_graduates = df[df['Статус'] == 'Завершено']['ID пользователя'].nunique()
    average_score = df['Результат'].mean()
    average_time = df['Время результирующей попытки'].mean()
    
    # Статистика завершения
    classification_by_direction = {}
    unique_directions = df['Наименование оценочной сессии'].unique()
    for direction in unique_directions:
        classification_by_direction[direction] = {
            "Не прошли все 3 этапа": 0,
            "Прошли все 3 этапа": 0,
            "Отчислены": 0
        }
    grouped = df.groupby('ID пользователя')
    for user_id, user_data in grouped:
        direction = user_data['Наименование оценочной сессии'].iloc[0]
        if "Отчислен" in user_data['Состояние'].values:
            classification_by_direction[direction]["Отчислены"] += 1
        else:
            completed_stages = user_data[user_data['Статус'] == 'Завершено']['Этап оценки'].nunique()
            if completed_stages == 3:
                classification_by_direction[direction]["Прошли все 3 этапа"] += 1
            else:
                classification_by_direction[direction]["Не прошли все 3 этапа"] += 1
    
    # Другие метрики
    target_achieved_students = df[df['Итоговый уровень развития компетенции'].isin(['Достигнут', 'Превышен'])]
    target_achieved_by_direction = target_achieved_students.groupby('Наименование оценочной сессии')['ID пользователя'].nunique()
    listeners_by_direction = df.groupby('Наименование оценочной сессии')['ID пользователя'].nunique()
    stage_1_data = df[df['Этап оценки'] == 1]
    participants_stage_1_by_direction = stage_1_data.groupby('Наименование оценочной сессии')['ID пользователя'].nunique()
    
    # --- УРОВЕНЬ КОМПЕТЕНЦИЙ НА ЭТАПАХ АССЕССМЕНТА ---
    df = df_original.copy()
    level_mapping = {
        'Минимальный исходный': 1,
        'Базовый': 2,
        'Продвинутый': 3,
        'Экспертный': 4
    }
    if df['Итоговый уровень сформированности компетенций'].dtype == 'object':
        df['Итоговый уровень сформированности компетенций'] = (
            df['Итоговый уровень сформированности компетенций']
            .map(level_mapping)
        )
    
    columns_to_drop = [
        'ID оценочной сессии', 'Опубликование макета', 'Отраслевая принадлежность',
        'Обучающиеся направления', 'Трудоемкость программы', 'Поток',
        'Срок реализации программы', 'ID пользователя', 'Дата регистрации',
        'Дата начала прохождения оценки', 'Статус', 'Результат',
        'Количество попыток', 'Время результирующей попытки', 'Состояние'
    ]
    df = df.drop(columns=columns_to_drop)
    df = df.dropna(subset=['Итоговый уровень сформированности компетенций'])
    try:
        grouped = df.groupby(['Этап оценки', 'Наименование оценочной сессии',
                             'Наименование компетенции', 'Итоговый уровень сформированности компетенций']).size().unstack(fill_value=0)
        if not grouped.empty:
            percentage_grouped = grouped.div(grouped.sum(axis=1), axis=0) * 100
            percentage_grouped = percentage_grouped.rename(columns={
                1: 'Минимальный исходный (%)',
                2: 'Базовый (%)',
                3: 'Продвинутый (%)',
                4: 'Экспертный (%)'
            })
        else:
            print("\nНет данных для анализа уровней сформированности компетенций")
    except Exception as e:
        print(f"\nОшибка при анализе уровней компетенций: {str(e)}")
    
    # --- СРЕДНИЕ ЗНАЧЕНИЯ ПО НАПРАВЛЕНИЯМ И ГОДАМ ---
    
    df = df_original.copy()
    df['Год'] = df['Наименование оценочной сессии'].str.extract(r'(\d{4})')
    if df['Год'].isnull().all():
        df['Год'] = df['Поток'].apply(lambda x: '20' + str(x).split('/')[0] if pd.notnull(x) else None)
    df['Результат'] = pd.to_numeric(df['Результат'], errors='coerce')
    df = df.dropna(subset=['Обучающиеся направления', 'Год', 'Результат'])
    
    # Рассчитываем средний балл по направлениям и годам
    average_score_by_direction = df.groupby(['Наименование оценочной сессии', 'Год'])['Результат'].mean().reset_index()
    
    # Рассчитываем среднее время по каждому этапу
    average_time_by_stage = df.groupby(['Этап оценки', 'Наименование оценочной сессии'])['Время результирующей попытки'].mean().reset_index()
    
    # Среднее количество попыток
    average_attempts_by_competency = df.groupby(['Год', 'Наименование оценочной сессии'])['Количество попыток'].mean()
    
    # Рассчитываем средний балл по этапам
    average_scores_by_stage = df.groupby(['Год', 'Этап оценки', 'Наименование оценочной сессии'])['Результат'].mean().reset_index()
    
    #Рассчитаем средний балл по компетенциям
    average_score_by_competency = df.groupby(['Год', 'Наименование компетенции'])['Результат'].mean()
    
    # Фильтруем данные по первому этапу
    first_stage = df[df['Этап оценки'] == 1]
    
    # Рассчитываем средний балл по компетенциям
    average_scores_by_competency = first_stage.groupby(['Год', 'Наименование компетенции'])['Результат'].mean().reset_index()
    
    # --- ВЛИЯНИЕ ВРЕМЕНИ НА РЕЗУЛЬТАТ АССЕССМЕНТА ---
    df = df_original.copy()
    df['Время результирующей попытки'] = pd.to_numeric(df['Время результирующей попытки'], errors='coerce')
    df['Итоговый уровень сформированности компетенций'] = df['Итоговый уровень сформированности компетенций'].fillna('Не указано')
    grouped_data = df.groupby(['Наименование оценочной сессии', 'Этап оценки'])
    def analyze_time_impact(group):
        avg_time = group['Время результирующей попытки'].mean()
        faster_than_avg = group[group['Время результирующей попытки'] < avg_time]
        slower_than_avg = group[group['Время результирующей попытки'] > avg_time]
    
        faster_results = faster_than_avg['Итоговый уровень сформированности компетенций'].value_counts(normalize=True) * 100
        slower_results = slower_than_avg['Итоговый уровень сформированности компетенций'].value_counts(normalize=True) * 100
    
        return faster_results, slower_results, avg_time
        
    for name, group in grouped_data:
        session_name, stage = name
        if len(group) < 5 or group['Время результирующей попытки'].isnull().all():
            continue
        try:
            faster_results, slower_results, avg_time = analyze_time_impact(group)
        except Exception as e:
            print(f"Ошибка при анализе для {session_name}, этап {stage}: {str(e)}")
            continue
    
    data_dict = {}
    
    # --- ОБРАБОТКА СТАТИСТИКИ ЗАВЕРШЕНИЯ ОБУЧЕНИЯ ---
    for direction, stats in classification_by_direction.items():
        for category, count in stats.items():
            data_dict[f"Статистика/{direction}/{category}"] = count
    
    # --- ОБРАБОТКА ДОСТИЖЕНИЯ ЦЕЛЕВОГО ПОКАЗАТЕЛЯ ---
    for direction, count in target_achieved_by_direction.items():
        data_dict[f"Целевые показатели/{direction}/Достигли"] = count
    
    # --- ОБРАБОТКА КОЛИЧЕСТВА СЛУШАТЕЛЕЙ ---
    for direction, count in listeners_by_direction.items():
        data_dict[f"Участники/{direction}/Общее количество"] = count
    
    # --- ОБРАБОТКА УЧАСТНИКОВ 1 ЭТАПА ---
    for direction, count in participants_stage_1_by_direction.items():
        data_dict[f"Участники/{direction}/1 этап"] = count
    
    # --- ОБРАБОТКА СРЕДНИХ ЗНАЧЕНИЙ ---
    if not average_score_by_direction.empty:
        for idx, row in average_score_by_direction.iterrows():
            direction = row['Наименование оценочной сессии']
            year = row['Год']
            data_dict[f"Средние значения/{direction}/Средний балл ({year})"] = row['Результат']
    
    if not average_time_by_stage.empty:
        for idx, row in average_time_by_stage.iterrows():
            direction = row['Наименование оценочной сессии']
            stage = row['Этап оценки']
            data_dict[f"Средние значения/{direction}/Среднее время (этап {stage})"] = row['Время результирующей попытки']
    
    # --- ОБРАБОТКА УРОВНЕЙ КОМПЕТЕНЦИЙ ---
    if 'percentage_grouped' in locals():
        for (stage, session, competence), data in percentage_grouped.iterrows():
            data_dict[f"Уровни компетенций/{session}/Этап {stage}/{competence}/Базовый (%)"] = data['Базовый (%)']
            data_dict[f"Уровни компетенций/{session}/Этап {stage}/{competence}/Продвинутый (%)"] = data['Продвинутый (%)']
            data_dict[f"Уровни компетенций/{session}/Этап {stage}/{competence}/Экспертный (%)"] = data['Экспертный (%)']
            data_dict[f"Уровни компетенций/{session}/Этап {stage}/{competence}/Минимальный исходный (%)"] = data['Минимальный исходный (%)']
    
    # --- ОБРАБОТКА ВЛИЯНИЯ ВРЕМЕНИ НА РЕЗУЛЬТАТЫ ---
    if 'grouped_data' in locals():
        for name, group in grouped_data:
            session_name, stage = name
            if len(group) < 5 or group['Время результирующей попытки'].isnull().all():
                continue
            
            try:
                faster_results, slower_results, avg_time = analyze_time_impact(group)
                
                # Добавляем среднее время
                data_dict[f"Влияние времени/{session_name}/Этап {stage}/Среднее время"] = avg_time
                
                # Добавляем результаты для быстрых участников по уровням
                for level, percent in faster_results.items():
                    level_name = {
                        1: "Минимальный исходный",
                        2: "Базовый",
                        3: "Продвинутый",
                        4: "Экспертный"
                    }.get(level, str(level))
                    data_dict[f"Влияние времени/{session_name}/Этап {stage}/Быстрее среднего/{level_name}"] = percent
                
                # Добавляем результаты для медленных участников по уровням
                for level, percent in slower_results.items():
                    level_name = {
                        1: "Минимальный исходный",
                        2: "Базовый",
                        3: "Продвинутый",
                        4: "Экспертный"
                    }.get(level, str(level))
                    data_dict[f"Влияние времени/{session_name}/Этап {stage}/Медленнее среднего/{level_name}"] = percent
                    
            except Exception as e:
                continue
    # --- ОБРАБОТКА СРЕДНИХ ЗНАЧЕНИЙ ПО КОМПЕТЕНЦИЯМ ---
    
    # Средний балл по компетенциям (все этапы вместе)
    if not average_score_by_competency.empty:
        for (year, competence), score in average_score_by_competency.items():
            data_dict[f"Средние значения по компетенциям/Все этапы/{year}/{competence}"] = score
    
    # Средний балл по компетенциям по этапам
    # if not average_scores_by_competency_stage.empty:
    #     for idx, row in average_scores_by_competency_stage.iterrows():
    #         year = row['Год']
    #         stage = row['Этап оценки']
    #         competence = row['Наименование компетенции']
    #         score = row['Результат']
    #         data_dict[f"Средние значения по компетенциям/Этап {stage}/{year}/{competence}"] = score
    
    # --- ОБРАБОТКА СРЕДНИХ ЗНАЧЕНИЙ ПО ЭТАПАМ ---
    if not average_scores_by_stage.empty:
        for idx, row in average_scores_by_stage.iterrows():
            year = row['Год']
            stage = row['Этап оценки']
            direction = row['Наименование оценочной сессии']
            score = row['Результат']
            data_dict[f"Средние значения по этапам/{direction}/Этап {stage} ({year})"] = score
    
    
    # --- СОЗДАЕМ ИТОГОВЫЙ DATAFRAME ---
    final_df = pd.DataFrame.from_dict(data_dict, orient='index', columns=['Значение'])
    
    # --- ФОРМАТИРУЕМ ВЫВОД ---
    final_df.index = final_df.index.str.split('/').map(lambda x: " | ".join(x))
    final_df = final_df.transpose()
    final_df.columns = [col for col in final_df.columns]
    final_df['Поток'] = df['Поток'].unique()
    final_df['Число участников'] = num_participants
    final_df['Число выпускников'] = num_graduates
    final_df['Средний балл'] = average_score
    final_df['Среднее время'] = average_time
    
    print(final_df)
    # --- СОХРАНЕНИЕ ---
    
    try:
        output_file = f"итоговый_анализ_{str(df['Поток'][0])[0:2]}.xlsx"
        
        # Пробуем сохранить на рабочий стол
        desktop_path = os.path.join(os.path.expanduser('~'), 'Desktop')
        save_path = os.path.join(desktop_path, output_file)
        
        # Создаем директорию если нужно
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        
        # Сохраняем файл с красивыми заголовками
        final_df.reset_index().rename(columns={'index': 'Метрика'}).to_excel(
            save_path, 
            index=False,
            sheet_name='Результаты асессмента'
        )
        print(f"\nФайл успешно сохранен: {save_path}")
        
    except PermissionError:
        print("\nОшибка: Нет доступа к файлу. Закройте Excel и попробуйте снова.")
    except Exception as e:
        print(f"\nОшибка при сохранении: {str(e)}")
        print("Попробуйте сохранить вручную:")
    
    dbname = 'Ассессмент'
    user = 'postgres'
    password = 'root'
    host = '127.0.0.1'
    port = 5432
    
    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    cursor = conn.cursor()
    
    for index, row in final_df.iterrows():
        sql = "INSERT INTO table_1 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" 
        cursor.execute(sql, tuple(row))
        
    conn.commit()
    cursor.close()
    conn.close()
    print("Данные успешно записаны в таблицу table_1")

if __name__ == "__main__":
    main()