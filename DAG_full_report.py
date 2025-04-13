from datetime import datetime, timedelta
import telegram
import numpy as np
import matplotlib.pyplot as plt
import io
import seaborn as sns
import pandas as pd
import pandahouse as ph

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

my_token = '8007338291:AAEsaQUsvT1-NuycoTN2f3oz-oLWJyp2tHc' 
bot = telegram.Bot(token=my_token)
chat_id = -938659451

default_args = {
    'owner': 's.savchits',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 3, 22),
}   

schedule_interval = '0 11 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)

def dag_full_peport_s_savchits_v4():
    
    @task()
    def full_report_sergey_savchits(chat_id):
        
        connection = {'host': 'https://clickhouse.lab.karpov.courses',
        'database':'simulator_20250220',
        'user':'student',
        'password':'dpo_python_2020'
        }
    
        query_1 = """
            SELECT this_week,
                   status,
                   count_users
              FROM       
             (SELECT this_week,
                     status,
                     toInt64(-uniq(user_id)) AS count_users
                FROM       
             (SELECT user_id,
                     addWeeks(arrayJoin(weeks_visited), 1) AS this_week,
                     arrayJoin(weeks_visited) AS previous_week,
                     'gone' AS status,
                     weeks_visited
                FROM       
             (SELECT user_id,
                     groupUniqArray(toMonday(toDate(time))) AS weeks_visited
                FROM simulator_20250220.feed_actions
            GROUP BY user_id) 
               WHERE has(weeks_visited, this_week) = 0 AND this_week <= today())
            GROUP BY this_week, status

               UNION ALL

              SELECT this_week,
                     status,
                     toInt64(uniq(user_id)) AS count_users
                FROM       
             (SELECT user_id,
                     arrayJoin(weeks_visited) AS this_week,
                     addWeeks(arrayJoin(weeks_visited), -1) AS previous_week,
                     if(has(weeks_visited, addWeeks(arrayJoin(weeks_visited), -1)) = 0, 'new', 'retained') AS status,
                     weeks_visited
                FROM       
             (SELECT user_id,
                     groupUniqArray(toMonday(toDate(time))) AS weeks_visited
                FROM simulator_20250220.feed_actions
               GROUP BY user_id))
               GROUP BY this_week, status) t
               ORDER BY this_week DESC
               LIMIT 3
                """

        weekly_metrics = ph.read_clickhouse(query_1, connection=connection)

        weekly_metrics['this_week'] = pd.to_datetime(weekly_metrics['this_week']) 
        formatted_date = weekly_metrics.this_week[0].strftime('%d.%m.%Y') 
        chat_id = -938659451
        msg = (
            f"За текущую неделю с {formatted_date} ключевые метрики всего приложения имеют следующие показатели:\n"
            f"WAU = {weekly_metrics.loc[[1, 2], 'count_users'].sum()}\n"
            f"gone_users = {weekly_metrics.loc[[0][0], 'count_users']}\n"
            f"retained_users = {weekly_metrics.loc[[1][0], 'count_users']}\n"
            f"new_users = {weekly_metrics.loc[[2][0], 'count_users']}"
            )


        bot.sendMessage(chat_id=chat_id, text=msg)

        query_2 = """
                SELECT this_week,
                       status,
                       toInt64(-uniq(user_id)) AS count_users
                  FROM       
               (SELECT user_id,
                       addWeeks(arrayJoin(weeks_visited), 1) AS this_week,
                       arrayJoin(weeks_visited) AS previous_week,
                       'gone' AS status,
                       weeks_visited
                  FROM       
               (SELECT user_id,
                       groupUniqArray(toMonday(toDate(time))) AS weeks_visited
                  FROM simulator_20250220.feed_actions
                 GROUP BY user_id) 
                 WHERE has(weeks_visited, this_week) = 0 AND this_week <= today())
                 GROUP BY this_week, status
 
                 UNION ALL

                SELECT this_week,
                       status,
                       toInt64(uniq(user_id)) AS count_users
                  FROM       
               (SELECT user_id,
                       arrayJoin(weeks_visited) AS this_week,
                       addWeeks(arrayJoin(weeks_visited), -1) AS previous_week,
                       if(has(weeks_visited, addWeeks(arrayJoin(weeks_visited), -1)) = 0, 'new', 'retained') AS status,
                       weeks_visited
                  FROM       
               (SELECT user_id,
                       groupUniqArray(toMonday(toDate(time))) AS weeks_visited
                  FROM simulator_20250220.feed_actions
                 GROUP BY user_id))
                 GROUP BY this_week, status
                   """

        weekly_activity = ph.read_clickhouse(query_2, connection=connection)
        
        # Преобразование 'this_week' в формат datetime
        weekly_activity['this_week'] = pd.to_datetime(weekly_activity['this_week'])

        # Создание сводной таблицы (pivot)
        pivot_data = weekly_activity.pivot_table(index='this_week', columns='status', values='count_users', aggfunc='sum', fill_value=0)

        # Подготовка данных для построения графика
        weeks = pivot_data.index.strftime('%d.%m.%Y')
        gone = pivot_data['gone']
        retained = pivot_data['retained']
        new = pivot_data['new']

        # Построение графика
        plt.figure(figsize=(10, 6))
        plt.axhline(0, color='black', linewidth=0.8, linestyle='--')  # Горизонтальная пунктирная линия для нуля

        # Отображение положительных значений
        plt.bar(weeks, gone.where(gone < 0, 0), label='Gone (Negative)', color='purple', bottom=gone.where(gone > 0, 0))
        plt.bar(weeks, retained.where(retained > 0, 0), label='Retained', color='yellow', bottom=gone.where(gone > 0, 0)) 
        plt.bar(weeks, new.where(new > 0, 0), label='New', color='cyan', bottom=gone.where(gone > 0, 0) + retained.where(retained > 0, 0))

        # Настройка графика
        plt.xlabel('Weeks')
        plt.ylabel('count users')
        plt.title('Weekly activity')
        plt.legend(title="status")
        plt.xticks(rotation=45)
        plt.grid(True, axis='y')
        plt.tight_layout()

        # Сохранение и отображение графика
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'Weekly_activity.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
        query_3 = """
              SELECT toString(start_day) start_day,
                     toString(day) day,
                     count(user_id) AS users
                FROM
             (SELECT *
                FROM
             (SELECT user_id,
                     min(toDate(time)) AS start_day
                FROM simulator_20250220.feed_actions
               GROUP BY user_id
              HAVING start_day >= today() - 15) t1
                JOIN
             (SELECT DISTINCT user_id,
                     toDate(time) AS day
                FROM simulator_20250220.feed_actions) t2 USING user_id)
               GROUP BY start_day, day
                """
        retention_by_cohort = ph.read_clickhouse(query_3, connection=connection)
        
        # Преобразование строкового формата в datetime
        retention_by_cohort['start_day'] = pd.to_datetime(retention_by_cohort['start_day'])
        retention_by_cohort['day'] = pd.to_datetime(retention_by_cohort['day'])
        retention_by_cohort['users'] = retention_by_cohort['users'].astype('int64')

        # Извлечение только даты
        retention_by_cohort['start_day'] = retention_by_cohort['start_day'].dt.date
        retention_by_cohort['day'] = retention_by_cohort['day'].dt.date

        # Создание сводной таблицы (pivot)
        pivot_table = retention_by_cohort.pivot(index='start_day', columns='day', values='users')
        pivot_table = pivot_table.fillna(0).astype('int')

        # Построение тепловой карты
        plt.figure(figsize=(10, 8))
        sns.heatmap(pivot_table, annot=True, fmt="d", cmap="YlGnBu", cbar=True)  # Используем fmt="d" для целых чисел

        # Настройка заголовков и осей
        plt.title('Retention rate by cohort', fontsize=16)
        plt.xlabel('day', fontsize=12)
        plt.ylabel('start day', fontsize=12)
        plt.xticks(rotation=45)
        plt.yticks(rotation=0)
        plt.tight_layout()

        # Сохранение и отображение графика
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'Retention_rate.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
        query_4 = """   

        SELECT post_id,
               countIf(action = 'like') as likes,
               countIf(action = 'view') as views
          FROM simulator_20250220.feed_actions t1
          JOIN simulator_20250220.message_actions t2
         USING (user_id)
         WHERE toDate(time) >= toStartOfWeek(today(), 1) 
           AND toDate(time) <= today()
         GROUP BY post_id
         ORDER BY likes DESC, views DESC
         LIMIT 100

        """
        
        top_posts = ph.read_clickhouse(query_4, connection=connection)
        file_object = io.StringIO()
        top_posts.to_csv(file_object)
        file_object.name = 'top100_posts_for_current_week.csv'
        file_object.seek(0)
        bot.sendDocument(chat_id=chat_id, document=file_object)
        
        query_5 = """   

        SELECT user_id,
               count(receiver_id) AS messages_received
          FROM simulator_20250220.message_actions
         WHERE toDate(time) >= toStartOfWeek(today(), 1) AND toDate(time) <= today()
         GROUP BY user_id
         ORDER BY messages_received DESC
         LIMIT 100

        """
        
        top_recipients = ph.read_clickhouse(query_5, connection=connection)
        file_object = io.StringIO()
        top_posts.to_csv(file_object)
        file_object.name = 'top100_recipients.csv'
        file_object.seek(0)
        bot.sendDocument(chat_id=chat_id, document=file_object)
        
        query_6 = """   

        SELECT receiver_id,
               count(user_id) AS messages_sent
          FROM simulator_20250220.message_actions
         WHERE toDate(time) >= toStartOfWeek(today(), 1) AND toDate(time) <= today()
         GROUP BY receiver_id
         ORDER BY messages_sent DESC
         LIMIT 100

        """
        
        top_senders = ph.read_clickhouse(query_6, connection=connection)
        file_object = io.StringIO()
        top_posts.to_csv(file_object)
        file_object.name = 'top100_senders.csv'
        file_object.seek(0)
        bot.sendDocument(chat_id=chat_id, document=file_object)
        
    full_report_sergey_savchits(chat_id)
        
dag_full_peport_s_savchits_v4 = dag_full_peport_s_savchits_v4()