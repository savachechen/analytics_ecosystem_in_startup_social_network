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

default_args = {
    'owner': 's-savchits',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 4, 4),
}   

schedule_interval = '*/15 * * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_alert_sistem_s_savchits_v5():
    
    @task()
    def get_alert_metrics():
        # Параметры соединения
        connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database': 'simulator_20250220',
                      'user': 'student',
                      'password': 'dpo_python_2020'}

        # SQL-запрос
        query = """
        WITH 
        feed_agg AS (
            SELECT
                toStartOfFifteenMinutes(time) as ts,
                toDate(ts) as date,
                formatDateTime(ts, '%R') as hm,
                toDayOfWeek(ts) as day_of_week,
                uniqExact(user_id) as active_users_feed,
                countIf(action = 'view') as views,
                countIf(action = 'like') as likes,
                countIf(action = 'like') / countIf(action = 'view') as ctr
            FROM simulator_20250220.feed_actions
            WHERE toDayOfWeek(ts) = toDayOfWeek(now()) AND date >= today() - 31 AND ts < toStartOfFifteenMinutes(now())
            GROUP BY ts, date, hm 
            ORDER BY ts
        ),
        message_agg AS (
            SELECT
                toStartOfFifteenMinutes(time) as ts,
                toDate(ts) as date,
                formatDateTime(ts, '%R') as hm,
                toDayOfWeek(ts) as day_of_week,
                uniqExact(user_id) as active_users_messages,
                count(user_id) as sent_messages
            FROM simulator_20250220.message_actions
            WHERE toDayOfWeek(ts) = toDayOfWeek(now()) AND date >= today() - 31 AND ts < toStartOfFifteenMinutes(now())
            GROUP BY ts, date, hm 
            ORDER BY ts
        )
        SELECT 
            f.ts,
            f.date,
            f.hm,
            f.day_of_week,
            f.active_users_feed,
            f.views,
            f.likes,
            f.ctr,
            m.active_users_messages,
            m.sent_messages
        FROM feed_agg f
        JOIN message_agg m
        USING (ts) 
         """
        return ph.read_clickhouse(query, connection=connection)
    
    # Поиск аномалии
    def check_anomaly(df, metric, threshold=0.3):
        currently_metrics = df.tail(1)
        current_value = currently_metrics[metric].iloc[0]
        current_time = currently_metrics['hm'].iloc[0]

        data_for_verification = df.query("hm == @current_time").iloc[:-1]
        avg_value_last_month = data_for_verification[metric].mean().round(2)

        # Вычисляем отклонение
        if current_value <= avg_value_last_month:
            diff = abs(current_value / avg_value_last_month - 1)
        else:
            diff = abs(avg_value_last_month / current_value - 1)
            
        if diff > threshold:
            is_alert = 1
        else:
            is_alert = 0

        #Проверяем больше ли отклонение метрики заданного порога threshold
        return is_alert, current_value, diff, current_time

    @task()
    def run_alerts(alert_metrics, threshold=0.15, chat_id=-969316925):
        bot = telegram.Bot(token='7732518381:AAG5WXiU9fYkBq_4flYANxB03bv76yUtPMk')

        metrics = ['active_users_feed', 'active_users_messages', 'views', 'likes', 'ctr', 'sent_messages']
        for metric in metrics:
            is_alert, current_value, diff, current_time = check_anomaly(alert_metrics, metric, threshold)

            if is_alert:
                msg = f'''❗️⚠️Аномалия⚠️❗️\nМетрика {metric}:\nтекущее значение = {current_value:.2f}\nотклонение от среднего в текущий день недели в {current_time} = {diff:.2%}'''

                # Строим график
                sns.set(rc={'figure.figsize': (16, 10)})
                plt.tight_layout()

                # Настройка видимости подписей осей
                alert_metrics['color'] = alert_metrics['date'].apply(lambda x: "blue" if x == alert_metrics['date'].iloc[-1] else "gray")
                ax = sns.lineplot(data=alert_metrics.sort_values(by=['date', 'hm']),
                                  x="hm", y=metric, hue="color", legend=False)
                ax.set_title(f'{metric}')
                
                # Настройка оси X
                ax.set_xticks(alert_metrics['hm'][::2])  # Отображение через одну метку (каждую вторую)
                ax.set_xticklabels(alert_metrics['hm'][::2], rotation=90)  # Устанавливаем метки вертикально

                ax.set(ylim=(0, None))
                plot_object = io.BytesIO()
                ax.figure.savefig(plot_object)
                plot_object.seek(0)
                plot_object.name = f'{metric}.png'
                plt.close()

                # Отправляем алерт
                bot.sendMessage(chat_id=chat_id, text=msg)
                bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    alert_metrics = get_alert_metrics()
    run_alerts(alert_metrics)

dag_alert_sistem_s_savchits_v5 = dag_alert_sistem_s_savchits_v5()
