from airflow.decorators import dag, task  # импортируем декораторы TaskFlow API  
from airflow.operators.bash import BashOperator  # для выполнения bash-команд  
from datetime import datetime  # для указания времени

# 🎯 Определяем DAG с помощью декоратора @dag
@dag(
    dag_id='dbt_run_my_first_model',  # уникальный идентификатор DAG
    schedule=None,                    # запуск только вручную, без расписания
    catchup=False,                    # не выполнять старые даты при старте
)
def dbt_dag():
    # 🔧 Оператор для запуска DBT через Bash
    run_dbt_model = BashOperator(
        task_id="run_dbt_model_bash",  # имя задачи
        bash_command="cd /opt/dbt/poligon && echo '== DBT debug ==' && dbt debug && echo '== DBT run ==' && dbt run --select my_first_dbt_model",  # команда для выполнения
        do_xcom_push=True,  # сохраняем вывод команды в XCom (результаты)
    )
            #cd /opt/dbt/poligon              # переходим в рабочую директорию dbt
            #echo "== DBT debug =="         # выводим информацию
            #dbt debug                      # проверяем конфигурацию dbt
            #echo "== DBT run =="           # запускаем модель
            #dbt run --select models/example/my_first_dbt_model.sql - путь к модели, --select позволяет выбрать конкретную модель

# ✨ Создание экземпляра DAG: Airflow находит его по переменной dag
dag = dbt_dag()
