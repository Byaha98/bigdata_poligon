"""
DAG: Запуск dbt моделей stage_orders_moscow и events_sessions.
"""

from __future__ import annotations

import logging
import subprocess
from datetime import datetime

from airflow.decorators import dag, task

logger = logging.getLogger(__name__)

DBT_PROJECT_DIR = "/opt/dbt/poligon"


@dag(
    dag_id="dbt_jinja_guide_dag",
    description="dbt run для моделей stage_orders_moscow и events_sessions",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dbt", "dbt_jinja_guide"],
    max_active_runs=1,
)
def dbt_jinja_guide_dag():
    @task()
    def run_dbt_model_stage_orders_moscow():
        """Запускает dbt run для модели stage_orders_moscow."""
        cmd = f"cd {DBT_PROJECT_DIR} && dbt run --select stage_orders_moscow"
        logger.info("Команда: %s", cmd)
        subprocess.run(cmd, shell=True, check=True)
        logger.info("dbt run для stage_orders_moscow выполнен успешно")

    @task()
    def run_dbt_model_events_sessions():
        """Запускает dbt run для модели events_sessions."""
        cmd = f"cd {DBT_PROJECT_DIR} && dbt run --select events_sessions"
        logger.info("Команда: %s", cmd)
        subprocess.run(cmd, shell=True, check=True)
        logger.info("dbt run для events_sessions выполнен успешно")

    run_dbt_model_stage_orders_moscow() >> run_dbt_model_events_sessions()


dag_instance = dbt_jinja_guide_dag()
