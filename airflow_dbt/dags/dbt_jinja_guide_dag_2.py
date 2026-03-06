"""
DAG: Запуск dbt модели events_sessions+ (jinja pipeline).
"""

from __future__ import annotations

import logging
import subprocess
from datetime import datetime

from airflow.decorators import dag, task

logger = logging.getLogger(__name__)

DBT_PROJECT_DIR = "/opt/dbt/poligon"


@dag(
    dag_id="dbt_jinja_guide_dag_2",
    description="dbt run --select events_sessions+",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dbt", "dbt_jinja_guide"],
    max_active_runs=1,
)
def dbt_jinja_guide_dag_2():
    @task(task_id="jinja_pipeline")
    def jinja_pipeline():
        cmd = f"cd {DBT_PROJECT_DIR} && dbt run --select events_sessions+"
        logger.info("Команда: %s", cmd)
        subprocess.run(cmd, shell=True, check=True)
        logger.info("dbt run успешно выполнен для selection: events_sessions+")

    jinja_pipeline()


dag_instance = dbt_jinja_guide_dag_2()

