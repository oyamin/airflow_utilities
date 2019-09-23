"""Daily jobs that runs and checks and deletes Stale dags"""

import logging
from datetime import datetime, timedelta
from sqlalchemy import or_
from airflow import settings
from airflow.models import DagModel, DagBag, Base
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import DagNotFound

DEFAULT_ARGS = {
    'owner': 'owner',
    'depends_on_past': True,
    'start_date': datetime(2019, 2, 10),
    'email': ['foo@bar.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'queue': 'slca'
}

DAG = DAG(dag_id='DAILY_HEALTH_CHECK', default_args=DEFAULT_ARGS,
          catchup=False, schedule_interval="@daily")


def delete_dag(session, model, dag_id):
    """Deletes the DAG based on dag_id"""
    dag = session.query(model).filter(model.dag_id == dag_id).first()
    if dag is None:
        raise DagNotFound("Dag id {} not found".format(dag_id))
    for module in Base._decl_class_registry.values():
        if hasattr(module, "dag_id"):
            if module.__name__ == "DagModel" or module.__name__ == "XCom":
                cond = or_(module.dag_id == dag_id, module.dag_id.like(dag_id + ".%"))
                session.query(module).filter(cond).delete(synchronize_session='fetch')
            continue


def clean_deleted_dags():
    """
    Checks the DagBag and DagModel for Dags that have been removed.
    If the DAG exists in database, but file does not, DAG will be deleted.
    """
    session = settings.Session()
    dag_model = DagModel
    dag_bag = DagBag()
    try:
        dagbag_dag_ids = dag_bag.dags.keys()
        database_dags = session.query(dag_model).all()
        database_dag_ids = {d.dag_id for d in database_dags}
        for dag_id in database_dag_ids:
            if dag_id not in dagbag_dag_ids:
                logging.info(f"Deleting %s", dag_id)
                delete_dag(session, dag_model, dag_id)
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


CLEAN_DAGS = PythonOperator(
    task_id="DAILY_CLEAN_DELETED_DAGS",
    python_callable=clean_deleted_dags,
    dag=DAG)

CLEAN_DAGS
