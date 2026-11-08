from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
import logging

def get_simulation_dag(
        dag_id: str,
        table_name: str,
        insert_sql: str,
        update_sql: str,
        delete_sql: str,
        schedule_var_name: str,
        default_schedule_sec: int,
        conn_id: str = "postgres_source_id"
):
    """Фабрика для создания DAG'ов симуляции активности с динамическим расписанием из Variables."""

    try:
        schedule_sec = int(Variable.get(schedule_var_name, default_var=default_schedule_sec))
    except ValueError:
        logging.warning(f"Некорректное значение переменной {schedule_var_name}. Используем default: {default_schedule_sec}")
        schedule_sec = default_schedule_sec

    def execute_random_activity(**context):
        now = datetime.now()
        # Деление времени: остаток от деления текущей секунды на 3
        # 0 -> INSERT, 1 -> UPDATE, 2 -> DELETE
        action_type = now.second % 3

        if action_type == 0:
            sql = insert_sql
            action_name = "INSERT"
        elif action_type == 1:
            sql = update_sql
            action_name = "UPDATE"
        else:
            sql = delete_sql
            action_name = "DELETE"

        logging.info(f"[{table_name}] Выполняется операция: {action_name} (ID: {context['dag_run'].run_id})")

        hook = PostgresHook(postgres_conn_id=conn_id)
        try:
            hook.run(sql)
            logging.info(f"[{table_name}] Операция {action_name} успешно выполнена.")
        except Exception as e:
            logging.warning(f"[{table_name}] Ошибка при выполнении {action_name}: {e}")

    with DAG(
            dag_id=dag_id,
            schedule=timedelta(seconds=schedule_sec),
            start_date=datetime(2023, 1, 1),
            catchup=False,
            tags=["postgres", "simulation", "debezium", table_name],
            max_active_runs=1,
            doc_md=f"Симуляция активности в таблице {table_name}. Интервал задается переменной `{schedule_var_name}` (текущий: {schedule_sec} сек)."
    ) as dag:

        task = PythonOperator(
            task_id=f"simulate_{table_name}_activity",
            python_callable=execute_random_activity,
        )

    return dag


dag_clients = get_simulation_dag(
    dag_id="simulate_clients_activity",
    table_name="clients",
    insert_sql="""
        INSERT INTO clients (full_name, email, phone, birth_date, employer) 
        VALUES ('Mock Client ' || now(), 'mock@example.com', '+79000000000', CURRENT_DATE, 'Mock Employer')
    """,
    update_sql="""
        UPDATE clients 
        SET full_name = 'Updated ' || now() 
        WHERE client_id = (floor(random() * 100000) + 1)::bigint
    """,
    delete_sql="""
        DELETE FROM clients 
        WHERE client_id = (floor(random() * 100000) + 1)::bigint
    """,
    schedule_var_name="sim_clients_schedule_sec",
    default_schedule_sec=8
)


dag_cards = get_simulation_dag(
    dag_id="simulate_cards_activity",
    table_name="cards",
    insert_sql="""
        INSERT INTO cards (client_id, card_number, card_type, card_issue_date, card_expiry_date) 
        SELECT client_id, '4000' || lpad(floor(random() * 1000000000000)::text, 15, '0'), 'Visa', CURRENT_DATE, CURRENT_DATE + INTERVAL '3 years' 
        FROM clients ORDER BY random() LIMIT 1
        ON CONFLICT (card_number) DO NOTHING
    """,
    update_sql="""
        UPDATE cards 
        SET card_type = 'MasterCard' 
        WHERE card_number = (floor(random() * 100000) + 1)::text
    """,
    delete_sql="""
        DELETE FROM cards 
        WHERE card_number = (floor(random() * 100000) + 1)::text
    """,
    schedule_var_name="sim_cards_schedule_sec",
    default_schedule_sec=10
)


dag_transactions = get_simulation_dag(
    dag_id="simulate_transactions_activity",
    table_name="transactions",
    insert_sql="""
        INSERT INTO transactions (card_number, transaction_date, amount, operation_id) 
        SELECT card_number, NOW(), (random() * 10000)::numeric(15,2), (floor(random() * 4) + 1)::bigint 
        FROM cards ORDER BY random() LIMIT 1
    """,
    update_sql="""
        UPDATE transactions 
        SET amount = (random() * 10000)::numeric(15,2) 
        WHERE transaction_id = (floor(random() * 100000) + 1)::bigint
    """,
    delete_sql="""
        DELETE FROM transactions 
        WHERE transaction_id = (floor(random() * 100000) + 1)::bigint
    """,
    schedule_var_name="sim_transactions_schedule_sec",
    default_schedule_sec=12
)


dag_operation = get_simulation_dag(
    dag_id="simulate_operation_activity",
    table_name="operation",
    insert_sql="""
        INSERT INTO operation (operation_name) 
        VALUES ('mock_op_' || floor(random() * 1000000)::text) 
        ON CONFLICT (operation_name) DO NOTHING
    """,
    update_sql="""
        UPDATE operation 
        SET operation_name = operation_name 
        WHERE operation_id = (floor(random() * 100000) + 1)::bigint
    """,
    delete_sql="""
        DELETE FROM operation 
        WHERE operation_id = (floor(random() * 100000) + 1)::bigint
    """,
    schedule_var_name="sim_operation_schedule_sec",
    default_schedule_sec=15
)