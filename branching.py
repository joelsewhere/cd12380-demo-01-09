# ============================================================
#  DEMONSTRATION: Airflow Branching
# ============================================================
#
#  This DAG models the decision a driver makes before setting
#  off on a journey.
#
#  Two BranchSQLOperators drive the routing logic:
#
#    1. check_express_open
#         Is the express route available?
#         YES → take_express_route → arrive
#         NO  → check_needs_gas
#
#    2. check_needs_gas
#         Does the driver need to stop for gas first?
#         YES → visit_gas_station → take_standard_route → arrive
#         NO  →                     take_standard_route → arrive
#
#  Params let you toggle each condition from the Airflow UI
#  without needing a live database.
# ============================================================


from airflow.sdk import dag, task, Param
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import (
    BranchSQLOperator,
    SQLExecuteQueryOperator,
)


@dag(
    dag_id="branching_demo",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo"],
    params={
        "express_open": Param(True,  type="boolean", description="Is the express route open?"),
        "needs_gas":    Param(False, type="boolean", description="Does the driver need gas?"),
    },
)
def branching_demo():

    # ── Branch 1: Is the express route open? ─────────────────
    # The SQL expression renders the boolean param directly as
    # 1 (true) or 0 (false), giving BranchSQLOperator a
    # reliable truthy/falsy result without needing live data.

    check_express_open = BranchSQLOperator(
        task_id="check_express_open",
        conn_id="roads_db",
        sql="SELECT {{ 1 if params.express_open else 0 }}",
        follow_task_ids_if_true=["take_express_route"],
        follow_task_ids_if_false=["check_needs_gas"],
    )

    take_express_route = SQLExecuteQueryOperator(
        task_id="take_express_route",
        conn_id="roads_db",
        sql="SELECT road_name, avg_speed FROM roads WHERE level = 'express' ORDER BY RANDOM() LIMIT 1",
    )


    # ── Branch 2: Does the driver need gas? ──────────────────
    # Only reached if the express route is closed.
    # @task.branch reads the param directly in Python and
    # returns the task_id of the next task to run.

    @task.branch
    def check_needs_gas(needs_gas: bool):
        if needs_gas:
            return "visit_gas_station"
        return "take_standard_route"

    gas_branch = check_needs_gas("{{ params.needs_gas }}")

    visit_gas_station = EmptyOperator(task_id="visit_gas_station")

    # trigger_rule is required here because take_standard_route
    # has two possible upstream tasks — visit_gas_station (when
    # needs_gas is true) or check_needs_gas (when it is false).
    # One of them will always be skipped, which would block this
    # task under the default all_success rule.
    take_standard_route = SQLExecuteQueryOperator(
        task_id="take_standard_route",
        conn_id="roads_db",
        sql="SELECT road_name, avg_speed FROM roads WHERE level = 'standard' ORDER BY RANDOM() LIMIT 1",
        trigger_rule="none_failed_min_one_success",
    )


    # ── Terminal node ─────────────────────────────────────────
    # All active paths converge here.
    # Same trigger_rule reasoning — one upstream branch is
    # always skipped.

    arrive = EmptyOperator(
        task_id="arrive",
        trigger_rule="none_failed_min_one_success",
    )


    # ── Dependencies ─────────────────────────────────────────

    check_express_open >> take_express_route >> arrive
    check_express_open >> gas_branch
    gas_branch         >> visit_gas_station >> take_standard_route
    gas_branch         >> take_standard_route
    take_standard_route >> arrive


branching_demo()