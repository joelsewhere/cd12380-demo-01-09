# Demo: Airflow Branching

This DAG models a driver choosing a route. Toggling the `express_open` and `needs_gas` params changes which path executes each run.

- **`BranchSQLOperator`** evaluates a SQL expression and routes to one of two downstream task lists based on a truthy/falsy result
- **`@task.branch`** routes execution using Python logic, returning the `task_id` of the next task to run
- Branching always skips at least one downstream task — any task that sits below a branch point needs `trigger_rule="none_failed_min_one_success"` to run correctly
- All active paths converge at a single terminal node (`arrive`)