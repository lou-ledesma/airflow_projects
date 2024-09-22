# airflow_projects
A collection of my solo Airflow Projects. Use this README as a guide to understand what files go together.

**Context**: I installed Airflow within a Docker container. If you are attempting to replicate my projects, install Docker and run the following "yml" file included in this repo
--> "docker-compose-LocalExecutor.yml"

**DAG Project List**
1. Store_DAG.py
- Supporting Files:
  - datacleaner.py
  - create_table.sql
  - insert_into_table.sql
  - select_from_table.sql
  - adjust_date.sql
  - raw_store_transactions.csv
 
2.  student_dag.py
- Supporting Files:
  -   create_student_source.sql
  -   insert_into_student_source.sql
  -   create_student_destination.sql
  -   insert_student_destination.sql

3. my_first_dag.py
