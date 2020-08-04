from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 select_sql="",
                 append=False,
                 primary_key="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_sql = select_sql
        self.append = append
        self.primary_key=primary_key

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if not self.append:
            table_delete_sql = f"""
            DELETE FROM {self.table};
            """
        else:
            table_delete_sql = f"""
            DELETE FROM {self.table}
            USING ({self.select_sql}) upsert_table
            WHERE {self.table}.{self.primary_key} = upsert_table.{self.primary_key}
            """
        
        redshift_hook.run(table_delete_sql)
        
        table_insert_sql = f"""
                INSERT INTO {self.table}
                ({self.select_sql});
                """
        
        redshift_hook.run(table_insert_sql)
  