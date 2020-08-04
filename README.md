# Project summary

## Features of this implementation
* The `DummyOperator` implementation of `start_operator` was replaced with a `PostgresOperator`. The latter calls `create_tables.sql`, which was moved to the same folder as `udac_example_dag.py` and hence gets called with every execution of the DAG.
* The `CREATE` statements in `create_tables.sql` were expanded with `IF NOT EXISTS` in order to be callable repeatedly without conflicts. 
* The `LoadDimensionOperator` was implemented with a flag `append=False` and a `primary_key=""` parameter:
	* if `append=False`, the original table is deleted and the entire data will be replaced with the new data
	* if `append=True`, only the rows from the original table with duplicate primary keys will be deleted. This roughly corresponds to an `ON CONFLICT DO UPDATE` call (which is not available in the Postgres version that Redshift is using). 
* The data quality operator is used to check if there are any rows with null value of `artistid` in the `artists` table. This is an exemplary check and many other checks might be performed here as well. 

## Attributions
The following solution was used as guidance:
* **[davidrubinger](https://github.com/davidrubinger/udacity-dend-project-5)**