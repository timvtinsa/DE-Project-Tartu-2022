import os

from typing import TYPE_CHECKING, Iterable, Mapping, Sequence

from airflow.providers.neo4j.operators.neo4j import Neo4jOperator
from airflow.providers.neo4j.hooks.neo4j import Neo4jHook

class Neo4jExtendedOperator(Neo4jOperator):

    def __init__(self, is_sql_file: bool = False, **kwargs) -> None:
        super().__init__(**kwargs)
        self.is_sql_file = is_sql_file

    def execute(self, context) -> None:
        hook = Neo4jHook(conn_id=self.neo4j_conn_id)
        if self.is_sql_file:
            self.log.info("Reading SQL file")
            sql_file = self.sql
            with open(sql_file, 'r') as f:
                for line in f:
                    self.log.info("Executing: %s", line)
                    hook.run(line)
        else:
            self.log.info("Executing: %s", self.sql)
            hook.run(self.sql)
