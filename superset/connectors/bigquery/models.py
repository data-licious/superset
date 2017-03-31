import json
import logging
from copy import deepcopy
from datetime import datetime, timedelta

import pandas as pd
import sqlalchemy
import sqlparse
from six import string_types

import requests
import sqlalchemy as sa
from sqlalchemy import (
    Column, Integer, String, ForeignKey, Text, Boolean,
    DateTime,
    literal_column, column, table, desc, select)
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import backref, relationship
from dateutil.parser import parse as dparse

from flask import Markup, escape
from flask_appbuilder.models.decorators import renders
from flask_appbuilder import Model

from flask_babel import lazy_gettext as _
from sqlalchemy.pool import NullPool
from sqlalchemy.sql.elements import ColumnClause, and_
from sqlalchemy.sql.selectable import TextAsFrom

from superset.jinja_context import BaseTemplateProcessor

from superset import conf, db, import_util, utils, sm, get_session, app, sql_lab, dataframe
from superset.utils import (
    flasher, MetricPermException, DimSelector, DTTM_ALIAS,
    QueryStatus)
from superset.connectors.base import BaseDatasource, BaseColumn, BaseMetric
from superset.models.helpers import AuditMixinNullable, QueryResult, set_perm

from google.cloud import bigquery


class BigQueryColumn(Model, BaseColumn):
    """ORM model for storing BigQuery Dataset column metadata"""

    __tablename__ = 'bigquery_column'

    table_id = Column(Integer(), ForeignKey('bigquery_table.id'))

    table = relationship(
        'BigQueryTable',
        backref=backref('columns', cascade='all, delete-orphan'),
        enable_typechecks=False, foreign_keys=[table_id])

    expression = Column(Text, default='')

    export_fields = (
        'column_name', 'is_active', 'type', 'expression', 'groupby',
        'count_distinct', 'sum', 'avg', 'max', 'min', 'filterable',
        'description'
    )

    def __repr__(self):
        return self.column_name

    def generate_metrics(self):
        """Generate metrics based on the column metadata"""
        M = BigQueryMetric  # noqa
        metrics = []
        metrics.append(BigQueryMetric(
            metric_name='count',
            verbose_name='COUNT(*)',
            metric_type='count',
            expression='COUNT(*)',
        ))

        # @TODO Verify this block
        if self.type in ('DOUBLE', 'FLOAT'):
            corrected_type = 'DOUBLE'
        else:
            corrected_type = self.type

        if self.sum and self.is_num:
            mt = corrected_type.lower() + 'Sum'
            name = 'sum__' + self.column_name
            metrics.append(BigQueryMetric(
                metric_name=name,
                metric_type='sum',
                verbose_name='SUM({})'.format(self.column_name),
                expression="SUM('{}')".format(self.column_name)
            ))

        if self.avg and self.is_num:
            mt = corrected_type.lower() + 'Avg'
            name = 'avg__' + self.column_name
            metrics.append(BigQueryMetric(
                metric_name=name,
                metric_type='avg',
                verbose_name='AVG({})'.format(self.column_name),
                expression='AVG({})'.format(self.column_name),
            ))

        if self.min and self.is_num:
            mt = corrected_type.lower() + 'Min'
            name = 'min__' + self.column_name
            metrics.append(BigQueryMetric(
                metric_name=name,
                metric_type='min',
                verbose_name='MIN({})'.format(self.column_name),
                expression='MIN({})'.format(self.column_name),
            ))
        if self.max and self.is_num:
            mt = corrected_type.lower() + 'Max'
            name = 'max__' + self.column_name
            metrics.append(BigQueryMetric(
                metric_name=name,
                metric_type='max',
                verbose_name='MAX({})'.format(self.column_name),
                expression='MAX({})'.format(self.column_name),
            ))
        if self.count_distinct:
            name = 'count_distinct__' + self.column_name
            if self.type == 'hyperUnique' or self.type == 'thetaSketch':
                metrics.append(BigQueryMetric(
                    metric_name=name,
                    verbose_name='COUNT(DISTINCT {})'.format(self.column_name),
                    metric_type=self.type,
                    expression='COUNT(DISTINCT {})'.format(self.column_name),
                ))
            else:
                mt = 'count_distinct'
                metrics.append(BigQueryMetric(
                    metric_name=name,
                    verbose_name='COUNT(DISTINCT {})'.format(self.column_name),
                    metric_type='count_distinct',
                    expression='COUNT(DISTINCT {})'.format(self.column_name),
                ))
        session = get_session()
        new_metrics = []
        for metric in metrics:
            m = (
                session.query(M)
                    .filter(M.metric_name == metric.metric_name)
                    .filter(M.table_id == self.table_id)
                    .first()
            )
            metric.table_id = self.table_id
            if not m:
                new_metrics.append(metric)
                session.add(metric)
                session.flush()

    @property
    def sqla_col(self):
        name = self.column_name
        if not self.expression:
            col = column(self.column_name).label(name)
        else:
            col = literal_column(self.expression).label(name)
        return col

    @classmethod
    def import_obj(cls, i_column):
        def lookup_obj(lookup_column):
            # @TODO need changes
            return db.session.query(BigQueryColumn).filter(
                BigQueryColumn.table_id == lookup_column.table_id,
                BigQueryColumn.column_name == lookup_column.column_name).first()

        return import_util.import_simple_obj(db.session, i_column, lookup_obj)


class BigQueryMetric(Model, BaseMetric):
    """ORM object referencing BigQuery metrics for a dataset"""

    __tablename__ = 'bigquery_metric'

    table_id = Column(Integer, ForeignKey('bigquery_table.id'))

    table = relationship(
        'BigQueryTable',
        backref=backref('metrics', cascade='all, delete-orphan'),
        enable_typechecks=False, foreign_keys=[table_id])
    expression = Column(Text)

    export_fields = (
        'metric_name', 'verbose_name', 'metric_type',
        'expression', 'description', 'is_restricted', 'd3format'
    )

    @property
    def perm(self):
        return (
            "{parent_name}.[{obj.metric_name}](id:{obj.id})"
        ).format(obj=self,
                 parent_name=self.table.full_name
                 ) if self.table else None

    @property
    def sqla_col(self):
        name = self.metric_name
        return literal_column(self.expression).label(name)

    @classmethod
    def import_obj(cls, i_metric):
        def lookup_obj(lookup_metric):
            return db.session.query(BigQueryMetric).filter(
                BigQueryMetric.table_id == lookup_metric.table_id,
                BigQueryMetric.metric_name == lookup_metric.metric_name).first()

        return import_util.import_simple_obj(db.session, i_metric, lookup_obj)


class BigQueryTable(Model, BaseDatasource):
    """ORM object referencing BigQuery Dataset"""

    type = "bigquery"
    query_language = "sql"
    metric_class = BigQueryMetric
    column_class = BigQueryColumn

    baselink = "bigquerytablemodelview"

    __tablename__ = 'bigquery_table'
    id = Column(Integer, primary_key=True)
    project_id = Column(String(255), unique=False)
    dataset_name = Column(String(255), unique=False)
    table_name = Column(String(255), unique=False)
    is_featured = Column(Boolean, default=False)
    filter_select_enabled = Column(Boolean, default=False)
    description = Column(Text)
    user_id = Column(Integer, ForeignKey('ab_user.id'))
    owner = relationship(
        'User',
        backref=backref('bigquery_table', cascade='all, delete-orphan'),
        foreign_keys=[user_id])
    offset = Column(Integer, default=0)
    cache_timeout = Column(Integer)
    params = Column(String(1000))
    perm = Column(String(1000))
    metadata_last_refreshed = Column(DateTime)

    export_fields = (
        'project_id', 'dataset_name', 'table_name', 'description', 'is_featured', 'offset', 'cache_timeout', 'params'
    )

    @property
    def metrics_combo(self):
        return sorted(
            [(m.metric_name, m.verbose_name) for m in self.metrics],
            key=lambda x: x[1])

    @property
    def database(self):
        return self

    @property
    def num_cols(self):
        return [c.column_name for c in self.columns if c.is_num]

    @property
    def name(self):
        return utils.get_bigquery_table_full_name(self.project_id, self.dataset_name, self.table_name)

    @property
    def schema(self):
        return self.name

    @property
    def datasource_name(self):
        return self.name

    @property
    def schema_perm(self):
        """Returns schema permission if present, cluster one otherwise."""
        return utils.get_schema_perm(self.cluster, self.schema)

    def get_perm(self):
        return self.name

    @property
    def link(self):
        name = escape(self.name)
        return Markup('<a href="{self.url}">{name}</a>').format(**locals())

    @property
    def full_name(self):
        return self.name

    @property
    def time_column_grains(self):
        return {
            "time_columns": [
                'all', '5 seconds', '30 seconds', '1 minute',
                '5 minutes', '1 hour', '6 hour', '1 day', '7 days',
                'week', 'week_starting_sunday', 'week_ending_saturday',
                'month',
            ],
            "time_grains": ['now']
        }

    def __repr__(self):
        return self.name

    @renders('table_name')
    def datasource_link(self):
        url = "/superset/explore/{obj.type}/{obj.id}/".format(obj=self)
        name = escape(self.name)
        return Markup('<a href="{url}">{name}</a>'.format(**locals()))

    def refresh_metadata(self):
        """
            Refresh metadata of BigQuery Table
        """

        logging.info("Syncing BigQuery table metadata {}".format(self.table_name))
        session = get_session()
        flasher("Refreshing table {}".format(self.table_name), "info")
        session.flush()

        client = bigquery.Client(self.project_id)
        dataset = client.dataset(self.dataset_name)
        bigquery_table = dataset.table(self.table_name)
        bigquery_table.reload()

        for column in bigquery_table.schema:
            bigquery_column = (
                session
                    .query(BigQueryColumn)
                    .filter_by(table_id=self.id, column_name=column.name)
                    .first()
            )
            if not bigquery_column:
                bigquery_column = BigQueryColumn(table_id=self.id, column_name=column.name, type=column.field_type)
                bigquery_column.verbose_name = bigquery_column.column_name
                bigquery_column.description = column.description
                bigquery_column.expression = bigquery_column.column_name

                session.add(bigquery_column)

            if column.field_type == "STRING":
                bigquery_column.groupby = True
                bigquery_column.filterable = True
            elif column.field_type == "INTEGER":
                bigquery_column.sum = True
                bigquery_column.min = True
                bigquery_column.max = True
                bigquery_column.avg = True
            elif column.field_type == "RECORD":
                # @TODO implement recursive field check
                pass

            bigquery_column.generate_metrics()
            self.columns.append(bigquery_column)
            session.flush()
        session.commit()

    def query(self, query_obj):
        qry_start_dttm = datetime.now()
        qry = self.get_sqla_query(**query_obj)
        sql = self.get_query_str(**query_obj)
        print(query_obj)
        print(sql)
        status = QueryStatus.SUCCESS
        error_message = None
        df = None
        try:
            client = bigquery.Client()
            # bigquery_table.reload()
            query = client.run_sync_query(sql)
            query.max_results = 100
            query.run()

            column_names = ([column.name for column in query.schema])

            column_names = sql_lab.dedup(column_names)
            df = pd.DataFrame(query.rows, columns=column_names)

        except Exception as e:
            status = QueryStatus.FAILED
            logging.exception(e)
            error_message = str(e)

        return QueryResult(
            status=status,
            df=df,
            duration=datetime.now() - qry_start_dttm,
            query=sql,
            error_message=error_message)

    def get_sqla_query(  # sqla
            self,
            groupby, metrics,
            granularity,
            from_dttm, to_dttm,
            filter=None,  # noqa
            is_timeseries=True,
            timeseries_limit=15,
            timeseries_limit_metric=None,
            row_limit=None,
            inner_from_dttm=None,
            inner_to_dttm=None,
            orderby=None,
            extras=None,
            columns=None):
        """Querying any sqla table from this common interface"""

        template_kwargs = {
            'from_dttm': from_dttm,
            'groupby': groupby,
            'metrics': metrics,
            'row_limit': row_limit,
            'to_dttm': to_dttm,
        }
        template_processor = BaseTemplateProcessor()

        # For backward compatibility
        if granularity not in self.dttm_cols:
            granularity = self.main_dttm_col

        # @TODO remove this later
        granularity = None


        cols = {col.column_name: col for col in self.columns}
        metrics_dict = {m.metric_name: m for m in self.metrics}

        if not granularity and is_timeseries:
            raise Exception(_(
                "Datetime column not provided as part table configuration "
                "and is required by this type of chart"))
        for m in metrics:
            if m not in metrics_dict:
                raise Exception(_("Metric '{}' is not valid".format(m)))
        metrics_exprs = [metrics_dict.get(m).sqla_col for m in metrics]
        timeseries_limit_metric = metrics_dict.get(timeseries_limit_metric)
        timeseries_limit_metric_expr = None
        if timeseries_limit_metric:
            timeseries_limit_metric_expr = \
                timeseries_limit_metric.sqla_col
        if metrics:
            main_metric_expr = metrics_exprs[0]
        else:
            main_metric_expr = literal_column("COUNT(*)").label("ccount")

        select_exprs = []
        groupby_exprs = []

        if groupby:
            select_exprs = []
            inner_select_exprs = []
            inner_groupby_exprs = []
            for s in groupby:
                col = cols[s]
                outer = col.expression
                inner = col.column_name + '__'

                groupby_exprs.append(outer)
                select_exprs.append(outer)
                inner_groupby_exprs.append(inner)
                inner_select_exprs.append(inner)
        elif columns:
            for s in columns:
                select_exprs.append(cols[s].sqla_col)
            metrics_exprs = []

        if granularity:
            @compiles(ColumnClause)
            def visit_column(element, compiler, **kw):
                """Patch for sqlalchemy bug

                TODO: sqlalchemy 1.2 release should be doing this on its own.
                Patch only if the column clause is specific for DateTime
                set and granularity is selected.
                """
                text = compiler.visit_column(element, **kw)
                try:
                    if (
                            element.is_literal and
                            hasattr(element.type, 'python_type') and
                            type(element.type) is DateTime
                    ):
                        text = text.replace('%%', '%')
                except NotImplementedError:
                    # Some elements raise NotImplementedError for python_type
                    pass
                return text

            dttm_col = cols[granularity]
            time_grain = extras.get('time_grain_sqla')

            if is_timeseries:
                timestamp = dttm_col.get_timestamp_expression(time_grain)
                select_exprs += [timestamp]
                groupby_exprs += [timestamp]

            time_filter = dttm_col.get_time_filter(from_dttm, to_dttm)

        select_exprs += metrics_exprs
        qry = sa.select(select_exprs)

        # Supporting arbitrary SQL statements in place of tables
        # if self.sql:
        #     from_sql = template_processor.process_template(self.sql)
        #     tbl = TextAsFrom(sa.text(from_sql), []).alias('expr_qry')
        # else:
        tbl = TextAsFrom(sa.text(self.name), [])
        # tbl = self.get_sqla_table()

        if not columns:
            qry = qry.group_by(*groupby_exprs)

        where_clause_and = []
        having_clause_and = []
        for flt in filter:
            if not all([flt.get(s) for s in ['col', 'op', 'val']]):
                continue
            col = flt['col']
            op = flt['op']
            eq = flt['val']
            col_obj = cols.get(col)
            if col_obj:
                if op in ('in', 'not in'):
                    values = [types.strip("'").strip('"') for types in eq]
                    if col_obj.is_num:
                        values = [utils.js_string_to_num(s) for s in values]
                    cond = col_obj.sqla_col.in_(values)
                    if op == 'not in':
                        cond = ~cond
                    where_clause_and.append(cond)
                elif op == '==':
                    where_clause_and.append(col_obj.sqla_col == eq)
                elif op == '!=':
                    where_clause_and.append(col_obj.sqla_col != eq)
                elif op == '>':
                    where_clause_and.append(col_obj.sqla_col > eq)
                elif op == '<':
                    where_clause_and.append(col_obj.sqla_col < eq)
                elif op == '>=':
                    where_clause_and.append(col_obj.sqla_col >= eq)
                elif op == '<=':
                    where_clause_and.append(col_obj.sqla_col <= eq)
                elif op == 'LIKE':
                    where_clause_and.append(col_obj.sqla_col.like(eq))
        if extras:
            where = extras.get('where')
            if where:
                where = template_processor.process_template(where)
                where_clause_and += [sa.text('({})'.format(where))]
            having = extras.get('having')
            if having:
                having = template_processor.process_template(having)
                having_clause_and += [sa.text('({})'.format(having))]
        if granularity:
            qry = qry.where(and_(*([time_filter] + where_clause_and)))
        else:
            qry = qry.where(and_(*where_clause_and))
        qry = qry.having(and_(*having_clause_and))
        if groupby:
            qry = qry.order_by(desc(main_metric_expr))
        elif orderby:
            for col, ascending in orderby:
                direction = asc if ascending else desc
                qry = qry.order_by(direction(col))

        qry = qry.limit(row_limit)

        if is_timeseries and timeseries_limit and groupby:
            # some sql dialects require for order by expressions
            # to also be in the select clause -- others, e.g. vertica,
            # require a unique inner alias
            inner_main_metric_expr = main_metric_expr.label('mme_inner__')
            inner_select_exprs += [inner_main_metric_expr]
            subq = select(inner_select_exprs)
            subq = subq.select_from(tbl)
            inner_time_filter = dttm_col.get_time_filter(
                inner_from_dttm or from_dttm,
                inner_to_dttm or to_dttm,
            )
            subq = subq.where(and_(*(where_clause_and + [inner_time_filter])))
            subq = subq.group_by(*inner_groupby_exprs)
            ob = inner_main_metric_expr
            if timeseries_limit_metric_expr is not None:
                ob = timeseries_limit_metric_expr
            subq = subq.order_by(desc(ob))
            subq = subq.limit(timeseries_limit)
            on_clause = []
            for i, gb in enumerate(groupby):
                on_clause.append(
                    groupby_exprs[i] == column(gb + '__'))

            tbl = tbl.join(subq.alias(), and_(*on_clause))

        return qry.select_from(tbl)

    def get_sqla_table(self):
        return table(self.name)

    def get_query_str(self, **kwargs):
        engine = sqlalchemy.create_engine(
            app.config.get('SQLALCHEMY_DATABASE_URI'), poolclass=NullPool)
        qry = self.get_sqla_query(**kwargs)
        sql = str(
            qry.compile(
                engine,
                compile_kwargs={"literal_binds": True}
            )
        )
        logging.info(sql)
        sql = sqlparse.format(sql, reindent=True)
        return sql


sa.event.listen(BigQueryTable, 'after_insert', set_perm)
sa.event.listen(BigQueryTable, 'after_update', set_perm)
