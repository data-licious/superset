import json
import logging
from copy import deepcopy
from datetime import datetime, timedelta
from six import string_types

import requests
import sqlalchemy as sa
from sqlalchemy import (
    Column, Integer, String, ForeignKey, Text, Boolean,
    DateTime,
)
from sqlalchemy.orm import backref, relationship
from dateutil.parser import parse as dparse

from flask import Markup, escape
from flask_appbuilder.models.decorators import renders
from flask_appbuilder import Model

from flask_babel import lazy_gettext as _

from superset import conf, db, import_util, utils, sm, get_session
from superset.utils import (
    flasher, MetricPermException, DimSelector, DTTM_ALIAS
)
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

    dimension_spec_json = Column(Text)

    export_fields = (
        'column_name', 'is_active', 'type', 'groupby',
        'count_distinct', 'sum', 'avg', 'max', 'min', 'filterable',
        'description', 'dimension_spec_json'
    )

    def __repr__(self):
        return self.column_name

    @property
    def dimension_spec(self):
        if self.dimension_spec_json:
            return json.loads(self.dimension_spec_json)

    def generate_metrics(self):
        """Generate metrics based on the column metadata"""
        M = BigQueryMetric  # noqa
        metrics = []
        metrics.append(BigQueryMetric(
            metric_name='count',
            verbose_name='COUNT(*)',
            metric_type='count',
            json=json.dumps({'type': 'count', 'name': 'count'})
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
                json=json.dumps({
                    'type': mt, 'name': name, 'fieldName': self.column_name})
            ))

        if self.avg and self.is_num:
            mt = corrected_type.lower() + 'Avg'
            name = 'avg__' + self.column_name
            metrics.append(BigQueryMetric(
                metric_name=name,
                metric_type='avg',
                verbose_name='AVG({})'.format(self.column_name),
                json=json.dumps({
                    'type': mt, 'name': name, 'fieldName': self.column_name})
            ))

        if self.min and self.is_num:
            mt = corrected_type.lower() + 'Min'
            name = 'min__' + self.column_name
            metrics.append(BigQueryMetric(
                metric_name=name,
                metric_type='min',
                verbose_name='MIN({})'.format(self.column_name),
                json=json.dumps({
                    'type': mt, 'name': name, 'fieldName': self.column_name})
            ))
        if self.max and self.is_num:
            mt = corrected_type.lower() + 'Max'
            name = 'max__' + self.column_name
            metrics.append(BigQueryMetric(
                metric_name=name,
                metric_type='max',
                verbose_name='MAX({})'.format(self.column_name),
                json=json.dumps({
                    'type': mt, 'name': name, 'fieldName': self.column_name})
            ))
        if self.count_distinct:
            name = 'count_distinct__' + self.column_name
            if self.type == 'hyperUnique' or self.type == 'thetaSketch':
                metrics.append(BigQueryMetric(
                    metric_name=name,
                    verbose_name='COUNT(DISTINCT {})'.format(self.column_name),
                    metric_type=self.type,
                    json=json.dumps({
                        'type': self.type,
                        'name': name,
                        'fieldName': self.column_name
                    })
                ))
            else:
                mt = 'count_distinct'
                metrics.append(BigQueryMetric(
                    metric_name=name,
                    verbose_name='COUNT(DISTINCT {})'.format(self.column_name),
                    metric_type='count_distinct',
                    json=json.dumps({
                        'type': 'cardinality',
                        'name': name,
                        'fieldNames': [self.column_name]})
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
    json = Column(Text)

    export_fields = (
        'metric_name', 'verbose_name', 'metric_type',
        'json', 'description', 'is_restricted', 'd3format'
    )

    @property
    def json_obj(self):
        try:
            obj = json.loads(self.json)
        except Exception:
            obj = {}
        return obj

    @property
    def perm(self):
        return (
            "{parent_name}.[{obj.metric_name}](id:{obj.id})"
        ).format(obj=self,
                 parent_name=self.table.full_name
                 ) if self.table else None

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


sa.event.listen(BigQueryTable, 'after_insert', set_perm)
sa.event.listen(BigQueryTable, 'after_update', set_perm)
