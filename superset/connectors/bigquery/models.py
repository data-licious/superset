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

    table_id = Column(
        String(255),
        ForeignKey('bigquery_table.id'))
    # Setting enable_typechecks=False disables polymorphic inheritance.
    table = relationship(
        'BigQueryTable',
        backref=backref('columns', cascade='all, delete-orphan'),
        enable_typechecks=False)
    dimension_spec_json = Column(Text)

    export_fields = (
        'table_id', 'column_name', 'is_active', 'type', 'groupby',
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

        #Verify this block
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
                .filter(M.table_name == self.table_name)
                .first()
            )
            metric.table_name = self.table_name
            if not m:
                new_metrics.append(metric)
                session.add(metric)
                session.flush()

    @classmethod
    def import_obj(cls, i_column):
        def lookup_obj(lookup_column):
            return db.session.query(BigQueryColumn).filter(
                BigQueryColumn.table_name == lookup_column.table_name,
                BigQueryColumn.column_name == lookup_column.column_name).first()

        return import_util.import_simple_obj(db.session, i_column, lookup_obj)
    

class BigQueryMetric(Model, BaseMetric):
    """ORM object referencing BigQuery metrics for a dataset"""

    __tablename__ = 'bigquery_metric'

    table_id = Column(
        String(255),
        ForeignKey('bigquery_table.id'))

    table = relationship(
        'BigQueryTable',
        backref=backref('bigquery_metric', cascade='all, delete-orphan'),
        enable_typechecks=False)
    json = Column(Text)

    export_fields = (
        'metric_name', 'verbose_name', 'metric_type', 'table_id',
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
                 parent_name=self.dataset.full_name
                 ) if self.dataset else None

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
    query_language = "json"
    metric_class = BigQueryMetric
    column_class = BigQueryColumn

    baselink = "bigquerymodelview"

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

    export_fields = (
        'project_id', 'dataset_name', 'table_name', 'description', 'default_endpoint', 'is_featured', 'offset', 'cache_timeout', 'params'
    )

sa.event.listen(BigQueryTable, 'after_insert', set_perm)
sa.event.listen(BigQueryTable, 'after_update', set_perm)