from datetime import datetime
import logging

import sqlalchemy as sqla

from flask import Markup, flash, redirect
from flask_appbuilder import CompactCRUDMixin, expose
from flask_appbuilder.models.sqla.interface import SQLAInterface

from flask_babel import lazy_gettext as _
from flask_babel import gettext as __

import superset
from superset import db, utils, appbuilder, sm, security
from superset.connectors.connector_registry import ConnectorRegistry
from superset.utils import has_access
from superset.views.base import BaseSupersetView
from superset.views.base import (
    SupersetModelView, validate_json, DeleteMixin, ListWidgetWithCheckboxes,
    DatasourceFilter, get_datasource_exist_error_mgs)

from . import models


class BigQueryColumnInlineView(CompactCRUDMixin, SupersetModelView):  # noqa
    datamodel = SQLAInterface(models.BigQueryColumn)
    edit_columns = [
        'table', 'column_name', 'description', 'dimension_spec_json',
        'groupby', 'count_distinct', 'sum', 'min', 'max']
    add_columns = edit_columns
    list_columns = [
        'table', 'column_name', 'type', 'groupby', 'filterable', 'count_distinct',
        'sum', 'min', 'max']
    can_delete = False
    page_size = 500
    label_columns = {
        'table': _("BigQuery Table"),
        'column_name': _("Column"),
        'type': _("Type"),
        'groupby': _("Groupable"),
        'filterable': _("Filterable"),
        'count_distinct': _("Count Distinct"),
        'sum': _("Sum"),
        'min': _("Min"),
        'max': _("Max"),
    }
    description_columns = {
        'dimension_spec_json': utils.markdown(
            "",
            True),
    }

    def post_update(self, col):
        col.generate_metrics()
        utils.validate_json(col.dimension_spec_json)

    def post_add(self, col):
        self.post_update(col)


appbuilder.add_view_no_menu(BigQueryColumnInlineView)


class BigQueryMetricInlineView(CompactCRUDMixin, SupersetModelView):  # noqa
    datamodel = SQLAInterface(models.BigQueryMetric)
    list_columns = ['table', 'metric_name', 'verbose_name', 'metric_type']
    edit_columns = [
        'table', 'metric_name', 'description', 'verbose_name', 'metric_type', 'json', 'd3format', 'is_restricted']
    add_columns = edit_columns
    page_size = 500
    validators_columns = {
        'json': [validate_json],
    }
    description_columns = {
        'metric_type': utils.markdown(
            "",
            True),
        'is_restricted': _("Whether the access to this metric is restricted "
                           "to certain roles. Only roles with the permission "
                           "'metric access on XXX (the name of this metric)' "
                           "are allowed to access this metric"),
    }
    label_columns = {
        'table': _("BigQuery Table"),
        'metric_name': _("Metric"),
        'description': _("Description"),
        'verbose_name': _("Verbose Name"),
        'metric_type': _("Type"),
        'json': _("JSON"),
    }

    def post_add(self, metric):
        utils.init_metrics_perm(superset, [metric])

    def post_update(self, metric):
        utils.init_metrics_perm(superset, [metric])


appbuilder.add_view_no_menu(BigQueryMetricInlineView)


class BigQueryTableModelView(SupersetModelView, DeleteMixin):  # noqa
    datamodel = SQLAInterface(models.BigQueryTable)
    list_widget = ListWidgetWithCheckboxes
    list_columns = [
        'datasource_link', 'changed_by_', 'changed_on_', 'offset', 'metadata_last_refreshed']
    order_columns = [
        'datasource_link', 'changed_on_', 'offset']
    related_views = [BigQueryColumnInlineView, BigQueryMetricInlineView]
    edit_columns = [
        'project_id', 'dataset_name', 'table_name', 'description', 'is_featured',
        'filter_select_enabled', 'offset', 'cache_timeout']
    add_columns = edit_columns
    show_columns = add_columns + ['perm']
    page_size = 500
    base_order = ('table_name', 'asc')
    description_columns = {
        'offset': _("Timezone offset (in hours) for this table"),
        'description': Markup(
            ""),
    }
    base_filters = [['id', DatasourceFilter, lambda: []]]
    label_columns = {
        'datasource_link': _("BigQuery Table"),
        'description': _("Description"),
        'is_featured': _("Is Featured"),
        'filter_select_enabled': _("Enable Filter Select"),
        'offset': _("Time Offset"),
        'cache_timeout': _("Cache Timeout"),
    }

    def pre_add(self, table):
        number_of_existing_tables = db.session.query(
            sqla.func.count('*')).filter(
            models.BigQueryTable.table_name ==
            table.table_name
        ).scalar()

        # table object is already added to the session
        if number_of_existing_tables > 1:
            raise Exception(get_datasource_exist_error_mgs(
                datasource.full_name))

    def post_add(self, table):
        table.generate_metrics()
        security.merge_perm(sm, 'datasource_access', table.get_perm())
        if table.schema:
            security.merge_perm(sm, 'schema_access', table.schema_perm)

    def post_update(self, table):
        self.post_add(table)


appbuilder.add_view(
    BigQueryTableModelView,
    "BigQuery Tables",
    label=__("BigQuery Tables"),
    category="Sources",
    category_label=__("Sources"),
    icon="fa-table")


class BigQuery(BaseSupersetView):
    """The base views for Superset!"""

    @has_access
    @expose("/refresh_metadata/")
    def refresh_metadata(self):
        """endpoint that refreshes BigQuery Table metadata"""
        session = db.session()
        for table in session.query(ConnectorRegistry.sources['bigquery']).all():
            try:
                table.refresh_metadata()
            except Exception as e:
                flash(
                    "Error while processing table '{}'\n{}".format(
                        table.table_name, utils.error_msg_from_exception(e)),
                    "danger")
                logging.exception(e)
                return redirect('/bigquerytablemodelview/list/')
            table.metadata_last_refreshed = datetime.now()
            flash(
                "Refreshed metadata from %s" % table.name, 'info')
        session.commit()
        return redirect("/bigquerytablemodelview/list/")


appbuilder.add_view_no_menu(BigQuery)

appbuilder.add_link(
    "Refresh BigQuery Table Metadata",
    label=__("Refresh BigQuery Table Metadata"),
    href='/bigquery/refresh_metadata/',
    category='Sources',
    category_label=__("Sources"),
    category_icon='fa-database',
    icon="fa-cog")

appbuilder.add_separator("Sources", )
