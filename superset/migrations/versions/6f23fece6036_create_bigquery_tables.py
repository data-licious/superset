"""Create bigquery_table

Revision ID: 6f23fece6036
Revises: 979c03af3341
Create Date: 2017-03-28 13:49:50.162150

"""

# revision identifiers, used by Alembic.
revision = '6f23fece6036'
down_revision = '979c03af3341'

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.create_table('bigquery_table',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('table_name', sa.String(length=255), nullable=False), #project_id:dataset_name.table_name
    sa.Column('is_featured', sa.Boolean(), nullable=True),
    sa.Column('filter_select_enabled', sa.Boolean(), nullable=True),
    sa.Column('description', sa.Text(), nullable=True),
    sa.Column('user_id', sa.Integer(), sa.ForeignKey("ab_user.id"), nullable=True),
    sa.Column('offset', sa.Integer(), default=0),
    sa.Column('cache_timeout', sa.Integer(), nullable=True),
    sa.Column('params', sa.String(length=1000), nullable=True),
    sa.Column('perm', sa.String(length=1000), nullable=True),
    sa.Column('created_on', sa.DateTime(), nullable=False),
    sa.Column('changed_on', sa.DateTime(), nullable=False),
    sa.Column('created_by_fk', sa.Integer(), sa.ForeignKey("ab_user.id"), nullable=True),
    sa.Column('changed_by_fk', sa.Integer(), sa.ForeignKey("ab_user.id"), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('table_name')
    )

    op.create_table('bigquery_column',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('table_name', sa.String(length=255), sa.ForeignKey("bigquery_table.table_name"), nullable=False),
    sa.Column('column_name', sa.String(length=255), nullable=True),
    sa.Column('verbose_name', sa.String(length=1024), nullable=True),
    sa.Column('is_active', sa.Boolean(), nullable=True),
    sa.Column('type', sa.String(length=32), nullable=True),
    sa.Column('groupby', sa.Boolean(), nullable=True),
    sa.Column('count_distinct', sa.Boolean(), nullable=True),
    sa.Column('sum', sa.Boolean(), nullable=True),
    sa.Column('avg', sa.Boolean(), nullable=True),
    sa.Column('max', sa.Boolean(), nullable=True),
    sa.Column('min', sa.Boolean(), nullable=True),
    sa.Column('filterable', sa.Boolean(), nullable=True),
    sa.Column('description', sa.Text(), nullable=True),
    sa.Column('dimension_spec_json', sa.Text(), nullable=True),
    sa.Column('created_on', sa.DateTime(), nullable=False),
    sa.Column('changed_on', sa.DateTime(), nullable=False),
    sa.Column('created_by_fk', sa.Integer(), sa.ForeignKey("ab_user.id"), nullable=True),
    sa.Column('changed_by_fk', sa.Integer(), sa.ForeignKey("ab_user.id"), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    
    op.create_table('bigquery_metric',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('metric_name', sa.String(length=512), nullable=True),
    sa.Column('verbose_name', sa.String(length=1024), nullable=True),
    sa.Column('metric_type', sa.String(length=32), nullable=True),
    sa.Column('table_name', sa.String(length=255), sa.ForeignKey("bigquery_table.table_name"), nullable=False),
    sa.Column('json', sa.Text(), nullable=True),
    sa.Column('description', sa.Text(), nullable=True),
    sa.Column('d3format', sa.String(length=128), nullable=True),
    sa.Column('is_restricted', sa.Boolean(), nullable=True),
    sa.Column('created_on', sa.DateTime(), nullable=False),
    sa.Column('changed_on', sa.DateTime(), nullable=False),
    sa.Column('created_by_fk', sa.Integer(), sa.ForeignKey("ab_user.id"), nullable=True),
    sa.Column('changed_by_fk', sa.Integer(), sa.ForeignKey("ab_user.id"), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    pass


def downgrade():
    op.drop_table('bigquery_column')
    op.drop_table('bigquery_metric')
    op.drop_table('bigquery_table')
