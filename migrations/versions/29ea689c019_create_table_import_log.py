"""create table import_log

Revision ID: 29ea689c019
Revises: 15a9fac5360
Create Date: 2015-10-09 12:05:23.202531

"""

# revision identifiers, used by Alembic.
revision = '29ea689c019'
down_revision = '15a9fac5360'
branch_labels = None
depends_on = None

from alembic import op, environment
import sqlalchemy as sa


def upgrade():
    op.create_table(
        'import_log',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('import_format_id', sa.Integer, nullable=False),
        sa.Column('creation_date', sa.Text, nullable=False),
        sa.Column('import_status', sa.Enum('new', 'inprogress','complete','failed', name='import_status'), nullable=False, default='new'),
        sa.Column('num_rows_processed', sa.Integer, nullable=True, default=0),
        sa.Column('file_checksum', sa.Text, nullable=True),
    )


def downgrade():
    op.drop_table('import_log')

    # sqlalchemy.Enum(native_enum=True) creates a separate DB entity for the enum.
    # since there are no built in sqlalchemy methods for dropping enum entities,
    # we'll do so via direct DDL
    dropEnum = "drop type import_status";
    environment.EnvironmentContext.execute(op, sql=dropEnum)
