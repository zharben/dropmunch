"""create table import_format_column

Revision ID: 5a46afbcc50
Revises: 13954b670c3
Create Date: 2015-10-09 11:56:08.681059

"""

# revision identifiers, used by Alembic.
revision = '5a46afbcc50'
down_revision = '13954b670c3'
branch_labels = None
depends_on = None

from alembic import op, environment
import sqlalchemy as sa


def upgrade():
    op.create_table(
        'import_format_column',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('import_format_id', sa.Integer, nullable=False),
        sa.Column('name', sa.String(15), nullable=False),
        sa.Column('width', sa.Integer, nullable=False),
        sa.Column('datatype', sa.Enum('TEXT', 'BOOLEAN', 'INTEGER', name='format_datatype'), nullable=False),
        sa.Column('nullable', sa.Boolean, nullable=False)
    )

def downgrade():
    op.drop_table('import_format_column')

    # sqlalchemy.Enum(native_enum=True) creates a separate DB entity for the enum.
    # since there are no built in sqlalchemy methods for dropping enum entities,
    # we'll do so via direct DDL
    dropEnum = "drop type format_datatype";
    environment.EnvironmentContext.execute(op, sql=dropEnum)