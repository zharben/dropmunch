"""create table import_format

Revision ID: 13954b670c3
Revises: 
Create Date: 2015-10-09 11:54:30.362452

"""

# revision identifiers, used by Alembic.
revision = '13954b670c3'
down_revision = None
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.create_table(
        'import_format',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('name', sa.String(15), nullable=False)
    )

def downgrade():
    op.drop_table('import_format')