"""add import_format fk to import_log

Revision ID: 5498dae6d7b
Revises: 29ea689c019
Create Date: 2015-10-09 12:17:08.747703

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '5498dae6d7b'
down_revision = '29ea689c019'
branch_labels = None
depends_on = None



def upgrade():
    op.create_foreign_key(
            "fk_import_format_ifl", "import_log",
            "import_format", ["import_format_id"], ["id"])

def downgrade():
    op.drop_constraint(
            "fk_import_format_ifl", "import_log",
            "foreignkey")