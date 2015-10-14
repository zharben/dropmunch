"""add import_format fk to import_format_column

Revision ID: 15a9fac5360
Revises: 5a46afbcc50
Create Date: 2015-10-09 12:01:16.555550

"""

# revision identifiers, used by Alembic.
revision = '15a9fac5360'
down_revision = '5a46afbcc50'
branch_labels = None
depends_on = None

from alembic import op


def upgrade():
    op.create_foreign_key(
            "fk_import_format_ifc", "import_format_column",
            "import_format", ["import_format_id"], ["id"])

def downgrade():
    op.drop_constraint(
            "fk_import_format_ifc", "import_format_column",
            "foreignkey")
