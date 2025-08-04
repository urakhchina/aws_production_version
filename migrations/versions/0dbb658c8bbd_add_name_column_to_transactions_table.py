"""Add name column to transactions table

Revision ID: 0dbb658c8bbd
Revises: e060ca9297de
Create Date: 2025-08-02 15:34:14.421555

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '0dbb658c8bbd'
down_revision = 'e060ca9297de'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('transactions', sa.Column('name', sa.String(length=100), nullable=True))


def downgrade():
    op.drop_column('transactions', 'name')

