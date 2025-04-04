"""amalgamation_out

Revision ID: fe158a53151f
Revises: 24b59f535ec3
Create Date: 2025-03-14 11:34:01.606149

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'fe158a53151f'
down_revision = '24b59f535ec3'
branch_labels = None
depends_on = None

consent_out_types_enum = postgresql.ENUM('continuation_out', 'amalgamation_out', name='consent_out_types')


def upgrade():
    # add enum values
    consent_out_types_enum.create(op.get_bind(), checkfirst=True)

    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('businesses', sa.Column('amalgamation_out_date', sa.DateTime(timezone=True), nullable=True))
    op.add_column('businesses_version', sa.Column('amalgamation_out_date', sa.DateTime(timezone=True), autoincrement=False, nullable=True))
    op.add_column('consent_continuation_outs', sa.Column('consent_type', consent_out_types_enum, nullable=False, server_default='continuation_out'))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('consent_continuation_outs', 'consent_type')
    consent_out_types_enum.drop(op.get_bind(), checkfirst=True)
    op.drop_column('businesses_version', 'amalgamation_out_date')
    op.drop_column('businesses', 'amalgamation_out_date')
    # ### end Alembic commands ###
