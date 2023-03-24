from models.base.column_names import ColumnNames
from models.base.table import Table


class Organization(Table):
    organization_id = ColumnNames.organization_id
    supplier_org = ColumnNames.supplier_org
    organization_type = ColumnNames.organization_type
    parent_hierarchy_ids = ColumnNames.parent_hierarchy_ids
    valid_from = ColumnNames.valid_from
    valid_to = ColumnNames.valid_to
