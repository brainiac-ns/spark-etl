from models.base.column_names import ColumnNames
from models.base.table import Table


class IfaMaster(Table):
    ifanr = ColumnNames.ifanr
    loekz = ColumnNames.loekz
    deletion_date = ColumnNames.deletion_date
