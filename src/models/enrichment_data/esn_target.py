from models.base.column_names import ColumnNames
from models.base.table import Table


class EsnTarget(Table):
    logsys = ColumnNames.logsys
    budat = ColumnNames.budat
    esn = ColumnNames.esn
