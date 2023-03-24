from models.base.column_names import ColumnNames
from models.base.table import Table


class IfaTarget(Table):
    logsys = ColumnNames.logsys
    budat = ColumnNames.budat
    ifacmd_lifnr = ColumnNames.ifacmd_lifnr
    ifacmd_krenr = ColumnNames.ifacmd_krenr
    ifacmd_filkd = ColumnNames.ifacmd_filkd
