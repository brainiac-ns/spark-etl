from models.base.column_names import ColumnNames
from models.base.table import Table


class IfaInvoices(Table):
    logsys = ColumnNames.logsys
    budat = ColumnNames.budat
    lifnr = ColumnNames.lifnr
    krenr = ColumnNames.krenr
    filkd = ColumnNames.filkd
    valid_org = ColumnNames.valid_org
