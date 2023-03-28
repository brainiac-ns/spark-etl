from constants import Constants
from enrichment.buo.buo import Buo
from enrichment.esn.esn import Esn
from enrichment.ifa.ifa import Ifa


class Factory:
    def __init__(self, job_name: str):
        self.job_name = job_name

    def __call__(self):
        if self.job_name == Constants.IFA.value:
            ifa = Ifa()
            ifa()
        elif self.job_name == Constants.BUO.value:
            buo = Buo()
            buo()
        elif self.job_name == Constants.ESN.value:
            esn = Esn()
            esn()
        else:
            raise NotImplementedError
