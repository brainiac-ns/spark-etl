import concurrent.futures

from base.spark_utils import SparkUtils
from enrichment.buo.buo import Buo
from enrichment.esn.esn import Esn
from enrichment.ifa.ifa import Ifa
from preprocessing.preprocessing import PreprocessingJob


class Pipeline:
    def __init__(self):
        self.spark = SparkUtils().get_or_create_spark_session()

    def __call__(self):
        job1 = PreprocessingJob()
        job1()

        ifa = Ifa(self.spark)
        buo = Buo(self.spark)
        esn = Esn(self.spark)

        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.submit(ifa.__call__)
            executor.submit(buo.__call__)

        esn()


pipeline = Pipeline()
pipeline()
