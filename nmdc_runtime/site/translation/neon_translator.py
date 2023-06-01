import pandas as pd

from nmdc_schema import nmdc
from nmdc_runtime.site.translation.translator import Translator

class NeonDataTranslator(Translator):
    def __init__(self, data: pd.DataFrame, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    def _translate_biosample(self) -> nmdc.Biosample:
        pass

    def _translate_omics_processing(self) -> nmdc.OmicsProcessing:
        pass

    def get_database(self) -> nmdc.Database:
        database = nmdc.Database()

        # database.biosample_set creation by initializing
        # instances of nmdc.Biosample() will happen here

        return database