from nmdc_schema import nmdc
from nmdc_runtime.site.translation.translator import Translator

class NeonDataTranslator(Translator):
    def __init__(self, data: dict, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.data = data

    def _translate_biosample(self) -> nmdc.Biosample:
        pass

    def _translate_omics_processing(self) -> nmdc.OmicsProcessing:
        pass

    def get_database(self) -> nmdc.Database:
        database = nmdc.Database()

        return database