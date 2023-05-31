from nmdc_runtime.site.translation.translator import Translator

class NeonDataTranslator(Translator):
    def __init__(self, product_code: str, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.product_code = product_code
