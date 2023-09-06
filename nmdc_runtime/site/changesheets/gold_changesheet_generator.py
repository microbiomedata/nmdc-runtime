# nmdc_runtime/site/change_sheets/gold_changesheet_generator.py
"""
gold_changesheet_generator.py: Provides classes to generate and validate changesheets for NMDC database objects
with missing or incorrect GOLD-derived metadata.
"""

from nmdc_runtime.site.changesheets.changesheet_generator import BaseChangesheetGenerator, JOSN_OBJECT


class BaseGoldBiosampleChangesheetGenerator(BaseChangesheetGenerator):
    """
    Class for generating changesheets for GOLD-derived metadata
    """

    def __init__(self, name: str, gold_biosamples: list[JOSN_OBJECT]) -> None:
        super().__init__(name)
        self.gold_biosamples = gold_biosamples
        
        self.gold_biosample_names= [biosample["biosampleName"].split()[-1] for biosample in gold_biosamples]
        
    def get_biosample_name(self, biosample: JOSN_OBJECT) -> str:
        """
        Get the name of a biosample
        :param biosample: a biosample object
        :return: str
        """
        return biosample["biosampleName"].split()[-1]


class Issue397ChangesheetGenerator(BaseGoldBiosampleChangesheetGenerator):
    """
    Class for generating changesheet for issue #397
    """
    issue_link = "https://github.com/microbiomedata/issues/issues/397"

    def __init__(self, name: str, gold_biosamples: list[JOSN_OBJECT]) -> None:
        super().__init__(name, gold_biosamples)
