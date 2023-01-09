from typing import Dict


def get_pi_dict(nmdc_entry: Dict[str, str]) -> Dict[str, str]:
    """Get dictionary with PI information like name, email, etc.
    :param nmdc_entry: An NMDC record like study, project, etc.
    :return: dictionary with PI information.
    """
    pi_dict = next(
        (contact for contact in nmdc_entry["contacts"] if "PI" in contact["roles"])
    )

    return pi_dict
