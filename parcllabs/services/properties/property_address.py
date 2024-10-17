from typing import List, Dict

import pandas as pd

from parcllabs.common import VALID_US_STATE_ABBREV
from parcllabs.services.validators import Validators
from parcllabs.services.parcllabs_service import ParclLabsService


class PropertyAddressSearch(ParclLabsService):
    """
    Retrieve parcl_property_ids based on provided addresses.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def retrieve(
        self,
        addresses: List[Dict],
    ):
        """
        Retrieve parcl_property_ids based on provided addresses.

        Args:

            addresses (list): A list of dictionaries containing address information.

        Returns:

            DataFrame: A DataFrame containing the parcl_property_id and address information.
        """

        required_params = ["address", "city", "state_abbreviation", "zip_code"]
        for address in addresses:
            param = Validators.validate_field_exists(address, required_params)

            param = Validators.validate_input_str_param(
                param=address.get("state_abbreviation"),
                param_name="state_abbreviation",
                valid_values=VALID_US_STATE_ABBREV,
                params_dict={},
            )
            param = Validators.validate_us_zip_code(zip_code=address.get("zip_code"))

        response = self._post(url=self.full_url, data=addresses)
        resp_data = response.json()
        results = pd.DataFrame(resp_data)
        self.client.estimated_session_credit_usage += results.shape[0]
        return results
