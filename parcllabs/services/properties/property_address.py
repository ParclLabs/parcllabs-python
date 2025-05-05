import pandas as pd

from parcllabs.common import VALID_US_STATE_ABBREV
from parcllabs.services.parcllabs_service import ParclLabsService
from parcllabs.services.validators import Validators


class PropertyAddressSearch(ParclLabsService):
    """
    Retrieve parcl_property_ids based on provided addresses.
    """

    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)

    def retrieve(
        self,
        addresses: list[dict],
    ) -> pd.DataFrame:
        """
        Retrieve parcl_property_ids based on provided addresses.

        Args:

            addresses (list): A list of dictionaries containing address information.
            Each dictionary should contain the following keys:
            - address: The street address
            - city: The city
            - state_abbreviation: The state abbreviation
            - zip_code: The zip code

        Returns:

            DataFrame: A DataFrame containing the parcl_property_id and address
            information.
        """

        required_params = ["address", "city", "state_abbreviation", "zip_code"]
        for address in addresses:
            Validators.validate_field_exists(address, required_params)
            Validators.validate_input_str_param(
                param=address.get("state_abbreviation"),
                param_name="state_abbreviation",
                valid_values=VALID_US_STATE_ABBREV,
                params_dict={},
            )
            Validators.validate_us_zip_code(zip_code=address.get("zip_code"))

        response = self._post(url=self.full_url, data=addresses)
        results = response.json()
        data = pd.DataFrame(results.get("items"))
        self._update_account_info(results.get("account"))
        return data
