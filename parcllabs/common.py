from parcllabs.enums import (
    RequestMethods,
    ResponseColumns,
    USStateAbbreviations,
    get_enum_values,
)

VALID_US_STATE_ABBREV = get_enum_values(USStateAbbreviations)

ID_COLUMNS = [ResponseColumns.PARCL_ID.value, ResponseColumns.PARCL_PROPERTY_ID.value]
DATE_COLUMNS = [ResponseColumns.DATE.value, ResponseColumns.EVENT_DATE.value]

DELETE_FROM_OUTPUT = [
    ResponseColumns.TOTAL.value,
    ResponseColumns.LIMIT.value,
    ResponseColumns.OFFSET.value,
    ResponseColumns.LINKS.value,
    ResponseColumns.ACCOUNT.value,
]

PARCL_LABS_DASHBOARD_URL = "https://dashboard.parcllabs.com/signup"
NO_API_KEY_ERROR = (
    f"API Key is required. Please visit {PARCL_LABS_DASHBOARD_URL} to get an API key."
)

GET_METHOD = RequestMethods.GET.value
POST_METHOD = RequestMethods.POST.value


ZIP_CODE_LENGTH = 5
