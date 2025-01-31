from parcllabs.enums import *


VALID_PROPERTY_TYPES = get_enum_values(PropertyTypes)
VALID_PROPERTY_TYPES_UNIT_SEARCH = get_enum_values(PropertyTypesUnit)
VALID_ENTITY_NAMES = get_enum_values(EntityNames)
VALID_PORTFOLIO_SIZES = get_enum_values(PortfolioSizes)
VALID_LOCATION_TYPES = get_enum_values(LocationTypes)
VALID_US_REGIONS = get_enum_values(USRegions)
VALID_US_STATE_ABBREV = get_enum_values(USStateAbbreviations)
VALID_US_STATE_FIPS_CODES = get_enum_values(USStateFIPSCodes)
VALID_SORT_BY = get_enum_values(SortByParams)
VALID_SORT_ORDER = get_enum_values(SortOrder)
VALID_EVENT_TYPES = get_enum_values(EventTypes)

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
