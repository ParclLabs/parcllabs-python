from parcllabs.enums import *


VALID_PROPERTY_TYPES = get_enum_values(PropertyTypes)
VALID_PROPERTY_TYPES_UNIT_SEARCH = get_enum_values(PropertyTypesUnit)
VALID_ENTITY_NAMES = get_enum_values(EntityNames)
VALID_PORTFOLIO_SIZES = get_enum_values(PortfolioSizes)
VALID_LOCATION_TYPES = get_enum_values(LocationTypes)
VALID_US_REGIONS = get_enum_values(USRegions)
VALID_US_STATE_ABBREV = get_enum_values(USStateAbbreviations)

VALID_US_STATE_FIPS_CODES = [
    "01",
    "02",
    "04",
    "05",
    "06",
    "08",
    "09",
    "10",
    "11",
    "12",
    "13",
    "15",
    "16",
    "17",
    "18",
    "19",
    "20",
    "21",
    "22",
    "23",
    "24",
    "25",
    "26",
    "27",
    "28",
    "29",
    "30",
    "31",
    "32",
    "33",
    "34",
    "35",
    "36",
    "37",
    "38",
    "39",
    "40",
    "41",
    "42",
    "44",
    "45",
    "46",
    "47",
    "48",
    "49",
    "50",
    "51",
    "53",
    "54",
    "55",
    "56",
    "60",
    "66",
    "69",
    "72",
    "78",
    "ALL",
]

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
