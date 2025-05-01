from enum import Enum
from typing import Any


class RequestMethods(Enum):
    GET = "GET"
    POST = "POST"


class RequestLimits(Enum):
    DEFAULT = 12
    MAX_POST = 1000
    DEFAULT_SMALL = 1000
    DEFAULT_LARGE = 10000


class ResponseCodes(Enum):
    SUCCESS = 200
    CLIENT_ERROR = 400
    FORBIDDEN = 403
    NOT_FOUND = 404
    VALIDATION_ERROR = 422
    RATE_LIMIT_EXCEEDED = 429
    SERVER_ERROR = 500


class PropertyTypes(Enum):
    SINGLE_FAMILY = "SINGLE_FAMILY"
    CONDO = "CONDO"
    TOWNHOUSE = "TOWNHOUSE"
    ALL_PROPERTIES = "ALL_PROPERTIES"


class PropertyTypesUnit(Enum):
    SINGLE_FAMILY = "SINGLE_FAMILY"
    CONDO = "CONDO"
    TOWNHOUSE = "TOWNHOUSE"
    OTHER = "OTHER"


class EntityNames(Enum):
    AMH = "AMH"
    TRICON = "TRICON"
    INVITATION_HOMES = "INVITATION_HOMES"
    HOME_PARTNERS_OF_AMERICA = "HOME_PARTNERS_OF_AMERICA"
    PROGRESS_RESIDENTIAL = "PROGRESS_RESIDENTIAL"
    FIRSTKEY_HOMES = "FIRSTKEY_HOMES"
    AMHERST = "AMHERST"
    VINEBROOK_HOMES = "VINEBROOK_HOMES"
    MAYMONT_HOMES = "MAYMONT_HOMES"
    SFR3 = "SFR3"


class PortfolioSizes(Enum):
    PORTFOLIO_2_TO_9 = "PORTFOLIO_2_TO_9"
    PORTFOLIO_10_TO_99 = "PORTFOLIO_10_TO_99"
    PORTFOLIO_100_TO_999 = "PORTFOLIO_100_TO_999"
    PORTFOLIO_1000_PLUS = "PORTFOLIO_1000_PLUS"
    ALL_PORTFOLIOS = "ALL_PORTFOLIOS"


class LocationTypes(Enum):
    COUNTY = "COUNTY"
    CITY = "CITY"
    ZIP5 = "ZIP5"
    CDP = "CDP"
    VILLAGE = "VILLAGE"
    TOWN = "TOWN"
    CBSA = "CBSA"
    ALL = "ALL"


class USRegions(Enum):
    EAST_NORTH_CENTRAL = "EAST_NORTH_CENTRAL"
    EAST_SOUTH_CENTRAL = "EAST_SOUTH_CENTRAL"
    MIDDLE_ATLANTIC = "MIDDLE_ATLANTIC"
    MOUNTAIN = "MOUNTAIN"
    NEW_ENGLAND = "NEW_ENGLAND"
    PACIFIC = "PACIFIC"
    SOUTH_ATLANTIC = "SOUTH_ATLANTIC"
    WEST_NORTH_CENTRAL = "WEST_NORTH_CENTRAL"
    WEST_SOUTH_CENTRAL = "WEST_SOUTH_CENTRAL"
    ALL = "ALL"


class SortByParams(Enum):
    TOTAL_POPULATION = "TOTAL_POPULATION"
    MEDIAN_INCOME = "MEDIAN_INCOME"
    CASE_SHILLER_20_MARKET = "CASE_SHILLER_20_MARKET"
    CASE_SHILLER_10_MARKET = "CASE_SHILLER_10_MARKET"
    PRICEFEED_MARKET = "PRICEFEED_MARKET"
    PARCL_EXCHANGE_MARKET = "PARCL_EXCHANGE_MARKET"


class SortOrder(Enum):
    ASC = "ASC"
    DESC = "DESC"


class EventTypes(Enum):
    SALE = "SALE"
    LISTING = "LISTING"
    RENTAL = "RENTAL"
    ALL = "ALL"


class ResponseColumns(Enum):
    PARCL_ID = "parcl_id"
    PARCL_PROPERTY_ID = "parcl_property_id"
    DATE = "date"
    EVENT_DATE = "event_date"
    EVENT_TYPE = "event_type"
    TOTAL = "total"
    LIMIT = "limit"
    OFFSET = "offset"
    LINKS = "links"
    ACCOUNT = "account"


class USStateAbbreviations(Enum):
    AK = "AK"
    AL = "AL"
    AR = "AR"
    AZ = "AZ"
    CA = "CA"
    CO = "CO"
    CT = "CT"
    DC = "DC"
    DE = "DE"
    FL = "FL"
    GA = "GA"
    HI = "HI"
    IA = "IA"
    ID = "ID"
    IL = "IL"
    IN = "IN"
    KS = "KS"
    KY = "KY"
    LA = "LA"
    MA = "MA"
    MD = "MD"
    ME = "ME"
    MI = "MI"
    MN = "MN"
    MO = "MO"
    MS = "MS"
    MT = "MT"
    NC = "NC"
    ND = "ND"
    NE = "NE"
    NH = "NH"
    NJ = "NJ"
    NM = "NM"
    NV = "NV"
    NY = "NY"
    OH = "OH"
    OK = "OK"
    OR = "OR"
    PA = "PA"
    PR = "PR"
    RI = "RI"
    SC = "SC"
    SD = "SD"
    TN = "TN"
    TX = "TX"
    UT = "UT"
    VA = "VA"
    VI = "VI"
    VT = "VT"
    WA = "WA"
    WI = "WI"
    WV = "WV"
    WY = "WY"
    ALL = "ALL"


class USStateFIPSCodes(Enum):
    ALABAMA = "01"
    ALASKA = "02"
    ARIZONA = "04"
    ARKANSAS = "05"
    CALIFORNIA = "06"
    COLORADO = "08"
    CONNECTICUT = "09"
    DELAWARE = "10"
    DISTRICT_OF_COLUMBIA = "11"
    FLORIDA = "12"
    GEORGIA = "13"
    HAWAII = "15"
    IDAHO = "16"
    ILLINOIS = "17"
    INDIANA = "18"
    IOWA = "19"
    KANSAS = "20"
    KENTUCKY = "21"
    LOUISIANA = "22"
    MAINE = "23"
    MARYLAND = "24"
    MASSACHUSETTS = "25"
    MICHIGAN = "26"
    MINNESOTA = "27"
    MISSISSIPPI = "28"
    MISSOURI = "29"
    MONTANA = "30"
    NEBRASKA = "31"
    NEVADA = "32"
    NEW_HAMPSHIRE = "33"
    NEW_JERSEY = "34"
    NEW_MEXICO = "35"
    NEW_YORK = "36"
    NORTH_CAROLINA = "37"
    NORTH_DAKOTA = "38"
    OHIO = "39"
    OKLAHOMA = "40"
    OREGON = "41"
    PENNSYLVANIA = "42"
    RHODE_ISLAND = "44"
    SOUTH_CAROLINA = "45"
    SOUTH_DAKOTA = "46"
    TENNESSEE = "47"
    TEXAS = "48"
    UTAH = "49"
    VERMONT = "50"
    VIRGINIA = "51"
    WASHINGTON = "53"
    WEST_VIRGINIA = "54"
    WISCONSIN = "55"
    WYOMING = "56"
    AMERICAN_SAMOA = "60"
    GUAM = "66"
    NORTHERN_MARIANA_ISLANDS = "69"
    PUERTO_RICO = "72"
    US_VIRGIN_ISLANDS = "78"
    ALL = "ALL"


def get_enum_values(enum_class: Enum) -> list[Any]:
    """
    Get the values of an enum class.

    Args:
        enum_class (Enum): The enum class to get the values of.

    Returns:
        List[Any]: A list of the values of the enum class.
    """

    return [item.value for item in enum_class]
