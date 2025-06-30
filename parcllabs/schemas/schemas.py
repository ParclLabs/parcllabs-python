"""
Pydantic schemas for PropertyV2Service input parameters.
"""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, ValidationInfo, field_validator

from parcllabs.enums import PropertyTypes, RequestLimits


class GeoCoordinates(BaseModel):
    """Schema for geographic coordinates with radius."""

    latitude: float = Field(..., ge=-90, le=90, description="Latitude in decimal degrees")
    longitude: float = Field(..., ge=-180, le=180, description="Longitude in decimal degrees")
    radius: float = Field(..., gt=0, description="Radius in miles")


class PropertyV2RetrieveParams(BaseModel):
    """
    Input parameters schema for PropertyV2Service.retrieve() method.

    This schema validates and manages all input parameters for property searches,
    including search criteria, property filters, event filters, and owner filters.
    """

    # Search criteria
    parcl_ids: list[int] | None = Field(default=None, description="List of parcl_ids to filter by")
    parcl_property_ids: list[int] | None = Field(
        default=None, description="List of parcl_property_ids to filter by"
    )
    geo_coordinates: GeoCoordinates | None = Field(
        default=None, description="Geographic coordinates with radius to filter by"
    )

    # Property filters
    property_types: list[str] | None = Field(
        default=None, description="List of property types to filter by"
    )
    min_beds: int | None = Field(default=None, ge=0, description="Minimum number of bedrooms")
    max_beds: int | None = Field(default=None, ge=0, description="Maximum number of bedrooms")
    min_baths: float | None = Field(default=None, ge=0, description="Minimum number of bathrooms")
    max_baths: float | None = Field(default=None, ge=0, description="Maximum number of bathrooms")
    min_sqft: int | None = Field(default=None, ge=0, description="Minimum square footage")
    max_sqft: int | None = Field(default=None, ge=0, description="Maximum square footage")
    min_year_built: int | None = Field(
        default=None, ge=1800, le=2100, description="Minimum year built"
    )
    max_year_built: int | None = Field(
        default=None, ge=1800, le=2100, description="Maximum year built"
    )
    include_property_details: bool | None = Field(
        default=None, description="Whether to include property details"
    )
    min_record_added_date: str | None = Field(
        default=None, description="Minimum record added date (YYYY-MM-DD)"
    )
    max_record_added_date: str | None = Field(
        default=None, description="Maximum record added date (YYYY-MM-DD)"
    )

    # Event filters
    event_names: list[str] | None = Field(
        default=None, description="List of event names to filter by"
    )
    min_event_date: str | None = Field(default=None, description="Minimum event date (YYYY-MM-DD)")
    max_event_date: str | None = Field(default=None, description="Maximum event date (YYYY-MM-DD)")
    min_price: int | None = Field(default=None, ge=0, description="Minimum price")
    max_price: int | None = Field(default=None, ge=0, description="Maximum price")
    is_new_construction: bool | None = Field(
        default=None, description="Whether to filter by new construction"
    )
    min_record_updated_date: str | None = Field(
        default=None, description="Minimum record updated date (YYYY-MM-DD)"
    )
    max_record_updated_date: str | None = Field(
        default=None, description="Maximum record updated date (YYYY-MM-DD)"
    )

    # Owner filters
    is_current_owner: bool | None = Field(
        default=None, description="Whether to filter by current owner"
    )
    owner_name: list[str] | None = Field(
        default=None, description="List of owner names to filter by"
    )
    is_investor_owned: bool | None = Field(
        default=None, description="Whether to filter by investor owned"
    )
    is_owner_occupied: bool | None = Field(
        default=None, description="Whether to filter by owner occupied"
    )

    # Market flags
    current_on_market_flag: bool | None = Field(
        default=None, description="Whether to filter by current on market flag"
    )
    current_on_market_rental_flag: bool | None = Field(
        default=None, description="Whether to filter by current on market rental flag"
    )

    # Pagination
    limit: int | None = Field(
        default=None,
        ge=1,
        le=RequestLimits.PROPERTY_V2_MAX.value,
        description=f"Number of results to return (max: {RequestLimits.PROPERTY_V2_MAX.value})",
    )

    # Additional parameters
    params: dict[str, Any] | None = Field(
        default_factory=dict, description="Additional parameters to pass to the request"
    )

    @field_validator("property_types")
    @classmethod
    def validate_property_types(cls, v: list[str] | None) -> list[str] | None:
        """Validate property types against allowed values."""
        if v is not None:
            valid_types = [pt.value for pt in PropertyTypes]
            for prop_type in v:
                if prop_type.upper() not in valid_types:
                    raise ValueError(f"Invalid property type: {prop_type}")
            return [pt.upper() for pt in v]
        return v

    @field_validator("event_names")
    @classmethod
    def validate_event_names(cls, v: list[str] | None) -> list[str] | None:
        """Validate event names and convert to uppercase."""
        if v is not None:
            return [name.upper() for name in v]
        return v

    @field_validator("owner_name")
    @classmethod
    def validate_owner_names(cls, v: list[str] | None) -> list[str] | None:
        """Validate owner names and convert to uppercase."""
        if v is not None:
            return [name.upper() for name in v]
        return v

    @field_validator(
        "min_record_added_date",
        "max_record_added_date",
        "min_event_date",
        "max_event_date",
        "min_record_updated_date",
        "max_record_updated_date",
    )
    @classmethod
    def validate_date_format(cls, v: str | None) -> str | None:
        """Validate date format is YYYY-MM-DD."""
        if v is not None:
            try:
                datetime.fromisoformat(v)
            except ValueError as err:
                raise ValueError(f"Date must be in YYYY-MM-DD format, got: {v}") from err
            else:
                return v
        return v

    @field_validator("min_beds", "max_beds")
    @classmethod
    def validate_bedroom_range(cls, v: int | None, info: ValidationInfo) -> int | None:
        """Validate bedroom range consistency."""
        if v is not None and info.data:
            field_name = info.field_name
            if field_name == "max_beds" and info.data.get("min_beds"):
                if v < info.data["min_beds"]:
                    raise ValueError("max_beds cannot be less than min_beds")
            elif field_name == "min_beds" and info.data.get("max_beds"):
                if v > info.data["max_beds"]:
                    raise ValueError("min_beds cannot be greater than max_beds")
        return v

    @field_validator("min_baths", "max_baths")
    @classmethod
    def validate_bathroom_range(cls, v: float | None, info: ValidationInfo) -> float | None:
        """Validate bathroom range consistency."""
        if v is not None and info.data:
            field_name = info.field_name
            if field_name == "max_baths" and info.data.get("min_baths"):
                if v < info.data["min_baths"]:
                    raise ValueError("max_baths cannot be less than min_baths")
            elif field_name == "min_baths" and info.data.get("max_baths"):
                if v > info.data["max_baths"]:
                    raise ValueError("min_baths cannot be greater than max_baths")
        return v

    @field_validator("min_sqft", "max_sqft")
    @classmethod
    def validate_sqft_range(cls, v: int | None, info: ValidationInfo) -> int | None:
        """Validate square footage range consistency."""
        if v is not None and info.data:
            field_name = info.field_name
            if field_name == "max_sqft" and info.data.get("min_sqft"):
                if v < info.data["min_sqft"]:
                    raise ValueError("max_sqft cannot be less than min_sqft")
            elif field_name == "min_sqft" and info.data.get("max_sqft"):
                if v > info.data["max_sqft"]:
                    raise ValueError("min_sqft cannot be greater than max_sqft")
        return v

    @field_validator("min_year_built", "max_year_built")
    @classmethod
    def validate_year_built_range(cls, v: int | None, info: ValidationInfo) -> int | None:
        """Validate year built range consistency."""
        if v is not None and info.data:
            field_name = info.field_name
            if field_name == "max_year_built" and info.data.get("min_year_built"):
                if v < info.data["min_year_built"]:
                    raise ValueError("max_year_built cannot be less than min_year_built")
            elif field_name == "min_year_built" and info.data.get("max_year_built"):
                if v > info.data["max_year_built"]:
                    raise ValueError("min_year_built cannot be greater than max_year_built")
        return v

    @field_validator("min_price", "max_price")
    @classmethod
    def validate_price_range(cls, v: int | None, info: ValidationInfo) -> int | None:
        """Validate price range consistency."""
        if v is not None and info.data:
            field_name = info.field_name
            if field_name == "max_price" and info.data.get("min_price"):
                if v < info.data["min_price"]:
                    raise ValueError("max_price cannot be less than min_price")
            elif field_name == "min_price" and info.data.get("max_price"):
                if v > info.data["max_price"]:
                    raise ValueError("min_price cannot be greater than max_price")
        return v

    @field_validator("min_record_added_date", "max_record_added_date")
    @classmethod
    def validate_record_added_date_range(cls, v: str | None, info: ValidationInfo) -> str | None:
        """Validate record added date range consistency."""
        if v is not None and info.data:
            field_name = info.field_name
            if field_name == "max_record_added_date" and info.data.get("min_record_added_date"):
                if v < info.data["min_record_added_date"]:
                    raise ValueError("max_record_added_date cannot be before min_record_added_date")
            elif field_name == "min_record_added_date" and info.data.get("max_record_added_date"):
                if v > info.data["max_record_added_date"]:
                    raise ValueError("min_record_added_date cannot be after max_record_added_date")
        return v

    @field_validator("min_event_date", "max_event_date")
    @classmethod
    def validate_event_date_range(cls, v: str | None, info: ValidationInfo) -> str | None:
        """Validate event date range consistency."""
        if v is not None and info.data:
            field_name = info.field_name
            if field_name == "max_event_date" and info.data.get("min_event_date"):
                if v < info.data["min_event_date"]:
                    raise ValueError("max_event_date cannot be before min_event_date")
            elif field_name == "min_event_date" and info.data.get("max_event_date"):
                if v > info.data["max_event_date"]:
                    raise ValueError("min_event_date cannot be after max_event_date")
        return v

    @field_validator("min_record_updated_date", "max_record_updated_date")
    @classmethod
    def validate_record_updated_date_range(cls, v: str | None, info: ValidationInfo) -> str | None:
        """Validate record updated date range consistency."""
        if v is not None and info.data:
            field_name = info.field_name
            if field_name == "max_record_updated_date" and info.data.get("min_record_updated_date"):
                if v < info.data["min_record_updated_date"]:
                    raise ValueError(
                        "max_record_updated_date cannot be before min_record_updated_date"
                    )
            elif field_name == "min_record_updated_date" and info.data.get(
                "max_record_updated_date"
            ):
                if v > info.data["max_record_updated_date"]:
                    raise ValueError(
                        "min_record_updated_date cannot be after max_record_updated_date"
                    )
        return v

    class Config:
        """Pydantic configuration."""

        extra = "forbid"  # Reject any extra fields
        validate_assignment = True  # Validate on assignment
        str_strip_whitespace = True  # Strip whitespace from strings


class PropertyV2RetrieveParamCategories(BaseModel):
    """High level categories for PropertyV2RetrieveParams."""

    property_filters: dict[str, Any] = Field(default_factory=dict, description="Property filters")
    event_filters: dict[str, Any] = Field(default_factory=dict, description="Event filters")
    owner_filters: dict[str, Any] = Field(default_factory=dict, description="Owner filters")
