"""
    Marketplace data layout models. These models are used to structure
    stats results into a format that is compatibly with and friendly to
    HealthVerity Marketplace.
"""
# pylint: disable=too-few-public-methods
import attr

from ._base import BaseModel

from .fields import (
    create_required_str_field,
    create_optional_str_field,
    create_model_field,
    create_model_list_field,
)
from .validators import optional_instance_of
from .results import ProviderStatsResult


@attr.s(frozen=True)
class LayoutDataTable(BaseModel):
    """ A data table for the data layout """
    id = create_required_str_field()
    name = create_required_str_field()
    description = create_required_str_field()
    sequence = create_required_str_field()


@attr.s(frozen=True)
class LayoutField(BaseModel):
    """ A single field in the data layout """

    # Required Fields
    id = create_required_str_field()
    name = create_required_str_field()
    description = create_required_str_field()
    category = create_required_str_field()
    data_feed = create_required_str_field()
    sequence = create_required_str_field()
    datatable = create_model_field(LayoutDataTable, optional=False)
    field_type_name = create_required_str_field()

    # Optional Fields
    supplemental_type_name = create_optional_str_field()
    fill = attr.ib(validator=optional_instance_of(float), default=None)
    top_values = create_optional_str_field()


@attr.s(frozen=True)
class Layout(BaseModel):
    """ The full data layout """

    # Stores the structured stats results, as-is
    stats = create_model_field(ProviderStatsResult, optional=False)

    # Stores flattened list of all data fields for the provider. Note that
    # this is offered for legacy support of existing Marketplace serialization.
    # See spark.stats.data_layout for the logic that constructs these fields
    fields = create_model_list_field(LayoutField)
