"""
    Models for stats results
"""

import attr

from ._base import BaseModel
from .fields import (
    create_required_str_field,
    create_model_list_field,
    create_model_field,
    create_model_map_field
)


@attr.s(frozen=True)
class FillRateResult(BaseModel):
    """ Fill rates result for a single column """
    field = create_required_str_field()
    fill = attr.ib(validator=attr.validators.instance_of(float))


@attr.s(frozen=True)
class TopValuesResult(BaseModel):
    """ Top values result for a single column/value pair """
    field = create_required_str_field()
    value = attr.ib()
    count = attr.ib(validator=attr.validators.instance_of(int))
    percentage = attr.ib(validator=attr.validators.instance_of(float))


@attr.s(frozen=True)
class LongitudinalityResult(BaseModel):
    """ Longitudinality result """
    average = attr.ib(validator=attr.validators.instance_of(int))
    duration = create_required_str_field()
    std_dev = attr.ib(validator=attr.validators.instance_of(int))
    value = attr.ib(validator=attr.validators.instance_of(int))


@attr.s(frozen=True)
class YearOverYearResult(BaseModel):
    """ Year over year result for a single year """
    count = attr.ib(validator=attr.validators.instance_of(int))
    year = attr.ib(validator=attr.validators.instance_of(int))


@attr.s(frozen=True)
class GenericStatsResult(BaseModel):
    """ Generic field/value result set """
    field = create_required_str_field()
    value = attr.ib(validator=attr.validators.instance_of(int))


@attr.s(frozen=True)
class StatsResult(BaseModel):
    """ All combined stats results """
    fill_rate = create_model_list_field(FillRateResult, optional=True)
    top_values = create_model_list_field(TopValuesResult, optional=True)
    longitudinality = create_model_list_field(LongitudinalityResult, optional=True)
    year_over_year = create_model_list_field(YearOverYearResult, optional=True)
    epi_calcs = create_model_list_field(GenericStatsResult, optional=True)
    key_stats = create_model_list_field(GenericStatsResult, optional=True)


@attr.s(frozen=True)
class ProviderStatsResult(BaseModel):
    """ All results for a provider, including for sub-models """

    # Global result set
    results = create_model_field(StatsResult, optional=False)
    # Result set for individual sub-models
    model_results = create_model_map_field(StatsResult)
