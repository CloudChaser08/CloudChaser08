"""
    Data models for stats generation
"""
# pylint: disable=too-few-public-methods
import attr

from .validators import is_bool, optional_instance_of, is_date_str

from .converters import (
    model_converter,
    model_map_converter,
    model_list_converter,
)
from .fields import (
    create_required_str_field,
    create_optional_str_field,
    create_optional_bool_field,
    create_model_field,
    create_str_list_field
)


# Set of valid data types
VALID_DATATYPES = {
    'emr',
    'emr_clin_obsn',
    'emr_diag',
    'emr_enc',
    'emr_lab_test',
    'emr_medctn',
    'emr_proc',
    'emr_prov_ord',
    'events',
    'labtests',
    'medicalclaims',
    'pharmacyclaims',
}


class _BaseModel(object):

    def copy_with(self, **kwargs):
        """ Copies the model instance, with additional args """
        return attr.evolve(self, **kwargs)

    def to_dict(self):
        """ Converts the model to a dictionary """
        return attr.asdict(self)


@attr.s(frozen=True)
class Column(_BaseModel):
    """ Column data """
    name = create_required_str_field()
    field_id = create_required_str_field()
    sequence = create_required_str_field()


@attr.s(frozen=True)
class FillRateConfig(_BaseModel):
    """ Fill rate configuration """
    columns = attr.ib(converter=model_map_converter(Column))


@attr.s(frozen=True)
class TopValuesConfig(_BaseModel):
    """ Fill rate configuration """
    columns = attr.ib(converter=model_map_converter(Column))
    max_values = attr.ib(
        validator=attr.validators.instance_of(int),
        default=10
    )


@attr.s(frozen=True)
class LongitudinalityConfig(_BaseModel):
    """ Longitudinality configuration """
    patient_id_field = create_required_str_field()


@attr.s(frozen=True)
class YearOverYearConfig(_BaseModel):
    """ Year-over-year configuration """
    patient_id_field = create_required_str_field()


@attr.s(frozen=True)
class KeyStatsConfig(_BaseModel):
    """ Year-over-year configuration """
    patient_field = create_required_str_field()
    record_field = create_required_str_field()


@attr.s(frozen=True)
class EPICalcsConfig(_BaseModel):
    """ EMI Calcs configuration (currently empty) """

    fields = create_str_list_field()


@attr.s(frozen=True)
class ProviderModel(_BaseModel):
    """ A provider config model object """

    # Required fields
    datatype = attr.ib(validator=attr.validators.in_(VALID_DATATYPES))

    # Optional fields
    record_field = create_optional_str_field()
    date_fields = create_str_list_field()
    fill_rate = create_optional_bool_field()
    top_values = create_optional_bool_field()

    fill_rate_conf = create_model_field(FillRateConfig)
    top_values_conf = create_model_field(TopValuesConfig)


@attr.s(frozen=True)
class Provider(_BaseModel):
    """ A provider config object """

    # Required fields
    name = create_required_str_field()
    datafeed_id = attr.ib(validator=lambda _, __, value: int(value))
    earliest_date = attr.ib(validator=is_date_str)
    datatype = attr.ib(validator=attr.validators.in_(VALID_DATATYPES))

    # Optional fields
    date_fields = create_str_list_field()
    record_field = create_optional_str_field()
    fill_rate = create_optional_bool_field()
    key_stats = create_optional_bool_field()
    top_values = create_optional_bool_field()
    longitudinality = create_optional_bool_field()
    year_over_year = create_optional_bool_field()
    epi_calcs = create_optional_bool_field()
    index_all_dates = create_optional_bool_field()
    index_null_dates = create_optional_bool_field()
    models = attr.ib(
        converter=model_list_converter(ProviderModel), default=None
    )
    custom_schema = create_optional_str_field()
    custom_table = create_optional_str_field()

    fill_rate_conf = create_model_field(FillRateConfig)
    top_values_conf = create_model_field(TopValuesConfig)
    epi_calc_conf = create_model_field(EPICalcsConfig)

    longitudinality_conf = create_model_field(LongitudinalityConfig)
    longitudinality_conf_file = create_optional_str_field()
    year_over_year_conf = create_model_field(YearOverYearConfig)
    year_over_year_conf_file = create_optional_str_field()
    key_stats_conf = create_model_field(KeyStatsConfig)
    key_stats_conf_file = create_optional_str_field()

    def merge_provider_model(self, provider_model):
        """ Merges non-null fields from a ProviderModel object into a copy of
            this provider config
        """
        sparese_prov_model_dict = {
            k: v for k, v in provider_model.to_dict().items() if v is not None
        }
        return self.copy_with(**sparese_prov_model_dict)
