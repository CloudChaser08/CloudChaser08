import csv

import spark.helpers.postprocessor as postprocessor
import spark.helpers.records_loader as records_loader
from pyspark.sql.functions import lit
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException


def get_headers(file_name, table_name):
    table_name = table_name.lower()

    file_schema = TABLE_COLS[table_name]

    with open(file_name, 'r') as infile:
        dialect = csv.Sniffer().sniff(infile.read(4096))
        infile.seek(0)
        reader = csv.reader(infile, dialect)
        header = next(reader)

    return header, file_schema


def validate_file(file_name, table_name):
    header, file_schema = get_headers(file_name, table_name)
    return header == file_schema


def get_tablename_for_date(table, batch_date):
    table = table.lower()
    tns = sorted([tn for tn in TABLE_COLS.keys() if tn.startswith(table)])
    tn = None
    for t in tns:
        if t > (table + '_' + batch_date.replace('-', '')):
            tn = t
            break
    if tn is None:
        tn = table
    return tn


def validate_header(df, tn):
    first_col = df.columns[0]
    header = df.where(df[first_col].contains('_')).first()
    if header:
        header = [s for s in header]
        if header != TABLE_COLS[tn]:
            print("Error in header validation for table {}.\nSchema: {}\nActual Header: {}".format(tn, TABLE_COLS[tn],
                                                                                                   header))
            print("Difference: {}".format([(i, j) for (i, j) in zip(header, TABLE_COLS[tn]) if i != j]))
        return header == TABLE_COLS[tn]
    else:
        return True


def load(spark, runner, table_locs, batch_date, test=False):
    for table, input_path in table_locs.items():
        try:
            tn = get_tablename_for_date(table, batch_date)
            df = records_loader.load(runner, input_path, TABLE_COLS[tn], 'csv', '|')
            if not validate_header(df, tn):
                raise ValueError('Error in header validation')
            if tn == 'd_lab_directory' and 'provider_key' not in df.columns:
                df = df.withColumn('provider_key', lit(None).cast('string'))
            if table in ['d_costar', 'd_patient', 'd_cpt', 'd_drug', 'd_icd10', 'd_icd9', 'd_lab_directory',
                         'd_multum_to_ndc', 'd_provider', 'd_vaccine_cpt']:
                postprocessor.compose(
                    postprocessor.trimmify,
                    lambda df: postprocessor.nullify(df, ['NULL', ''])
                )(df).cache().createOrReplaceTempView(table)
                runner.sqlContext.table(tn).count()
            else:
                postprocessor.compose(
                    postprocessor.trimmify,
                    lambda df: postprocessor.nullify(df, ['NULL', ''])
                )(df).where("date_key != 'date_key'").repartition(1 if test else 5000,
                                                                  'patient_key').cache().createOrReplaceTempView(table)
        except AnalysisException:
            df = spark.createDataFrame(
                spark.sparkContext.emptyRDD(),
                schema=StructType(map(lambda x: StructField(x, StringType()), TABLE_COLS[table]))
            )
            if tn == 'd_lab_directory' and 'provider_key' not in df.columns:
                df = df.withColumn('provider_key', lit(None).cast('string'))
            df.createOrReplaceTempView(table)


TABLE_COLS = {
    'd_costar': [
        'costar_key',
        'full_code',
        'base_code',
        'code_to_store',
        'concept_name',
        'semantic_type',
        'base_code_onto_by_icd_9cm',
        'representative_icd_9cm_code',
        'icd_9cm_set',
        'representative_icd_10cm_code',
        'representative_icd_10cm_branch'
    ],
    'd_cpt': [
        'cpt_key',
        'cpt_id',
        'cpt_code',
        'cpt_description',
        'cpt_short_description',
        'cpt_common',
        'fee',
        'rvu'
    ],
    'd_drug': [
        'drug_key',
        'drug_id',
        'drug_sub_id_1',
        'drug_name',
        'drug_route_id',
        'drug_dosage_form_id',
        'strength',
        'strength_uom',
        'route',
        'dosage_form',
        'generic_name',
        'dea_generic_named_code',
        'dea_legend_code',
        'theraputic_category',
        'drug_info',
        'generic_drug_name_override',
        'manufacturer'
    ],
    'd_icd10': [
        'icd10_key',
        'order_number',
        'icd_10_code',
        'is_complete',
        'short_description',
        'long_description',
        'is_active'
    ],
    'd_icd9': [
        'icd9_key',
        'id',
        'code',
        'short_description',
        'shorter_description',
        'description'
    ],
    'd_lab_directory_20180101_deprecated': [
        'lab_directory_key',
        'test_code',
        'lab_company',
        'test_name'
    ],
    'd_lab_directory': [
        'practice_key',
        'lab_directory_key',
        'test_code',
        'lab_company',
        'test_name'
    ],
    'd_multum_to_ndc': [
        'ndc',
        'drug_name',
        'multum_id',
        'active_status',
        'touch_date'
    ],
    'd_patient': [
        'patient_key',
        'practice_key',
        'gender',
        'birth_year',
        'state',
        'zip',
        'inactive',
        'reason_inactive',
        'marital_status',
        'takes_no_meds',
        'patient_race',
        'language_preference',
        'ethnicity_id',
        'date_of_death',
        'state_code',
        'ethnicity_description',
        'race_description'
    ],
    'd_provider': [
        'provider_key',
        'practice_key',
        'degree',
        'state',
        'specialty'
    ],
    'd_vaccine_cpt': [
        'vaccine_cpt_key',
        'vaccine_cpt_id',
        'vaccine_name',
        'vaccine_sub_component',
        'cpt_code',
        'cvx_code',
        'country',
        'unique_id',
        'date_last_touched',
        'is_generic',
        'cvx_code_unspecified_formulation',
        'immunity_code'
    ],
    'f_diagnosis_20180301_deprecated': [
        'date_key',
        'time_key',
        'record_type',
        'practice_key',
        'patient_key',
        'problem_icd',
        'icd_type',
        'date_active',
        'date_inactive',
        'provider_key',
        'date_row_added',
        'date_last_activated',
        'date_resolved',
        'costar_key',
        'snomed'
    ],
    'f_diagnosis': [
        'date_key',
        'time_key',
        'record_type',
        'practice_key',
        'patient_key',
        'problem_icd',
        'icd_type',
        'date_active',
        'date_inactive',
        'provider_key',
        'date_row_added',
        'date_last_activated',
        'date_resolved',
        'costar_key',
        'snomed'
    ]
    ,
    'f_diagnosis_20180701_deprecated': [
        'date_key',
        'time_key',
        'practice_key',
        'patient_key',
        'record_type',
        'problem_icd',
        'icd_type',
        'date_active',
        'date_inactive',
        'provider_key',
        'date_row_added',
        'date_last_activated',
        'date_resolved',
        'costar_key',
        'snomed'
    ],
    'f_encounter': [
        'date_key',
        'time_key',
        'practice_key',
        'patient_key',
        'encounter_date',
        'chief_complaint',
        'history_of_present_illness',
        'review_of_systems',
        'past_medical_history',
        'current_medications',
        'allergies',
        'social_history',
        'family_history',
        'physical_exam',
        'assessment',
        'plan',
        'blood_pressure',
        'temperature',
        'respiratory_rate',
        'pulse',
        'weight',
        'height',
        'body_mass_index',
        'head_circumference',
        'vital_comments',
        'provider_key',
        'age_gender_sentence',
        'callback_comment',
        'cpt_code',
        'cpt_comments',
        'tests',
        'date_row_added',
        'oxygen_saturation',
        'pain_scale',
        'pulmonary_function',
        'other_vitals',
        'oxygen_saturation_room_air',
        'supplemental_O2_amount',
        'peak_flow_post_bronchodilator',
        'supplemental_O2_type',
        'packs_per_day',
        'years_smoked',
        'years_quit',
        'last_menstrual_period',
        'estimated_delivery_date',
        'pregnancy_comments',
        'vision_os',
        'vision_od',
        'hearing',
        'hearing_comments',
        'systolic_blood_pressure_supine',
        'diastolic_blood_pressure_supine',
        'weight_in_pounds',
        'systolic',
        'diastolic',
        'height_in_inches'
    ],
    'f_injection_20180301_deprecated': [
        'date_key',
        'time_key',
        'record_type',
        'record_name',
        'practice_key',
        'patient_key',
        'vaccine_cpt_key',
        'provider_key',
        'lot_no',
        'date_given',
        'volume',
        'route',
        'site',
        'manufacturer',
        'expiration',
        'sequence',
        'type',
        'cpt',
        'is_given_elsewhere',
        'patient_refused',
        'vis_version',
        'vis_date_given',
        'deleted',
        'date_sent_to_registry',
        'patient_parent_refused',
        'patient_had_infection',
        'how_migrated',
        'reaction_date'
    ],
    'f_injection': [
        'date_key',
        'time_key',
        'record_type',
        'record_name',
        'practice_key',
        'patient_key',
        'vaccine_cpt_key',
        'provider_key',
        'lot_no',
        'date_given',
        'volume',
        'route',
        'site',
        'manufacturer',
        'expiration',
        'sequence',
        'type',
        'cpt',
        'is_given_elsewhere',
        'patient_refused',
        'vis_version',
        'vis_date_given',
        'deleted',
        'date_sent_to_registry',
        'patient_parent_refused',
        'patient_had_infection',
        'how_migrated',
        'reaction_date'
    ],
    'f_injection_20180701_deprecated': [
        'date_key',
        'time_key',
        'practice_key',
        'patient_key',
        'record_type',
        'record_name',
        'vaccine_cpt_key',
        'provider_key',
        'lot_no',
        'date_given',
        'volume',
        'route',
        'site',
        'manufacturer',
        'expiration',
        'sequence',
        'type',
        'cpt',
        'is_given_elsewhere',
        'patient_refused',
        'vis_version',
        'vis_date_given',
        'deleted',
        'date_sent_to_registry',
        'patient_parent_refused',
        'patient_had_infection',
        'how_migrated',
        'reaction_date'
    ],
    'f_lab_20180301_deprecated': [
        'date_key',
        'time_key',
        'practice_key',
        'lab_test_id',
        'patient_key',
        'created_date_lt',
        'lab_directory_key',
        'specimen_nbr_lt',
        'specimen_status',
        'fasting',
        'lab_test_status_lt',
        'sign_off_id',
        'sign_off_date',
        'comments',
        'lab_result_id',
        'accession_nbr_ac',
        'ordering_provider_id',
        'specimen_nbr_lr',
        'lab_test_code_lr',
        'specimen_volume',
        'specimen_collected_dt',
        'action_code',
        'clinical_info',
        'specimen_source',
        'alternate_id_1',
        'alternate_id_2',
        'lab_test_status_lr',
        'parent_for_reflex_obx',
        'parent_for_reflex_obr',
        'specimen_condition',
        'lab_result_detail_id',
        'inactive_flag',
        'corrects_lab_test_id',
        'corrected_by_lab_test_id',
        'lab_test_status_lrd',
        'lab_test_code_lrd',
        'loinc_test_code',
        'observation_sub_id',
        'observation_value',
        'uom',
        'reference_ranges',
        'abnormal_flag',
        'normal_abnormal_type',
        'value_type'
    ],
    'f_lab': [
        'date_key',
        'time_key',
        'practice_key',
        'lab_test_id',
        'patient_key',
        'created_date_lt',
        'lab_directory_key',
        'specimen_nbr_lt',
        'specimen_status',
        'fasting',
        'lab_test_status_lt',
        'sign_off_id',
        'sign_off_date',
        'comments',
        'lab_result_id',
        'accession_nbr_ac',
        'ordering_provider_id',
        'specimen_nbr_lr',
        'lab_test_code_lr',
        'specimen_volume',
        'specimen_collected_dt',
        'action_code',
        'clinical_info',
        'specimen_source',
        'alternate_id_1',
        'alternate_id_2',
        'lab_test_status_lr',
        'parent_for_reflex_obx',
        'parent_for_reflex_obr',
        'specimen_condition',
        'lab_result_detail_id',
        'inactive_flag',
        'corrects_lab_test_id',
        'corrected_by_lab_test_id',
        'lab_test_status_lrd',
        'lab_test_code_lrd',
        'loinc_test_code',
        'observation_sub_id',
        'observation_value',
        'uom',
        'reference_ranges',
        'abnormal_flag',
        'normal_abnormal_type',
        'value_type'
    ],
    'f_lab_20180701_deprecated': [
        'date_key',
        'time_key',
        'practice_key',
        'patient_key',
        'comments',
        'lab_test_id',
        'created_date_lt',
        'lab_directory_key',
        'specimen_nbr_lt',
        'specimen_status',
        'fasting',
        'lab_test_status_lt',
        'sign_off_id',
        'sign_off_date',
        'lab_result_id',
        'accession_nbr_ac',
        'ordering_provider_id',
        'specimen_nbr_lr',
        'lab_test_code_lr',
        'specimen_volume',
        'specimen_collected_dt',
        'action_code',
        'clinical_info',
        'specimen_source',
        'alternate_id_1',
        'alternate_id_2',
        'lab_test_status_lr',
        'parent_for_reflex_obx',
        'parent_for_reflex_obr',
        'specimen_condition',
        'lab_result_detail_id',
        'inactive_flag',
        'corrects_lab_test_id',
        'corrected_by_lab_test_id',
        'lab_test_status_lrd',
        'lab_test_code_lrd',
        'loinc_test_code',
        'observation_sub_id',
        'observation_value',
        'uom',
        'reference_ranges',
        'abnormal_flag',
        'normal_abnormal_type',
        'value_type'
    ],
    'f_medication_20180301_deprecated': [
        'date_key',
        'time_key',
        'practice_key',
        'patient_key',
        'provider_key',
        'med_name',
        'med_sig',
        'med_no',
        'med_refill',
        'med_dns',
        'date_initiated',
        'date_last_refilled',
        'med_comments',
        'prior_refills',
        'refillable',
        'inactive',
        'drug_key',
        'quick_add_reason_prescribed',
        'deleted',
        'date_inactivated',
        'date_started',
        'dispense_qualifier',
        'erx_status',
        'daw',
        'sent_by_sure_scripts',
        'inactivate_reason',
        'script_printed',
        'script_faxed'
    ],
    'f_medication_20180701_deprecated': [
        'date_key',
        'time_key',
        'practice_key',
        'patient_key',
        'inactive',
        'provider_key',
        'med_name',
        'med_sig',
        'med_no',
        'med_refill',
        'med_dns',
        'date_initiated',
        'date_last_refilled',
        'med_comments',
        'prior_refills',
        'refillable',
        'drug_key',
        'quick_add_reason_prescribed',
        'deleted',
        'date_inactivated',
        'date_started',
        'dispense_qualifier',
        'erx_status',
        'daw',
        'sent_by_sure_scripts',
        'inactivate_reason',
        'script_printed',
        'script_faxed'
    ],
    'f_medication': [
        'date_key',
        'time_key',
        'practice_key',
        'patient_key',
        'provider_key',
        'med_name',
        'med_sig',
        'med_no',
        'med_refill',
        'med_dns',
        'date_initiated',
        'date_last_refilled',
        'med_comments',
        'prior_refills',
        'refillable',
        'inactive',
        'drug_key',
        'quick_add_reason_prescribed',
        'deleted',
        'date_inactivated',
        'date_started',
        'dispense_qualifier',
        'erx_status',
        'daw',
        'sent_by_sure_scripts',
        'inactive_reason',
        'script_printed',
        'script_faxed'
    ],
    'f_procedure': [
        'date_key',
        'time_key',
        'practice_key',
        'patient_key',
        'provider_key',
        'date_of_service',
        'cpt_key',
        'units',
        'price',
        'date_performed'
    ]
}
