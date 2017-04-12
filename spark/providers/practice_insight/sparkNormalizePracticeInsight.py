#! /usr/bin/python
import os
import argparse
import time
from datetime import datetime
from spark.runner import Runner
from spark.spark import init
import spark.helpers.payload_loader as payload_loader
import spark.helpers.file_prefix as file_prefix
import spark.helpers.constants as constants
import spark.helpers.explode as explode
import spark.providers.practice_insight.udf as pi_udf


def get_rel_path(relative_filename):
    return os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            relative_filename
        )
    )


# init
spark, sqlContext = init("Practice Insight")

# register practice insight udfs:
sqlContext.registerFunction(
    'generate_place_of_service_std_id', pi_udf.generate_place_of_service_std_id
)
sqlContext.registerFunction(
    'generate_inst_type_of_bill_std_id',
    pi_udf.generate_inst_type_of_bill_std_id
)

# initialize runner
runner = Runner(sqlContext)

TODAY = time.strftime('%Y-%m-%d', time.localtime())

parser = argparse.ArgumentParser()
parser.add_argument('--date', type=str)
parser.add_argument('--output_path', type=str)
parser.add_argument('--period', type=str, default='current')
parser.add_argument('--shuffle_partitions', type=str, default="1200")
parser.add_argument('--debug', default=False, action='store_true')
args = parser.parse_args()

date_obj = datetime.strptime(args.date, '%Y-%m-%d')

setid = 'HV.data.837.' + str(date_obj.year) + '.csv.gz'

input_path = 's3a://salusv/incoming/medicalclaims/practice_insight/{}/{}/'.format(
    str(date_obj.year),
    str(date_obj.month).zfill(2)
)

if args.period == 'hist':
    matching_path = 's3a://salusv/matching/payload/medicalclaims/practice_insight/{}/'.format(
        str(date_obj.year)
    )
else:
    matching_path = 's3a://salusv/matching/payload/medicalclaims/practice_insight/{}/{}/'.format(
        str(date_obj.year),
        str(date_obj.month).zfill(2)
    )

min_date = '2010-01-01'

# create helper tables
runner.run_spark_script(get_rel_path('create_helper_tables.sql'))
payload_loader.load(runner, matching_path, ['claimId'])


def run(part):
    # Set shuffle partitions to stabilize job
    sqlContext.setConf("spark.sql.shuffle.partitions", args.shuffle_partitions)

    runner.run_spark_script(get_rel_path(
        '../../common/medicalclaims_common_model.sql'
    ), [
        ['table_name', 'medicalclaims_common_model', False],
        ['properties', '', False]
    ])

    # load transactions and payload
    runner.run_spark_script(get_rel_path('load_transactions.sql'), [
        ['input_path', input_path + part + '/']
    ])

    # create claim min npi map
    runner.run_spark_script(get_rel_path('create_min_claim_npi_table.sql'))

    # create explosion maps
    runner.run_spark_script(get_rel_path('create_exploded_diagnosis_map.sql'))
    runner.run_spark_script(get_rel_path('create_exploded_procedure_map.sql'))

    # normalize
    runner.run_spark_script(get_rel_path('normalize.sql'), [
        [
            'date_service_sl',
            """
            CASE
            WHEN extract_date(transactional.svc_from_dt, '%Y%m%d',
                CAST('{min_date}' as date), CAST('{max_date}' as date)) IS NOT NULL
            AND diags.diag_code IN (transactional.diag_cd_1,
                transactional.diag_cd_2, transactional.diag_cd_3,
                transactional.diag_cd_4)
            THEN CAST(extract_date(transactional.svc_from_dt, '%Y%m%d',
                CAST('{min_date}' as date), CAST('{max_date}' as date)) AS DATE)
            WHEN extract_date(transactional.stmnt_from_dt, '%Y%m%d',
                CAST('{min_date}' as date), CAST('{max_date}' as date)) IS NOT NULL
            THEN CAST(extract_date(transactional.stmnt_from_dt, '%Y%m%d',
                CAST('{min_date}' as date), CAST('{max_date}' as date)) AS DATE)
            ELSE (
            SELECT MIN(CAST(extract_date(t2.svc_from_dt, '%Y%m%d',
                CAST('{min_date}' as date), CAST('{max_date}' as date)) AS DATE))
            FROM transactional_raw t2
            WHERE t2.src_claim_id = transactional.src_claim_id
                )
            END
            """.format(
                min_date=min_date,
                max_date=args.date
            ), False
        ],
        [
            'date_service_inst',
            """
            CASE
            WHEN extract_date(transactional.svc_from_dt, '%Y%m%d',
                CAST('{min_date}' as date), CAST('{max_date}' as date)) IS NOT NULL
            THEN CAST(extract_date(transactional.svc_from_dt, '%Y%m%d',
                CAST('{min_date}' as date), CAST('{max_date}' as date)) AS DATE)
            WHEN extract_date(transactional.stmnt_from_dt, '%Y%m%d',
                CAST('{min_date}' as date), CAST('{max_date}' as date)) IS NOT NULL
            THEN CAST(extract_date(transactional.stmnt_from_dt, '%Y%m%d',
                CAST('{min_date}' as date), CAST('{max_date}' as date)) AS DATE)
            ELSE (
            SELECT MIN(CAST(extract_date(t2.svc_from_dt, '%Y%m%d',
                CAST('{min_date}' as date), CAST('{max_date}' as date)) AS DATE))
            FROM transactional_raw t2
            WHERE t2.src_claim_id = transactional.src_claim_id
                )
            END
            """.format(
                min_date=min_date,
                max_date=args.date
            ), False
        ],
        [
            'place_of_service_std_id',
            """
            generate_place_of_service_std_id(
                transactional.claim_type_cd,
                transactional.pos_cd,
                transactional.fclty_type_pos_cd,
                transactional.diag_cd_1,
                transactional.diag_cd_2,
                transactional.diag_cd_3,
                transactional.diag_cd_4,
                diags.diag_code
            )
            """,
            False
        ],
        ['setid', setid],
        ['today', TODAY],
        ['feedname', '22'],
        ['vendor', '3'],
        ['min_date', '2010-01-01'],
        ['max_date', args.date]
    ])

    # explode date ranges
    explode.explode_medicalclaims_dates(runner)

    # unload
    runner.run_spark_script(get_rel_path(
        '../../common/medicalclaims_common_model.sql'
    ), [
        ['table_name', 'final_unload', False],
        [
            'properties',
            constants.unload_properties_template.format(args.output_path),
            False
        ]
    ])

    runner.run_spark_script(
        get_rel_path('../../common/unload_common_model.sql'), [
            [
                'select_statement',
                "SELECT *, 'practice_insight' as provider, 'NULL' as best_date "
                + "FROM medicalclaims_common_model "
                + "WHERE date_service is NULL",
                False
            ],
            ['partitions', '20', False]
        ]
    )
    runner.run_spark_script(
        get_rel_path('../../common/unload_common_model.sql'), [
            [
                'select_statement',
                "SELECT *, 'practice_insight' as provider, regexp_replace("
                + "cast(date_service as string), "
                + "'-..$', '') as best_date "
                + "FROM medicalclaims_common_model "
                + "WHERE date_service IS NOT NULL",
                False
            ],
            ['partitions', '50', False]

        ]
    )

    file_prefix.prefix_part_files(spark, args.output_path, args.date + '_' + part)

    spark.catalog.dropTempView('medicalclaims_common_model')


for part in ['1', '2', '3', '4']:
    run(part)

spark.sparkContext.stop()
