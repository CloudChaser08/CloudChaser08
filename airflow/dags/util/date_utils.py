from datetime import datetime, timedelta

# two types of functions: those to obtain times and those to insert times

# Below are Ilia's functioms in the visionex_emr_pipeline

def get_file_date_nodash(kwargs):
	# add general argument for timedelta, set default value to 0 or 7? 
	# as in get_file_date_nodash(kwargs, period)

    return (kwargs['execution_date'] + timedelta(days=7)).strftime('%Y%m%d') # Interval unclear

def insert_file_date_function(template):
    def out(ds, kwargs)
        ds_nodash = get_file_date_nodash(kwargs)
        return template.format(
            ds_nodash[0:4],
            ds_nodash[4:6],
            ds_nodash[6:8]
        )
    return out

def insert_file_date(template, ds, kwargs):
    return insert_file_date_function(template)(ds, kwargs)

def insert_execution_date_function(template):
    def out(ds, kwargs):
        return template.format(
            kwargs['ds_nodash'][0:4],
            kwargs['ds_nodash'][4:6],
            kwargs['ds_nodash'][6:8]
        )
    return out

def insert_execution_date(template, ds, kwargs):
    return insert_execution_date_function(template)(ds, kwargs)

# some other functions

def get_file_date_nodash(kwargs):
    # TODO: Updat ethis to the correct date
    return (kwargs['execution_date']).strftime('%Y%m01')


def insert_formatted_file_date_function(template):
    def out(ds, kwargs):
        return template.format(get_file_date_nodash(kwargs))
    return out

def get_date(kwargs):
    return kwargs['ds_nodash']


def insert_formatted_date_function(template):
    def out(ds, kwargs):
        return template.format(get_date(kwargs))
    return out


def get_formatted_datetime(ds, kwargs):
    return kwargs['ti'].xcom_pull(dag_id = DAG_NAME, task_ids = 'get_datetime', key = 'file_datetime')


def insert_formatted_regex_function(template):
    def out(ds, kwargs):
        return template.format('\d{5}', get_file_date_nodash(kwargs))
    return out


def insert_current_date(template, kwargs):
    ds_nodash = get_file_date_nodash(kwargs)
    return template.format(
        ds_nodash[0:4],
        ds_nodash[4:6],
        ds_nodash[6:8]
    )


def insert_current_date_function(template):
    def out(ds, kwargs):
        return insert_current_date(template, kwargs)
    return out
