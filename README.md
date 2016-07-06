
# Dewey

General data importing and exporting routines

## Environment

Please have the following environment variables:

- AWS_ACCESS_KEY_ID
- AWS_SECRET_ACCESS_KEY
- RS_HOST
- RS_PORT
- RS_DB
- RS_USER
- RS_PASS

## Run

Runtime follows this pattern:

```Shell
% ./dewey *provider* *command*
```

For example, to run all targets (ie download and update the tables in redshift
for icd10)

```Shell
% ./dewey icd10 all
```

## Data Providers

Providers are any sources of data we might have both external and internal. 



