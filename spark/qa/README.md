Automated QA Framework
-

A framework for running automated QA checks on normalized data


Examples
---

The `examples` directory contains working QA routines.


To run an example, simply call the script with `python`:
```sh
python examples/emdeon.py
```

Note: Dewey must be on your PYTHONPATH


Adding Checks
---

- Pytest will look in the `checks` directory for checks.

- Check files must end with `_test`

- Check functions must begin with `test_`

- New checks should be added to the `conf.datatype_config` dictionary for any relevant datatypes