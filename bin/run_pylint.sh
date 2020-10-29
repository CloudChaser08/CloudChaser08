#!/bin/bash

# run pylint and get the score
find . -name '*.py' | xargs python3 -m pylint --rcfile=/etc/pylint/pylint.rc  | grep 'Your code' | sed -e 's/Your\ code\ has\ been\ rated\ at\ //' -e 's/\/.*//' > pylint_output.txt
pylint_score=$(cat pylint_output.txt)

echo "Got pylint score of $pylint_score"

# shoot it into datadog
echo "pylint.score.dewey:${pylint_score}|g"| nc -w5 -v -q2 -u localhost 8125
