#!/bin/bash

# get the score
find . -name '*.py' | xargs python3 -m pylint --rcfile=/etc/pylint/pylint.rc  | grep 'Your code' | sed -e 's/Your\ code\ has\ been\ rated\ at\ //' -e 's/\/.*//' > pylint_output.txt
cat pylint_output.txt grep 'Your code' | sed -e 's/Your\ code\ has\ been\ rated\ at\ //' -e 's/\/.*//'
pylint_score=$(cat pylint_output.txt grep 'Your code' | sed -e 's/Your\ code\ has\ been\ rated\ at\ //' -e 's/\/.*//')

echo "Got pylint score of $pylint_score"

# shoot it into datadog
echo "pylint.score.dewey:${pylint_score}|g"| nc -v -q2 -u localhost 8125
