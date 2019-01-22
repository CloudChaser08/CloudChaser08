#!/bin/bash

# get the score
pylint_score=$(find . -name '*.py' | xargs python3 -m pylint --rcfile=/etc/pylint/pylint.rc  | grep 'Your code' | sed -e 's/Your\ code\ has\ been\ rated\ at\ //' -e 's/\/.*//')

# shoot it into datadog
echo "pylint.score.dewey:${pylint_score}|g"| nc -v -q2 -u localhost 8125
