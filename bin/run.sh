#!/bin/bash

cd providers/$1 && make ${@:2}
