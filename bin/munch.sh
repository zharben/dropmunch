#!/bin/bash

cd ..
export PYTHONPATH=.
python3.4 dropmunch/munch_process.py "$@"
