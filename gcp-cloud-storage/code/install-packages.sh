#!/bin/bash
# install-packages.sh

# Install pip if it's not already installed
if ! command -v pip &> /dev/null; then
    easy_install pip
fi

# Install pymongo and pandas
pip install pymongo pandas
