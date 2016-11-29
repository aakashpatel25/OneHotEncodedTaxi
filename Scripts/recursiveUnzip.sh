#/bin/bash

# Unzip all the files in a given folder recusrvicely.

find . -name "*.zip" -exec unzip {} \;