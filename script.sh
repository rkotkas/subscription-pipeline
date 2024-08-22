#!/bin/bash

# Run tests
cd tests
python tests.py
cd ..

# Create the build directory
mkdir -p build

# Copy pipeline.py to the build directory
cp ./dev/pipeline.py ./build

# Run the Python script from the build directory
cd build
python pipeline.py
cd ..

# Check for the version file in the parent directory
if [ -f ./version.txt ]; then
    version=$(head -n 1 ./version.txt)
    echo "Current version: $version"
else
    echo "Version file not found."
fi
