#!/usr/bin/env bash

# Do a clean install
mvn clean install

# Remove any previous example-results.
rm -rf example/sample_output/*

# Run the program.
cd target && java -jar doc_urls_retriever-0.4-SNAPSHOT.jar -downloadDocFiles -firstDocFileNum 1 -docFilesStorage ../example/sample_output/DocFiles < ../example/sample_input/sample_input.json > ../example/sample_output/sample_output.json
