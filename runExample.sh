#!/usr/bin/env bash

# Do a clean install
mvn clean install

# Remove any previous example-results.
rm -rf example/sample_output/*

# Run the program.
cd target || exit

command="java -jar publications_retriever-0.4-SNAPSHOT.jar -retrieveDataType all -downloadDocFiles -firstDocFileNum 1 -docFilesStorage ../example/sample_output/DocFiles < ../example/sample_input/sample_input.json > ../example/sample_output/sample_output.json"
echo -e "\nRunning: $command\n"

# Unfortunately, the plain "$command" does not work ,so we have to re-type the commend..

java -jar publications_retriever-0.4-SNAPSHOT.jar -retrieveDataType all -downloadDocFiles -firstDocFileNum 1 -docFilesStorage ../example/sample_output/DocFiles < ../example/sample_input/sample_input.json > ../example/sample_output/sample_output.json

echo "Finished"
