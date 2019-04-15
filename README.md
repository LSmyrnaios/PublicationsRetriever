#DocUrlsRetriever    [![Build Status](https://travis-ci.com/LSmyrnaios/DocUrlsRetriever.svg?branch=master)](https://travis-ci.com/LSmyrnaios/DocUrlsRetriever)

A Java-program which finds the Document Urls from the given Publication-Web-Pages and downloads the docFiles.<br/>
It is being developed for the European organization: [OpenAIRE](https://www.openaire.eu/).<br/>

The **DocUrlsRetriever** takes as input the PubPages with their IDs -in JSON format- and gives an output -also in JSON format,
which contains the IDs, their PubPages with their DocUrls and a comment, which is either empty,
or it contains the DocFileName or the DocFilePath (if we have set to download-&-store the DocFiles), otherwise,
if there was any error which prevented the discovery of the DocUrl, it contains the ErrorCause (in that case, the DocUrl is set to "unreachable").<br/>

PubPage: *the web page with the publication's information.*<br/> 
DocUrl: *the url of the fulltext-document-file.*<br/>

If there are no IDs in the input, the user can specify -in ***LoaderAndChecker.java***- that wishes to process a data-set with no IDs.<br/>

An experimental **M.L.A.(= Machine Learning Algorithm)** was developed to predict the DocUrl of a PubPage, based on previous results.<br/>
<br/>
**Disclaimers**:
- This program was designed to be used with distributed execution, thus it was developed as a single-thread program.<br/>
- Keep in mind that it's best to run the program for a small set of urls (a few hundred maybe) at first,
    in order to see how it's operating and which parameters work best for you (url-timeouts, domainsBlocking ect.).
- Please note that **DocUrlsRetriever** is currently in **beta**, so you may encounter some issues.
    If so, please send an email or open a Github-Issue.<br/>
- I would be happy to merge any pull requests which is aligned with the [OpenAIRE](https://www.openaire.eu/) needs
    and pushes this software to a better future.<br/>
<br/>

##Install & Run (using MAVEN)
To install the application, navigate to the directory of the project, where the ***pom.xml*** is located.<br/>
Then enter this command in the terminal:<br/>
``mvn install``<br/>

To run the application you should navigate to the ***target*** directory, which will be created by *MAVEN* and run the executable ***JAR*** file,
while choosing the appropriate run-command.<br/> 

**Run with standard input/output:**<br/>
``java -jar doc_urls_retriever-0.4-SNAPSHOT.jar arg1:'-downloadDocFiles' arg2:'-firstDocFileNum' arg3:'NUM' arg4:'-docFilesStorage'
arg5:'storageDir' < stdIn:'inputJsonFile' > stdOut:'outputJsonFile'``<br/>

**Run tests with custom input/output:**<br/>
- Inside ***pom.xml***, change the **mainClass** of **maven-shade-plugin** from "**DocUrlsRetriever**" to "**TestNonStandardInputOutput**".
- Inside ***src/test/.../TestNonStandardInputOutput.java***, give the wanted testInput and testOutput files.<br/>
- If you want to provide a .tsv or a .csv file with a title in its column,
    you can specify it in the **FileUtils.skipFirstRow**-variable inside **FileUtils.java**, in order to be ignored.
- If you want to see the logging-messages in the *Console*, open the ***resources/logback.xml***
    and change the ***appender-ref***, from ***File*** to ***Console***.<br/>
- Run ``mvn install`` to create the new ***JAR*** file.<br/>
- Execute the program with the following command:<br/>
``java -jar doc_urls_retriever-0.4-SNAPSHOT.jar arg1:'-downloadDocFiles' arg2:'-firstDocFileNum' arg3:'NUM' arg4:'-docFilesStorage' arg5:'storageDir'``

**Arguments explanation:**<br/>
- **-downloadDocFiles** will tell the program to download the DocFiles.
    The absence of this argument will cause the program to NOT download the docFiles, but just to find the DocUrls instead.
    Either way, the DocUrls will be written to the JsonOutputFile.
- **-firstDocFileNum** and **NUM** will tell the program to use numbers as DocFileNames and the first DocFile will have the given number "*NUM*"
    The absence of this argument-group will cause the program to use the original-docFileNames.
- **-docFilesStorage** and **storageDir** will tell the program to use the given DocFiles-*storageDir*.
    The absence of this argument will cause the program to use a pre-defined storageDir which is: "*./docFiles*".

##Examples
- You can check the functionality of **DocUrlsRetriever** by running this example:
``java -jar doc_urls_retriever-0.4-SNAPSHOT.jar -downloadDocFiles -firstDocFileNum 1 -docFilesStorage ../example/sample_output/DocFiles
    < ../example/sample_input/sample_input.json > ../example/sample_output/sample_output.json``
This command will run the program with "**../example/sample_input/sample_input.json**" as input
and "**../example/sample_output/sample_output.json**" as the output.</br>
The arguments used are:
    - **-downloadDocFiles** which will tell the program to download the DocFiles.
    - **-firstDocFileNum 1** which will tell the program to use numbers as DocFileNames and the first DocFile will have the number <*1*>.
    - **-docFilesStorage ../example/sample_output/DocFiles** which will tell the program to use the custom
        DocFilesStorageDir: "*../example/sample_output/DocFiles*".

##Customizations
- You can set **File-related** customizations in ***FileUtils.java***.
- You can set **Connection-related** customizations in ***HttpConnUtils.java*** and ***ConnSupportUtils.java***.
- You can test the experimental-**MachineLearning-algorithm** by enabling it (through **useMLA**-variable) from ***MachineLearning.java***.
