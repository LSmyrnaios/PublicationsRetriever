DocUrlsRetriever
================
A program which finds the Document Urls from the given Publication-Web-Pages and downloads the docFiles.<br/>
It has been developed and maintained under the European project: [OpenAIRE](https://www.openaire.eu/).<br/>

The **DocUrlsRetriever** takes as input the PubPages -in JSON format- and gives an output -also in JSON format, which contains the PubPages with their DocUrls and a comment, which is either empty, or it contains the DocFileName or the DocFilePath (if we have set to download-&-store the DocFiles), otherwise, if there was any error which prevented the discovery of the DocUrl, it contains the ErrorCause.<br/>

PubPage: *the web page with the publication's data.*<br/> 
DocUrl: *the url of the document file.*<br/>

An experimental **M.L.A.(= Machine Learning Algorithm)** was developed to predict the DocUrl of a PubPage, based on previous results.<br/>

Please note that **DocUrlsRetriever** is currently in **beta**, so you may encounter some issues.<br/>
Keep in mind that it's best to run the program for a small set of urls (a few hundred maybe) at first, in order to see how it's operating and which parameters work best for you (timeouts, runOfMLA, domainsBlocking ect.).

Install & Run (using MAVEN)
---------------------------
To install the application, navigate to the directory of the project, where the ***pom.xml*** is located.<br/>
Then enter this command in the terminal:<br/>
``mvn install``<br/>

To run the application you should navigate to the ***target*** directory, which will be created by *MAVEN* and run the executable ***JAR*** file.<br/> 
*Note: If you choose to download the docFiles and store them with numbers as their names, you can set the number of the first DocFile as an argument. Although this argument is optional and the number <1> will be the default number.*<br/>

**Run with standard input/output and logging in a *log file*:**<br/>
``java -jar doc_urls_retriever-0.3-SNAPSHOT.jar <arg:firstDocFileNum> < 'stdIn:inputFile' > 'stdOut:outputFile'``<br/>

**Run with non-standard input/output:**<br/>
- Inside ***DocUrlsRetriever.java***, change the code from ***standard input/output*** to ***testing input/output*** and give the wanted testInputFile.<br/>
- Inside ***CrawlerController()*** , choose the wanted input-handling method: either ***LoadAndCheckUrls.loadAndCheckIdUrlPairs()*** or the ***LoadAndCheckUrls.loadAndCheckUrls()*** .<br/>
- If you want to see the logging-messages in the *Console*, open the ***resources/logback.xml*** and change the ***appender-ref***, from ***File*** to ***Console***.<br/>
- Execute the program with the following command:<br/>
``java -jar doc_urls_retriever-0.3-SNAPSHOT.jar <arg:firstDocFileNum>``

Customizations
--------------
- You can set **file-related** customizations (including the ability to store the DocFiles) in ***FileUtils.java***.
- You can set **connection-related** customizations in ***HttpConnUtils.java*** and ***ConnSupportUtils.java***.
