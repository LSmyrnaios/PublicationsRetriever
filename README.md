About
=====

A program which finds the Document Urls from the given Publication Page Urls.<br/>

It takes as input a JSON file with the pubPages and gives an output in JSON, which contains the pubPages with their docUrls and the errorCause, if any error prevented the discovery of the docUrl.<br/>

PubPage: *the web page with the publication's data.*<br/> 
DocUrl: *the url of the document file.*<br/>

It currently uses [crawler4j](https://github.com/yasserg/crawler4j) to crawl the web pages.<br/>
Many customizations were developed to make the program run efficiently, including advanced blocking techniques.<br/>

It uses an experimental **M.L.A.(= Machine Learning Algorithm)**, which tries to guess the docUrl of a pubPage, based on previous findings.<br/>

Please note that **DocUrlsRetriever** is currently in **beta**, so you may encounter some issues.<br/>
<br/>

Install & Run (using MAVEN)
---------------------------

To install the application, navigate to the directory of the project, where the ***pom.xml*** is.<br/>
Then enter this command in the terminal:<br/>
``mvn install``<br/>

To run the application you should navigate to the ***target*** directory, which will be created by *MAVEN* and run the ***.jar*** file.<br/> 

**Run with standard input/output and logging in a *log file*:**<br/>
``java -jar doc_urls_retriever-0.1.1-SNAPSHOT.jar < 'inputFile' > 'outputFile'``<br/>

<br/>

**Run with test input/output files and logging in *Console*:**<br/>
- Change from ***standard input/output*** to ***testing input/output*** code inside ***DocUrlsRetriever.java***.<br/>
- Change urls' loading from ***FileUtils.getNextUrlGroupFromJson()*** to ***FileUtils.getNextUrlGroupTest()*** , in ***UrlUtils.loadAndCheckUrls()*** .<br/>
- Change ***appender-ref***, in ***logback.xml***, from ***File*** to ***Console***.<br/>
- Execute the program with the following command:<br/>
``java -jar doc_urls_retriever-0.1.1-SNAPSHOT.jar``