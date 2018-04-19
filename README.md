About
=====

A program which finds the Document Urls from the given Publication Page Urls.<br/>

It takes as input a JSON file with the pubPages and gives an output in JSON, which contains the pubPages with their docUrls and a comment, which is either empty, or it contains the docFileName (if we have set to store the dcoFiles), or, if there was any error which prevented the discovery of the docUrl, it contains the errorCause.<br/>

PubPage: *the web page with the publication's data.*<br/> 
DocUrl: *the url of the document file.*<br/>

It currently uses [crawler4j](https://github.com/yasserg/crawler4j) to crawl the web pages.<br/>
Many customizations were developed to make the program run efficiently, including advanced blocking techniques.<br/>

It uses an experimental, newly-developed **M.L.A.(= Machine Learning Algorithm)**, which tries to guess the docUrl of a pubPage, based on previous findings.<br/>

Please note that **DocUrlsRetriever** is currently in **beta**, so you may encounter some issues.<br/>
<br/>

Install & Run (using MAVEN)
---------------------------

To install the application, navigate to the directory of the project, where the ***pom.xml*** is.<br/>
Then enter this command in the terminal:<br/>
``mvn install``<br/>

To run the application you should navigate to the ***target*** directory, which will be created by *MAVEN* and run the ***.jar*** file.<br/> 

**Run with standard input/output and logging in a *log file*:**<br/>
``java -jar doc_urls_retriever-0.2-SNAPSHOT.jar < 'inputFile' > 'outputFile'``<br/>

<br/>

**Run with test input/output files and logging in *Console*:**<br/>
- Inside ***DocUrlsRetriever.java***, change code from ***standard input/output*** to ***testing input/output*** and give the wanted testInputFile.<br/>
- Inside ***CrawlerController()*** , choose the wanted input-handling method: either ***UrlUtils.loadAndCheckIdUrlPairs()*** or the ***UrlUtils.loadAndCheckUrls()*** .<br/>
- Inside ***logback.xml***, change the ***appender-ref***, from ***File*** to ***Console***.<br/>
- Execute the program with the following command:<br/>
``java -jar doc_urls_retriever-0.2-SNAPSHOT.jar``