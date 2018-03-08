*About*
=============

A program which finds the Document Urls from the given Publication Page Urls.<br/>

It takes as input a JSON file with the pubPages and gives an output in JSON, which contains the pubPages with their docUrls and the errorCause, if any error prevented the discovery of the docUrl.<br/>

PubPage: *the web page with the publication's data.*<br/> 
DocUrl: *the url with the document file.*<br/>

It currently uses [crawler4j](https://github.com/yasserg/crawler4j) to crawl the web pages.<br/>
Many customizations were developed to make the program run efficiently, including advanced blocking techniques.<br/>

It uses an experimental **M.L.A.(= Machine Learning Algorithm)**, which tries to guess the docUrl of a docPage, based on previous findings.<br/>

Please note that **DocUrlsRetriever** is currently in **beta**, so you may encounter some issues.
