## Program's execution process

### FullText files downloading

During the program's execution, the following steps take place:

1) The given command-line-arguments are validated and the related variables are set.
2) The input-json-file is parsed and the id-url pairs are loaded in batches.
3) Each id-url pair is added to the "callableTasksList" and then is processed by one of the running threads.
4) For each id-url, the url is checked if it matches to some problematic url (which does not give the landing-page of the publication nor the full-text (or dataset) itself).
5) Each url gets ready to be connected. If it belongs to a "special-case" domain then a dedicated handler is applied and transforms it either to a full-text url or to a "workable" landing-page.
6) If the url gives a full-text (or dataset) file directly (even after redirections), the file will be saved immediately (if such option is chosen) and the results will be queued to be written to the disk in due time.
7) If the url leads to a web-page, the following steps take place:
   - The html of the page is downloaded.
   - The file-url that may be presented in the ***< meta >*** (metadata) tags is checked for whether it actually gives the file.
   - If the above step does not succeed, then the internal links are extracted from the page.
     - During the links-extraction process, the structure of the html-tags leading to each url is studied and if it matches to previously stored structure of a file-url, then it's possible to predict the file-url in the page, without coming across it.
     - Also, some likely-to-be-fulltext-links (based on text-mining of the surrounding text) are checked immediately.
     - Additionally, some invalid docUrls or irrelevant urls are identified and blocked before a chance is given to them be connected. 
   - After the extraction process, in case no file-url was identified, we loop through the internal-links list, identify some potential docOrDataset urls and check those first, before checking the rest.
   - If a file-url is verified, step 6 takes place. Otherwise, the negative result will be checked for its "re-triable status" and written to the disc in due time.
8) Once all records of a batch are processed the results of those records are written to the output-file.
9) Once all batches have finished being processed, the program exits.


### HTML files downloading

In case the program receives the "-downloadJustHtmlFiles" argument, it proceeds with downloading the HTML files, ignoring all other cases.<br>
The execution is the same for steps 1 to 5.<br>
Then, for step-6: If the url is a fulltext one, then it simply gets logged, not downloaded.<br>
On the contrary, if it is a landing-page, then the HTML is downloaded and stored. No meta-checks, nor internal-links-extraction takes place.<br>
Then steps 8 and 9 of the fulltext-plan take place.<br>
