## How to use LTI interface

While completing the programming assignment, you have to submit your assignment. In order to make submission, you have to create a notebook. When you created a Jupyter notebook, in the right top corner you could see a dropdown with tasks and a button "Submit assignment". You have to choose the appropriate task (the task that you have chosen from Coursera Interface) and press the blue button. 

After that you will see a popup window with notification that your task have submitted. 

In some minutes you will get a popup window with submission status: your grade and results from the grading system server.


## Notes

* The sample dataset is located at /data/wiki/en_articles_part.
* The sample dataset of Twitter Graph is located in local FileSystem at /data/twitter.
* The dataset with stop-words is located in local FileSystem on path /datasets.
* The system grades standard output and error streams from the last non-empty cell. 
* If you have clicked "Open tool" and received "404" error, please reload the page.

### Spark assignment notes

You can use Demo notebook as example. Demo notebook parses input dataset, clear out punctuations and other junk symbols and split the text by words.

* In this case you shouldn't worry about stderr. It is enough if you will give only stdout in your notebook.
* You should eliminate extra symbols in output (such as quotes, brackets etc.). Let's see on the Demo notebook. It is easier to print wiki.take(1) instead of traverse RDD using "for" cycle. But in this case a lot of junk symbols will be printed like

[[u'Anarchism',
  u'is',
  u'often',
  u'defined',
  u'as',
...
]]

