Simple urbandictionary.com crawler app.
For educational purposes only!
Compile: 
scalac -cp lib/play-json_2.11.jar:lib/play-iteratees_2.11.jar Crawlerv2.scala
Run:
scala -cp lib/*:. -Dscala.concurrent.context.numThreads=desiredNumberOfTread -Dscala.concurrent.context.maxThreads=desiredNumberOfTread CrawlerTest desiredLetterToCrawl

The results are stored in current/dict directory

