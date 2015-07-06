import java.net.{URL, URLDecoder}
import java.io.PrintWriter
import scala.io.BufferedSource
import scala.concurrent._
import scala.util.{Success, Failure}
import ExecutionContext.Implicits.global
import scala.collection.mutable.ArrayBuffer
import play.api.libs.json._

case class Term(name: String, definition: String, example: String, author: String, date: String, score: (String, String))

object Crawler {

  private val wordListPat = """<div id='columnist'>\n<ul class='no-bullet'>(?:.*\n)*?<\/ul>"""
  private val refPat = """<a (?:.*)href="(.*)">.*<\/a>"""
  private val termNamePat = """term=(.*)"""

  private val meanExPat = """<div class='meaning'>((?:.*\n)*?)<\/div>\n<div class='example'>((?:.*\n)*?)<\/div>"""
  private val authorDatePat = """\n<div class='contributor'>\nby <a class="author"(?:.*)>(.*)<\/a>\n(.*)\n<\/div>"""
  private val scorePat = """(?:.*\n)*?<span class='count'>(\d*)<\/span>(?:.*\n)*?<span class='count'>(\d*)<\/span>"""
  private val termPat = (meanExPat + authorDatePat + scorePat).r
  private val specialTagsToReplace = """(<.*?\/>)"""
  private val specialTagsToEliminate = """(&.*?;)"""

  private def lastPat(character: Char) = s"""<a href="\\/browse.php\\?character=$character&amp;page=(\\d*)">Last &raquo;<\\/a>"""

  private var criticalErrorCounter = 1;

  private def getUrlSource(url: String) : Option[String] = Option {
    val urlObj = new URL(url)
    val inBuffSource = new BufferedSource(urlObj.openStream)
    inBuffSource.foldRight("")((c, s) => c + s)
  } 

	private def getListOfTermDefsByName(name: String, counter: Int, maxWordInPage: Int): Option[List[Term]] = {
    println(s"${100 * counter/maxWordInPage}% process: http://www.urbandictionary.com/define.php?term=$name")
    getUrlSource(s"http://www.urbandictionary.com/define.php?term=$name").map { source => 
      val cleanedSource = source.replaceAll(specialTagsToReplace, " ")
                                .replaceAll(specialTagsToEliminate, "")
                                .replaceAll("\r", "")
                                .replaceAll("\t", "")
      
      termPat.findAllMatchIn(cleanedSource).toList.map { m => 
        val definition = m.group(1).replaceAll("\n", "").replaceAll("""define.php\?term=""", """define\?term=""")
        val example = m.group(2).replaceAll("\n", "").replaceAll("""define.php\?term=""", """define\?term=""")
        new Term(name, definition, example, m.group(3), m.group(4), (m.group(5), m.group(6)))
      }
    }
  }

  private def getListOfTermDefsByURL(url: String, counter: Int, maxWordInPage: Int): Option[(String, List[Term])] = {
    termNamePat.r.findFirstMatchIn(url).flatMap { m => 
      val name = URLDecoder.decode(m.group(1), "UTF-8")
      getListOfTermDefsByName(name, counter, maxWordInPage).map(name -> _)
    }
  }
    
  private def getListOfRef(character: Char, page: Int): Option[List[String]] = 
    getUrlSource(s"http://www.urbandictionary.com/browse.php?character=$character&amp;page=$page").map { source => 
      refPat.r.findAllMatchIn(wordListPat.r.findFirstIn(source).get).toList.map(m => m.group(1))
    }

  private implicit val termListWrites = new Writes[List[Term]] {
    def writes(terms: List[Term]): JsValue = {
      
      terms.foldRight(JsArray(Seq())) {
        case(term, acc) => 
          acc :+ JsObject(Seq(
          "name" -> JsString(term.name),
          "definition" -> JsString(term.definition),
          "example" -> JsString(term.example),
          "author" -> JsString(term.author),
          "date" -> JsString(term.date),
          "score" -> JsObject(Seq(
            "positive" -> JsNumber(term.score._1.toInt),
            "negative" -> JsNumber(term.score._2.toInt)
            ))
          ))
      }
    }
  }

  def getMaxPageNumberForChar(character: Char): Option[Int] = 
    getUrlSource(s"http://www.urbandictionary.com/browse.php?character=$character&amp;page=1").flatMap { source => 
      lastPat(character).r.findFirstMatchIn(source).map(_.group(1).toInt - 20)
    }

  // val alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
  // getMaxPageNumberForChar(c).getOrElse(1))
  def process(c: Char, start: Int, end: Int ) = {
    println(s"process for $c from $start to $end")
    val result: List[String] = (for (i <- start to end) yield {
      println(s"process page $i for $c")

      val maybeRefList = getListOfRef(c, i)

      if (maybeRefList.isEmpty) {
        println(s"ERROR: page for character $c and with number $i is not found")
        criticalErrorCounter += 1
        if (criticalErrorCounter > 10) {
          println(s"ERROR: we have reached 10 critical error -> EXIT")
          System.exit(1) 
        }
        s"ERROR: page for character $c and with number $i is not found"
      } else {

        var counter = 0
        val refList = maybeRefList.get
        var maxWordInPage = refList.toArray.length
        val terms: List[JsObject] = refList.map { url => 
          counter += 1
          val maybeListOfTerm = getListOfTermDefsByURL(url, counter, maxWordInPage)
          if (maybeListOfTerm.isEmpty) {
            println(s"WARN: for $url definition is not found")
            JsObject(Seq())
          } else {
            val listOfTerm = maybeListOfTerm.get
            JsObject(Seq(listOfTerm._1 -> Json.toJson(listOfTerm._2)))
          }
        }

        val json = JsArray(terms)
        val fileIsCreated = Option(new PrintWriter(s"dict/$c$i.json")).map{ p => 
          p.write(json.toString)
          p.close
          true
        }

        val res = s"Is dict/$c$i.json created? -> ${fileIsCreated.getOrElse(false)}\n"
        print(res)

        res
      }
    }).toList
    
    println(s"process for $c from $start to $end is finished")

    result
  }
}

object CrawlerTest {

  def main(args: Array[String]) {
    var listOfFutures: ArrayBuffer[Future[List[String]]] = new ArrayBuffer()

    val c = args(0)
    val resLog = new PrintWriter(s"result.log")

    val maxPageNumber = Crawler.getMaxPageNumberForChar(c).getOrElse(-1)
    println(s"The total amount of pages is $maxPageNumber")
    for (i <- 8 to maxPageNumber) listOfFutures += Future { 
      Crawler.process(c, i, i)
    }

    var successCounter = 0
    
    while(!listOfFutures.isEmpty) {
      listOfFutures.foreach { future =>
        if(future != null && future.isCompleted) { future.value.get match {
          case Success(list) =>
            successCounter += 1
            list foreach resLog.write _
          case Failure(t) => println("An error has occured: " + t.getMessage)
          }
          listOfFutures -= future
        }
      }
      //checks which futures are completed every half-a-second
      Thread.sleep(500)
    }

    resLog write s"The number of successed page is $successCounter and total amount of pages is $maxPageNumber"
    resLog.close
  }
}