import java.net.{URL, HttpURLConnection}
import java.io.{PrintWriter, FileInputStream, InputStreamReader, BufferedReader, DataOutputStream}
import scala.util.{Try, Success, Failure}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.core.`type`.TypeReference
import java.util.Map
import scala.collection.JavaConversions._
import scala.beans._
import java.text.Normalizer
import scala.util.control.Breaks._

class Score(
  @BeanProperty var positive: String,
  @BeanProperty var negative: String
  ) {

  def this() {
    this(null, null)
  }
}

class Term(
  @BeanProperty var name: String, 
  @BeanProperty var definition: String,
  @BeanProperty var example: String,
  @BeanProperty var author: String,
  @BeanProperty var date: String,
  @BeanProperty var score: Score,
  @BeanProperty var name_na: String
  ) {

  def this() {
    this(null, null, null, null, null, null, null)
  }
}

object ESPopulator {
  def manOf[T: Manifest](t: T): Manifest[T] = manifest[T]

  def main(args: Array[String]) {
    val letters = args(0)

    val mapper = new ObjectMapper();

    //val letters = "ABCDEFGHIJKLMNOŒPQRSTUVWXYZ"
    for(l <- letters) {
      var global_counter = 0
      var counter = 0
      var fail_count = 0
      breakable { for (i <- args(1).toInt to args(2).toInt) {
        var local_counter = 0

        val calc = Try {
          val fileName = s"dict/$l/$l$i.json"

          val input = new FileInputStream(fileName);

          val seqOfManyTerms: List[Map[String, java.util.ArrayList[Term]]] = 
            try 
              collectionAsScalaIterable(
                mapper.readValue(input, 
                  new TypeReference[java.util.ArrayList[java.util.Map[String, java.util.ArrayList[Term]]]](){}
                  )
              ).toList
            finally input.close()

          val allTerms = seqOfManyTerms.flatMap { case defsOfOneTerm: Map[String, java.util.ArrayList[Term]] =>
            val term = defsOfOneTerm.keys.toList(0)

            val defs: List[java.util.ArrayList[Term]] = defsOfOneTerm.values.toList

            defs.map(d => collectionAsScalaIterable(d)).flatten.map{ d => 
              val name = Normalizer.normalize(d.name, Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", "")

              new Term(
                name,
                Normalizer.normalize(d.definition, Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", ""),
                Normalizer.normalize(d.example, Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", ""),
                Normalizer.normalize(d.author, Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", ""),
                d.date,
                d.score,
                name)
              }
          }

          val bulkBody = allTerms.foldRight(""){case (term: Term, acc:String) => 
            acc + "{index: {}}\n" + mapper.writeValueAsString(term) + "\n"
          }
          
          val toSend = bulkBody + "\n"

          // println(toSend)

          //Create connection
          val url = new URL("http://localhost:9200/pirate_dictionary/dict/_bulk")
          val connection = url.openConnection().asInstanceOf[HttpURLConnection]
          connection.setRequestMethod("POST")
          connection.setRequestProperty("Content-Type", "application/json")
          connection.setDoInput(true)
          connection.setDoOutput(true)
          val os = new DataOutputStream(connection.getOutputStream)
          os.writeBytes(toSend)
          os.flush
          os.close

          local_counter += allTerms.length
          counter += 1
          // println(connection.getResponseCode)
          // println(connection.getResponseMessage)
          connection.getResponseCode
        }

        println(s"Page $counter contains $local_counter words")
        global_counter += local_counter

        calc match {
          case Success(s) => 
            counter += 1
          case Failure(e) => 
            fail_count += 1
            if(fail_count > 20) break
        }
      }
      }
      
      println(s"Success $counter")
      println(s"Fail $fail_count")
      println(s"There are $global_counter words for $l")
    }
  }
}