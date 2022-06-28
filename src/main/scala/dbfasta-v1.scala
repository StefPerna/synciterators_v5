

package proteomics

/** Version 1, to be used with dbmodel.DBFile Version 9.
 *
 *  Wong Limsoon
 *  27 February 2022
 */


object FASTAModel {

/** A simple model to turn FASTA files into ordered collection. 
 *  Ordered collection comes with Synchrony iterator, which provides
 *  efficient general synchronized iteration over multiple collections.
 */


  import scala.language.implicitConversions

  type Bool = Boolean
  type META = Map[String,Any]
  type ID   = Int

  def TMP = dbmodel.DBFile.TMP    // Default folder to use.


  case class FASTAEntry(id: ID, name: String, seq: String, meta: META)
  {
    /** Update fields
     */

    def newId(id: ID): FASTAEntry        = FASTAEntry(id, name, seq, meta)

    def newName(name: String): FASTAEntry = FASTAEntry(id, name, seq, meta)

    def newSeq(seq: String): FASTAEntry   = FASTAEntry(id, name, seq, meta)

    def newMeta(meta: META): FASTAEntry   = FASTAEntry(id, name, seq, meta)

    def newMeta(k: String, v: Any): FASTAEntry = (k, v) match {
      case ("id", v: ID)       => newId(v)
      case ("name", v: String) => newName(v)
      case ("seq", v: String)  => newSeq(v)
      case _ => newMeta(meta + (k -> v))
    }

    def ++(fields: (String, Any)*): FASTAEntry = {
      val kv = Map(fields: _*)
      val id = kv.getOrElse("id", this.id).asInstanceOf[ID]
      val nm = kv.getOrElse("name", this.name).asInstanceOf[String]
      val sq = kv.getOrElse("seq", this.seq).asInstanceOf[String]
      val mi = kv -- Vector("id", "name", "seq")
      FASTAEntry(id, nm, sq, meta ++ mi)
    }

    def ++(meta: META): FASTAEntry = newMeta(this.meta ++ meta)


    /** Check whether a field is present
     */

    def hasMeta(k: String): Bool = meta.contains(k)

    def checkMeta[A](k: String, check: A => Bool, error: Bool = false): Bool =
      try check(getMeta(k)) 
      catch { case _: Throwable => error }


    /** Retrieve a field
     */

    def apply(k: String): Any     = getMeta[Any](k)

    def getInt(k: String): Int    = getMeta[Int](k)

    def getDbl(k: String): Double = getMeta[Double](k)

    def getStr(k: String): String = getMeta[String](k)

    def getMeta[A](k: String): A  = {
      val v = meta.get(k) match {
        case Some(a) => a
        case None    => k match {
          case "id"   => id
          case "name" => name
          case "seq"  => seq
          case _      => throw new java.util.NoSuchElementException()
        }
      }
      v.asInstanceOf[A]
    }


    /** Delete and replace fields
     */

    def delMeta(k: String): FASTAEntry = newMeta(meta - k)

    def delMeta(): FASTAEntry          = newMeta(meta.empty)

  }  // end case class FASTAEntry


  object FASTAEntry
  {
    /** Alternative constructors for [[FASTAEntry]], autofilling some fields.
     */

    def apply(name: String, seq: String): FASTAEntry =
      FASTAEntry(0, name, seq, Map())

    def apply(name: String, seq: String, meta: META): FASTAEntry =
      FASTAEntry(0, name, seq, meta)

  }  // end object FASTAEntry




  import dbmodel.DBModel.CloseableIterator.CBI
  import dbmodel.DBModel.OrderedCollection.{ OColl, Key }
  import dbmodel.DBFile.{ OFile, OCollToFile, OTransient, PARSER, FORMATTER }

    
  type FASTAITERATOR  = CBI[FASTAEntry]

  type FASTAFILE      = OFile[FASTAEntry,ID]

  type FASTA[K]       = OFile[FASTAEntry,K]

  type SCHEMA         = Vector[(String, String => Any)]
                           // FASTA file re not self-describing.
                           // Schemas are a way for users to tell which
                           // fields correspond to what. An entry "f -> get"
                           // in a schema vector says the field name is "f",
                           // and a string representing the value of "f" can
                           // be parsed using the funtion "get".

  implicit class OCollToFASTAFile[K](entries: OColl[FASTAEntry,K])
  {
    def toFASTAFile: FASTA[K] = FASTAFile.transientFASTAFile(entries.cbi)
                                         .assumeOrderedBy(entries.key)
  }


  object FASTAFile
  {
    /** Key for FASTAFile
     */

    val fastaFileKey: Key[FASTAEntry,ID] = Key(_.id, Ordering[Int])


    /** Constructors
     */

    def apply(filename: String): FASTAFILE = FASTAFile(filename, TMP)

    def apply(filename: String, folder: String): FASTAFILE =
    {
      // Assume filename is a FASTA file
      val fname = OFile.mkFileName(filename, folder).toString
      OFile(fastaFileKey, fname, parser, formatter)
    }
 
    def customized(
      filename: String,
      folder: String,
      parser: PARSER[FASTAEntry] = parser,
      formatter: FORMATTER[FASTAEntry] = formatter): FASTAFILE =
    {
      val fname = OFile.mkFileName(filename, folder).toString
      OFile(fastaFileKey, fname, parser, formatter)
    }


    /** Sometimes, it is handy to have a transient FASTA file for
     *  temporary use without writing it to disk.
     */

    def transientFASTAFile(entries: IterableOnce[FASTAEntry]): FASTAFILE = 
      OTransient[FASTAEntry,ID](fastaFileKey, CBI(entries),
                                parser, formatter, OFile.destructorOFile _)
    

    def emptyFASTAFile: FASTAFILE = 
      OTransient[FASTAEntry,ID](fastaFileKey, CBI(),
                                parser, formatter, OFile.destructorOFile _)


    /** Formatter for FASTA file. Normal FASTA file does not use explicit
     *  field name. A header is used here to introduce field name.
     */

    val formatter: FORMATTER[FASTAEntry] = (options: String) =>
    {
      var labels: Seq[String] = null

      def typeOf(e: Any): String = e match {
        case _: Int    => "Int"
        case _: Double => "Double"
        case _ => "String"
      }

      def header(b: FASTAEntry) = labels.isEmpty match {
        case true  => ""
        case false =>
          s"; ##wonglimsoon@nus##\t" +
          labels.map(l => s"${l}:${typeOf(b.meta(l))}").mkString("\t") +
          "\n"
      }

      def entry(b: FASTAEntry) = {
        val nm = b.name
        val sp = if (labels.isEmpty) "" else "\t"
        val lb = labels.map(l => b.meta(l).toString).mkString("\t")
        val sq = b.seq.grouped(80).mkString("\n")
        s">${nm}${sp}${lb}\n${sq}\n"
      }

      def format(b: FASTAEntry, position: Int = 1): String =
      {
        (position == 0) match {
          case true  =>   // 1st entry. Write header if needed.
            labels = b.meta.keys.toSeq.sorted
            s"${header(b)}${entry(b)}"
          case false =>   // Other entries. No need header.
            entry(b)
        }
      }

      format _
    }


    /** Parser for [[FASTAEntry]]. 
     */

    private val getInt = (e: String) => e.toInt
    private val getDbl = (e: String) => e.toDouble
    private val getStr = (e: String) => e
    private val getAny = (e: String) => 
      try e.toInt catch { case _: Throwable =>
      try e.toDouble catch { case _: Throwable => e }
    }
    private val cvt = Map("Int"->getInt, "Double"->getDbl, "String"->getStr)


    private val emptySchema: Vector[(String, String => Any)] = null


    /** Parser for standard FASTA file
     */

    val stdParser: PARSER[FASTAEntry] = parser(emptySchema) 

  
    /** General schema-dependent parser
     */

    def parser(implicit sch: SCHEMA = null): PARSER[FASTAEntry] = 
    (option: String) =>
    {
      var schema: SCHEMA = sch
      var buf: String = ""

      def parse(it: Iterator[String], position: Int = 1): FASTAEntry =
      {
        var line               = ""
        var parsed: FASTAEntry = null
        var continue           = (buf != "") || it.hasNext

        while (continue)
        {
          if (buf == "") { line = it.next() } else { line = buf; buf = "" }

          if (line startsWith "; ##wonglimsoon@nus##") {
            // This line is a header. Define new schema.
            val e = line.split("\t")
            val f = e.toVector.tail
            schema = for (kv <- f; k = kv.split(":")) yield (k(0) -> cvt(k(1)))
            continue = it.hasNext
          } 

          else if (line startsWith ";") {
            // This line is a comment. Skip.
            continue = it.hasNext
          }

          else if (line startsWith ">") {
            // This line is an entry title.
            val hl = line.split("\t")
            val mx = hl.length - 1
            val nm = hl(0).drop(1)
            val mi = {
              val fvs = (schema == null) match {
                case true  => for (i <- 1 to mx)
                            yield (i.toString -> getAny(hl(i)))
                case false => for (i <- 1 to mx; (l, get) = schema(i - 1))
                            yield (l -> get(hl(i)))
              }
              fvs.toMap
            }

            var sq: List[String]   = List()
            var more = it.hasNext
            while (more) {
              val tmp = it.next()
              val end = ((tmp startsWith ";") || 
                         (tmp startsWith ">") || 
                         (tmp.length==0))
              if (end) { buf = tmp } // end of sequence reached.
              else { sq = tmp +: sq }
              more = (!end) && it.hasNext
            }           
            
            parsed   = FASTAEntry(position, nm, sq.reverse.mkString, mi)
            continue = false
          }

          else {
            // Blank line. Skip.
            continue = it.hasNext
          }
        }

        if (parsed == null) { throw new java.io.EOFException("") }
        else { return parsed }
      }

      parse _
    }

  }  // end object FASTAFile


  val implicits = dbmodel.DBFile.implicits

}  // end object FASTAModel
 


/** Examples **********************************************
 *
{{{

import proteomics.FASTAModel.FASTAFile
import proteomics.FASTAModel.implicits._

val fasta = {
  val fname = "Human_database_including_decoys_(cRAP_added).fasta"
  FASTAFile(fname)
}

fasta(9)

fasta(8).seq

fasta(7).name

fasta.length

val wls = fasta.map(s => s.newMeta("len", s.seq.length)).serialized("wls")

wls(8)("len")

wls.protection(false).close()


}}}
 *
 */




