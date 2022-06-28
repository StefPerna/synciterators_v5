

package dbmodel

/** Version 1, to be used with dbmodel.DBFile Version 9.
 *
 *  Wong Limsoon
 *  28 February 2022
 */


object TSVModel {

/** A simple model for tab-delimited files
 */

  import dbmodel.DBModel.OrderedCollection.Key
  import dbmodel.DBModel.CloseableIterator.CBI
  import dbmodel.DBFile.{ OFile, OTransient, PARSER, FORMATTER }

  def TMP   = dbmodel.DBFile.TMP    // Default folder to use.


  type Bool      = Boolean
  type TSV       = Array[Any]
  type FIELDNAME = String
  type FIELDTYPE = String  // Allowed values are "Int", "Double", "String"
  type SCHEMA    = Vector[(FIELDNAME, FIELDTYPE)]



  /** Class for defining TSV database connectivity
   */

  case class TSVdbc[B](
    schema: SCHEMA, 
    encoder: B => TSV, 
    decoder: (TSV, Int) => B)
  {
    val formatter = TSVdbc.genFormatter(schema, encoder)

    val parser    = TSVdbc.genParser(schema, decoder)

    def connect[K](
      key:      Key[B,K],
      filename: String, 
      folder:   String = TMP): OFile[B,K] =
    {
      val fname = OFile.mkFileName(filename, folder).toString
      OFile[B,K](key, fname, parser, formatter)
    }
  }


  object TSVdbc 
  {
    /** Generate a formatter given a schema and an encoder
     */

    def genFormatter[B](
      schema:  SCHEMA, 
      encoder: B => TSV): FORMATTER[B] = (options: String) =>
    {   
      val header      = (schema map { case (f, t) => s"$f:$t" }).mkString("\t")

      def entry(b: B) = encoder(b).map(_.toString).mkString("\t")

      def format(b: B, position: Int): String = (position == 0) match {
        case true  => s"${header}\n${entry(b)}"
        case false => entry(b)
      }

      format _
    }


    /** Generate a parser given a schema and a decoder
     */

    def genParser[B](
      schema: SCHEMA,
      decoder: (TSV, Int) => B): PARSER[B] = (options: String) =>
    {
      val getter = schema map { case (f, t) => TSVSupport.cvt(t) }

      val size   = schema.length

      def parse(it: Iterator[String], position: Int): B = {
        if (position == 0 && it.hasNext) { it.next() } // Skip header.
        var line        = ""
        var parsed: TSV = null
        var continue    = it.hasNext
        while (continue) {
          line = it.next()
          val en   = line split "\t"
          parsed   = Array.tabulate(size) { n => getter(n)(en(n)) }
          continue = false
        }
        if (parsed == null) { throw new java.io.EOFException("") }
        else { return decoder(parsed, position) }
      }

      parse _
    }

  }  // End object TSVdbc



  /** Class for more flexibly defining TSV database connectivity 
   */

  case class FlexibleTSVdbc[B](
    schema: SCHEMA, 
    formatter: FORMATTER[B],
    parser: PARSER[B])
  {
    def connect[K](
      key:      Key[B,K],
      filename: String, 
      folder:   String = TMP): OFile[B,K] =
    {
      val fname = OFile.mkFileName(filename, folder).toString
      OFile[B,K](key, fname, parser, formatter)
    }
  }


  object FlexibleTSVdbc 
  {
    /** Generate a formatter given a schema, a header generator,
     *  and an encoder
     */

    def genHeader(schema: SCHEMA): String =
      (schema map { case (f, t) => s"$f:$t" }).mkString("\t")


    def genHeader[B](schema: SCHEMA, b: B): String = genHeader(schema)


    def genFormatter[B](
      schema:    SCHEMA, 
      genHeader: Option[(SCHEMA,B)=>String],
      encoder:   B => TSV): FORMATTER[B] = (options: String) =>
    {   
      def entry(b: B) = encoder(b).map(_.toString).mkString("\t")

      def format(b: B, position: Int): String = (position == 0) match {
        case false => entry(b)
        case true  => genHeader match {
          case Some(header) => s"${header(schema, b)}\n${entry(b)}"
          case None         => entry(b)
        }
      }

      format _
    }


    /** Generate a parser given a schema, a guard, and a decoder
     *  The guard takes the current line, current line #, and
     *  current item #, and decide whether to skip the current line.
     */

    def genParser[B](
      schema:  SCHEMA,
      guard:   Option[(String,Int,Int)=>Bool],
      decoder: (TSV, Int) => B): PARSER[B] = (options: String) =>
    {
      val getter = schema map { case (f, t) => TSVSupport.cvt(t) }
      val size   = schema.length
      var lno    = 0

      def parse(it: Iterator[String], position: Int): B =
      {
        var line        = ""
        var parsed: TSV = null
        var continue    = it.hasNext

        while (continue)
        {
          line = it.next()
          lno  = lno + 1

          val ok = guard match {
            case None    => true
            case Some(g) => g(line, lno, position)
          }

          if (ok) {  
            val en   = line split "\t"
            parsed   = Array.tabulate(size) { n => getter(n)(en(n)) }
            continue = false
          }

          else { continue = it.hasNext }
        }

        if (parsed == null) { throw new java.io.EOFException("") }
        else { return decoder(parsed, position) }
      }

      parse _
    }

  }  // End object FlexibleTSVdbc




  /** [[TSVFile[A<:HasSchema](encoder, decoder, fixedSchema)]] 
   *  provides a mapping between a collection of [[A]] to a [[TSV]] file.
   *  I.e., it uses [[encoder]] to encode each item in the collection
   *  into a TSV, and uses [[decoder]] to decode each TSB into an item.
   *  The [[encoder]]/[[decoder]] may depends on a schema. 
   *  If a [[fixedSchema]] should be used for this purpose, this
   *  argument must have a non-null value; otherwise, it must be set
   *  to [[null]].
   */
    
  trait HasSchema 
  {
    def schema: SCHEMA
  }


  case class TSVFile[A<:HasSchema](
    encoder:  SCHEMA => A => TSV, 
    decoder:  SCHEMA => (TSV,Int) => A,
    fixedSch: SCHEMA = null)
  {
    def apply(filename: String): OFile[A,Unit] = apply(filename, TMP)

    def apply(filename: String, folder: String): OFile[A,Unit]  = {
      val sch = getSchema(filename, folder)
      val key  = Key((x: A) => (), Ordering[Unit])
      TSVdbc(sch, encoder(sch), decoder(sch)).connect(key, filename, folder)
    }

    def getSchema(filename: String, folder: String = TMP): SCHEMA =
      if (fixedSch != null) fixedSch
      else {
        val fname  = OFile.mkFileName(filename, folder).toString
        val file   = scala.io.Source.fromFile(fname)
        val tuple2 = (a: Array[String]) => (a(0), a(1))
        val header = 
          try file.getLines().next().trim()
          catch { case _: Throwable => "" }
        file.close()
        (header split "\t").map(a => tuple2(a.split(":"))).toVector 
    }

    def transientTSVFile[K](
      entries: IterableOnce[A],
      key:     Key[A,K],
      schema:  SCHEMA = fixedSch): OFile[A,K] =
    {
      val cbi  = CBI(entries)
      val sch  = (schema == null) match {
        case false => schema
        case true  => if (cbi.hasNext) cbi.head.schema else Vector()
      }
      val mx   = sch.length - 1
      val formatter = TSVdbc.genFormatter(sch, encoder(sch))
      val parser    = TSVdbc.genParser(sch, decoder(sch))
        OTransient[A,K](key, cbi, parser, formatter, OFile.destructorOFile _)
    }
  }  // End class TSVFile



  object TSVFile
  {
    /** Constructor when encoder/decoder is schema independent
     *  or uses a fixed schema.
     */

    def apply[A<:HasSchema](
      encoder:  A => TSV,
      decoder:  (TSV,Int) => A,
      fixedSch: SCHEMA): TSVFile[A] =
    {
      TSVFile((sch: SCHEMA) => encoder, (sch: SCHEMA) => decoder, fixedSch)
    }
  }  




  object REMYFile
  {
    /** Remy records. Uses a Map to provide named access to TSV.
     */

    case class Remy(remy: Map[String,Int], tsv: TSV) extends HasSchema
    {
      import TSVSupport._

      def apply(f: String) = tsv(remy(f))
      def int(f: String): Int = tsv(remy(f)).asInt
      def dbl(f: String): Double = tsv(remy(f)).asDbl
      def str(f: String): String = tsv(remy(f)).asStr

      def schema: SCHEMA = {
        val inv = remy map { case (x, y) => (y, x) }
        val mx = tsv.length - 1
        (for (i <- 0 to mx) yield (inv(i) -> tsv(i).ty)).toVector
      }
    }

    object Remy 
    {
       /** Alternative constructor. Builds Reny directory from schema.
        */

      def apply(sch: SCHEMA, tsv: TSV): Remy = {
        val remy: Map[String,Int] = {
          val mx = sch.length - 1
          val ix = for (i <- 0 to mx) yield (sch(i)._1 -> i)
          ix.toMap
        }
        Remy(remy, tsv)
      }
    }

    val RemyFile =
    {
      def encoder(sch: SCHEMA) = (a: Remy) => a.tsv

      def decoder(sch: SCHEMA) = { 
        val mx   = sch.length - 1
        val remy = { for (i <- 0 to mx) yield (sch(i)._1 -> i) }.toMap 
        (tsv: TSV, p: Int) => Remy(remy, tsv)
      }
      
      new TSVFile(encoder, decoder)
    }

    def transientRemyFile[K](
      entries: IterableOnce[Remy],
      key: Key[Remy,K],
      schema: SCHEMA = null): OFile[Remy,K] =
    {
      RemyFile.transientTSVFile(entries, key, schema)
    }

  }  // End object REMYFile




  object TSVSupport
  {
    /** Support functions to make it easy for users to convert between
     *  TSV and their own data types.
     */

    val getInt = (e: String) => e.toInt


    val getDbl = (e: String) => e.toDouble


    val getStr = (e: String) => e


    val getAny = (e: String) => 
      try e.toInt catch { case _: Throwable =>
      try e.toDouble catch { case _: Throwable => e }
    }


    val cvt = Map("Int"->getInt, "Double"->getDbl, "String"->getStr)


    implicit class TYPECAST(e: Any) {
      def asInt = e.asInstanceOf[Int]
      def asDbl = e.asInstanceOf[Double]
      def asStr = e.asInstanceOf[String]
      def ty    = e match {
        case _: Int    => "Int"
        case _: Double => "Double"
        case _ => "String"
      }
    }


    implicit class TSVCAST(tsv: TSV) {
      def int(n: Int): Int    = tsv(n).asInt
      def dbl(n: Int): Double = tsv(n).asDbl
      def str(n: Int): String = tsv(n).asStr
    }


    trait TSVBASETYPE[A] {
      val ty:   String
      val cast: Any => A = (a: Any) => a.asInstanceOf[A]
      val cvt:  String => A
    }


    implicit val tsvInt = new TSVBASETYPE[Int] {
      val ty  = "Int"
      val cvt = (a: String) => a.toInt
    }


    implicit val tsvDbl = new TSVBASETYPE[Double] {
      val ty  = "Double"
      val cvt = (a: String) => a.toDouble
    }


    implicit val tsvStr = new TSVBASETYPE[String] {
      val ty  = "String"
      val cvt = (a: String) => a
    }

  }  // End object TSVSupport


  val implicits = dbmodel.DBFile.implicits


}  // End object TSVModel



/** Examples **********************************************
 *
 *
{{{

  import dbmodel.TSVModel._
  import dbmodel.TSVModel.TSVSupport._
  import dbmodel.TSVModel.implicits._
  import dbmodel.DBModel.OrderedCollection._

  val remy = Map("name" -> 0, "age" -> 1, "sex" -> 2)
  val wls  = Array("WLS", 50, "m")
  val lsq  = Array("LSQ", 40, "f")
  val wms  = Array("WMS", 10, "m")
  val family = List(wms, lsq, wls).map(Remy(remy, _))

  val key  = Key.asc[Remy,Int](x => x.int("age"))
  val db = RemyFile.transientRemyFile(family, key)

  db.saveAs("xx")

  val xx = RemyFile("xx")

  xx.length

  xx.map(x => x("name")).toVector

  xx.protection(false).close()
  
}}}
 *
 */



