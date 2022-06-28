


package dbmodel

/** Version 9, to be used with dbmodel.DBModel Version 9.
 *
 *  Wong Limsoon
 *  18 March 2022
 */




object DBFile {


/** A simple model to turn files into ordered collection. Ordered
 *  collection comes with Synchrony iterator, which provides efficient
 *  general synchronized iteration over multiple collections.
 */

  import scala.language.implicitConversions
  import java.nio.file.{ Files, Paths, StandardCopyOption }
  import java.nio.charset.StandardCharsets.UTF_8
  import java.util.Base64
  import java.io.{
    BufferedWriter, PrintWriter, File, EOFException,
    ObjectInputStream, ByteArrayInputStream,
    ByteArrayOutputStream, ObjectOutputStream 
  }

  import scala.collection.BufferedIterator
  import dbmodel.DBModel.CloseableIterator._
  import dbmodel.DBModel.OrderedCollection.{ OColl, OSeq, Key }


  var TMP: String = "."     // Default folder for temporary files


  type Ord[K]    = Ordering[K]                   // Shorthand

  type Bool      = Boolean                       // Shorthand

  type PARSER[B] = String => (Iterator[String], Int) => B  // Type of parser.
          // A parser parses a few lines at a time until a object
          // can be constructed. It returns the object.
          // The second (Int) parameter tells the parser which
          // entry it is now parsing. 
          // The initial string can be used for specifying some
          // initialization parameters.

  type FORMATTER[B] = String => (B, Int) => String // Type of an unparser.
          // It converts/formats an object into a string for writing
          // to file. The second (Int) parameter tells the unparser
          // which entry it is formatting; e.g., it may need to generate
          // header info for 1st entry.
          // The initial string can be used for specifying some
          // initialization parameters.

  type DESTRUCTOR[B] = OFile[B,_] => Unit        // Type of file destructor.
          // A file destructor deletes a given file. Some descendent
          // types of [[OFile]] may be nested files. 



  /** [[FileIterator(filename, parser)]] uses the [[parser]] to parse
   *  the file [[filename]] into an iterators on items in the file.
   */

  case class FileIterator[B](filename: String, parser: PARSER[B]) extends CBI[B]
  {
    /** [[FIterator]] is iterator on items in a file.
     *  @param filename  is name of the file.
     *  @param parser is a parser for the file.
     */

    protected val file   =                      // File handle.
      Files.isRegularFile(Paths.get(filename)) match {
        case true  => scala.io.Source.fromFile(filename)
        case false => null 
    }

    protected val parse = parser(filename)      // Parser initialized

    protected var n     = 0                     // Position of current item.

    protected var hd: B = _                     // Current item in file.

    protected var isDefined: Bool = _           // Does file has more items?

    protected val ois   = (file==null) match {  // Remaining lines in file.
      case true  => Iterator() 
      case false => file.getLines().map(_.trim) 
    }

    /** Method to close the file.
     */

    override def close(): Unit  = { 
      isDefined = false
      held.foreach { _.close() }
      held = Seq()
    }

    /** Method for fetching the next item in file.
     *  Auto-close the file at end of file.
     */

    protected def shift() = 
      try { hd = parse(ois, n); n = n + 1; true  } 
      catch {
        case e: EOFException => close()
        case e: NoSuchElementException => close()
        case e: Throwable => { close(); throw e }
    }

    /** Methods defining buffered iterator
     */

    def head: B       = hd

    def hasNext: Bool = isDefined

    def next(): B     = { val h = hd; shift();  h } 

    /** Always read one item ahead to avoid disk latency :-).
     */

    isDefined = file != null    // Is file closed?

    if (isDefined) { use(file); shift() }

  }   // End case class FIterator.




  /** Implicit classes to provide nicer/more convenient syntax
   *  for converting between file name and iterators.
   *
   *  [[filename.fiterator(parser)]] converts [[fname]], 
   *  a presumed file name, into [[FIterator]].
   *
   *  [[it.file(formatter)(filename)]] writes [[it]] to 
   *  a file [[filename]].
   */
  
  implicit class FromFile[B](filename: String) 
  {
    def fiterator(parser: PARSER[B] = OFile.defaultParser[B] _)
    : FileIterator[B] = 
    {
      FileIterator(filename, parser)
    }
  }



  implicit class ToFile[B](it: Iterator[B])
  {
    def tofile
      (formatter: FORMATTER[B]   = OFile.defaultFormatter[B] _)
      (implicit filename: String = "", folder: String = TMP): String = 
    {
      /** Open the file for writing.
       */

      val fname  = OFile.mkFileName(filename, folder).toString
      val format = formatter(fname)    

      /** Write items in [[it]] to the file.
       */

      val oos = new BufferedWriter(new PrintWriter(new File(fname)))
      var n   = 0
      while (it.hasNext) {
        oos.write(format(it.next(), n))
        oos.newLine()
        n = n + 1
        if (n % 10000 == 0) oos.flush()
      }

      /** Close the file.
       */

      oos.flush()
      oos.close()

      /** Return the file name.
       */

      return fname
    }
  }



  implicit class OCollToFile[B,K](it: OColl[B,K]) extends ToFile[B](it.cbi)



  /** [[OFile(key, parser, formatter)]] represents a large possibly transient
   *  ordered collection [[OColl]], e.g. a dynamically produced data stream.
   *  The collection is assumed ordered by [[key]].  Items in the collection 
   *  can be written to a file using [[formatter]] and read back using 
   *  [[parser]] as needed.  This is useful, e.g., when collection has to be
   *  sorted.
   */

  sealed trait OFile[B,K] extends OColl[B,K]
  {
    // To be provided by inheriting instances:

    val key: Key[B,K]
    val filename = ""                      // Set [[filename]] to "" 
                                           // if [[OFile]] is a transient
                                           // collection; i.e., not on disk.
    def elems: IterableOnce[B] = null      // Set [[elems]] to null if
                                           // [[OFile]] is on disk.
    def parser:     PARSER[B]
    def formatter:  FORMATTER[B]
    def destructor: DESTRUCTOR[B]


    /** [[OFile]] has three different characteristics:
     *  - [[ODisk]], the file is on disk. 
     *  - [[OMemory]], the file is materialized in memory. 
     *  - [[OTransient]], the file is transient (i.e., still being produced.)
     */

    type OCBI[N] = OFile[B,N]


    protected def OCBI[N](es: IterableOnce[B], ky: Key[B,N]): OCBI[N] =
      es match {
        case ei: Iterable[B] => omemory(ei, ky)
        case _ => oitr(es, ky)
      }


    protected def oitr[N](es: IterableOnce[B], ky: Key[B,N]) =
      OTransient(ky, es, parser, formatter, destructor)

 
    protected def odisk[N] (filename: String, ky: Key[B,N]) =
      ODisk(ky, filename, parser, formatter, destructor)


    protected def omemory[N](es: Iterable[B], ky: Key[B,N]) =
      OMemory(ky, es, parser, formatter, destructor)



    /** An [[OFile]] may be associated with a temporary file, which needs
     *  to be destroyed / garbage collected after using it.
     */

    var protect = false   // If true, protect [[OFile]] from destruction


    def protection(implicit flag: Bool = true): this.type = {
      protect = flag
      this
    }

    

    /** Initialization depends on whether [[OFile]] is on disk, in memory,
     *  or transient. [[OFile]] constructors must call this method.
     */

    override protected def init = 
    {
      // Keep track of file handles if [[elems]] is transient.

      elems match {
        case null => { }
        case ei: CBI[B] => use(ei)
        case _ => { }
      }

      // Turn on protection by default if this [[OFile]] is on disk.

      protection(filename != "")
    }



    /** Delete the underlying file, and releasing resources.
     */

    implicit def destruct(): Unit = destructor(this)


    override def close(): Unit = if (!protect) {
      destruct()
      held.foreach { _.close() }
      held = Seq()
    }


    /** For [[OFile]] on disk, [[elems]] is null. Need to open and read
     *  the file as needed.
     */

    override def fileElems: FileIterator[B] = 
      FileIterator[B](filename, parser)



    /** [[OFile]] may be very large collection. Cannot do in-memory sorting.
     *  We split it into a few smaller files and do on-disk merge sort.
     *
     *  @param ky    is the new sorting key.
     *  @param bufSz is the # of items per smaller file.
     */

    var bufSz = 50000                      // Default # of items per small file.


    override def assumeOrderedBy[N](ky: Key[B,N]): OCOL[N] = 
      { if (filename != "") odisk(filename, ky) else OCOL(elems, ky)
      }.use(this)


    override def orderedBy[N](ky: Key[B,N]): OCBI[N] = orderedBy(ky, bufSz)


    def orderedBy[N](ky: Key[B,N], bufSz: Int): OCBI[N] =
    {
      /** Split big files into smaller sorted files.
       */

      val groups = { 
        for (
          g <- cbi.grouped(bufSz);         // Group into bufSz chunks.
          s = OSeq(g, key).orderedBy(ky);  // Sort chunk in memory.
          f = s.cbi.tofile(formatter))     // Write chunk to file.
        yield odisk(f, ky).protection(false)
      }.toSeq

      close()

      /** Merge the sorted smaller files into a big sorted file.
       */

      val merged = 
        if (groups.length == 0) { odisk("", ky) }
        else if (groups.length == 1) { groups.head } 
        else groups.head.mergedWith(groups.tail: _*)

      merged.protection(false)
    }


    
    /** [[OFile]] may be very large collection. Cannot do in-memory reversing.
     *  We split it into a few smaller files and do on-disk merge reverse.
     */

    override def reversed: OCOL[K] = reversed(bufSz)


    def reversed(bufSz: Int): OCBI[K] =
    {
      val groups = { 
        for (
          g <- cbi.grouped(bufSz);         // Group into bufSz chunks.
          s = OSeq(g, key).reversed;       // Reverse chunk in memory.
          f = s.cbi.tofile(formatter))     // Write chunk to file.
        yield odisk(f, key.reversed).protection(false)
      }.toSeq.reverse

      close()

      /** Merge the reversed smaller files into a big reversed file.
       */

      val merged =
        if (groups.length == 0) { odisk("", key.reversed) }
        else if (groups.length == 1) { groups.head } 
        else oitr(groups.flatMap(_.cbi), key.reversed).userev(groups: _*)

      merged.protection(false)
    }



    /** Slurp a copy of the file into memory
     */

    def slurped: String =
      try new String(Files.readAllBytes(Paths.get(filename)))
      catch { case _: Throwable => "" }


    /** Save a copy to a file
     */

    def saveAs(name: String, folder: String = TMP): OFile[B,K] = 
    {
      val newname = OFile.mkFileName(name, folder)
      val oldname = Paths.get(filename)
      val saved   = oldname.toString match {
        case "" =>
          // Collection is transient. Write it directly to the new file. 
          val copy = cbi.tofile(formatter)(newname.toString)
          odisk(copy, key)
        case _ =>
          // Collection is a file. Copy it to the new file.
          Files.copy(oldname, newname, StandardCopyOption.REPLACE_EXISTING)
          odisk(newname.toString, key)
      }
      close()
      saved
    } 


    /** Serialize the file. If it is already on disk, do nothing.
     */

    def serialized(
      implicit name: String = "", 
             folder: String = TMP): OFile[B,K] =
    {
      // filename != "" means [[OFile]] is already on disk.  name == "" means 
      // user does not care where the file is serialized to. Hence, when both
      // are true, no need to do anything.
      if (filename != "" && name == "") this
      /** An [[OColl]] may be associated with a temporary collection,
       *  needs to be destroyed / garbage collected after using it.  
       *  This is complex, as inheriting instances of [[OColl]] can
       *  be a collection on disk, a collection in memory, a transient
       *  collection with different properties and associated resources
       *  that need to be properly closed/terminated.
       */

      // Otherwise, must save this [[OFile]] to disk.
      // Unprotect it, if it is a tmp file.
      else saveAs(name, folder).protection(name != "")
    }



    /** Rename the file.
     */

    def renameAs(name: String, folder: String = TMP): OFile[B,K] =
    {
      val newname = OFile.mkFileName(name, folder)
      val oldname = Paths.get(filename)
      val saved   = oldname.toString match {
        case "" =>
          // Collection is transient. Write it directly to the new file. 
          val copy = cbi.tofile(formatter)(newname.toString)
          odisk(copy, key)
        case _ =>
          // Collection is a file. Rename it to the new file.
          Files.move(oldname, newname, StandardCopyOption.REPLACE_EXISTING)
          odisk(newname.toString, key)
      }
      close()
      saved
    } 

  } // End trait OFile




  /** [[OTransient(key, ei, parser, formatter)]] represents an iterator
   *  [[ei]] as an ordered collection which can be written to a file
   *  using the [[formatter]], and read back in using the [[parser]].
   */

  class OTransient[B,K](
    override val key:        Key[B,K],
    override val elems:      IterableOnce[B],
    override val parser:     PARSER[B],
    override val formatter:  FORMATTER[B],
    override val destructor: DESTRUCTOR[B]) extends OFile[B,K]  


  object OTransient {
    def apply[B,K](
      key:        Key[B,K],
      elems:      IterableOnce[B],
      parser:     PARSER[B],
      formatter:  FORMATTER[B],
      destructor: DESTRUCTOR[B]): OTransient[B,K] =
    {
      val tmp = new OTransient(key, elems, parser, formatter,destructor)
      tmp.init
      tmp
    }
  }


  /** [[OMemory(key, es, parser, formatter)]] represents a [[Seq]]
   *  [[es]] as an ordered collection which can be written to a file
   *  using the [[formatter]], and read back in using the [[parser]].
   */

  class OMemory[B,K](
    override val key:        Key[B,K],
    override val elems:      Iterable[B],
    override val parser:     PARSER[B],
    override val formatter:  FORMATTER[B],
    override val destructor: DESTRUCTOR[B]) extends OFile[B,K] 


  object OMemory {
    def apply[B,K](
      key:        Key[B,K],
      elems:      Iterable[B],
      parser:     PARSER[B],
      formatter:  FORMATTER[B],
      destructor: DESTRUCTOR[B]): OMemory[B,K] = 
    {
      val tmp = new OMemory(key, elems, parser, formatter, destructor)
      tmp.init
      tmp
    }
  }


  /** [[ODisk(key, filename, parser, formatter)]] represents the file
   *  [[filename]] as an ordered collection [[OFile]]. Items in the
   *  File are parsed using [[parser]] on demand as the [[OFile]] is
   *  accessed. Items in the [[OFile]] are written out using [[formatter]]
   *  as required; this is useful, e.g., when the file has to be sorted.
   *  The file is assumed to be already ordered by a [[key]].
   */

  class ODisk[B,K](
    override val key:        Key[B,K],
    override val filename:   String, 
    override val parser:     PARSER[B],
    override val formatter:  FORMATTER[B],
    override val destructor: DESTRUCTOR[B]) extends OFile[B,K]


  object ODisk {
    def apply[B,K](
      key:        Key[B,K],
      filename:   String, 
      parser:     PARSER[B],
      formatter:  FORMATTER[B],
      destructor: DESTRUCTOR[B]): ODisk[B,K] =
    {
      val tmp = new ODisk(key, filename, parser, formatter, destructor)
      tmp.init
      tmp
    }
  }



  object OFile 
  {
    def apply[B,K](
      key:       Key[B,K],
      filename:  String, 
      parser:    PARSER[B], 
      formatter: FORMATTER[B]): OFile[B,K] =
    {
      import java.nio.file.{ Paths, Files }
      if (Files.exists(Paths.get(filename))) {
        ODisk(key, filename, parser, formatter, destructorOFile _)
      }
      else throw new java.io.FileNotFoundException(filename)
    }


    /** More constructors for [[OFile]], so that programmer does not 
     *  need to provide ordering explicitly.
     */

    def apply[B,K]
      (filename:  String, 
       parser:    PARSER[B], 
       formatter: FORMATTER[B], 
       get: B => K)
      (implicit ord: Ord[K]): OFile[B,K] =
    {
      OFile(Key.asc(get)(ord), filename, parser, formatter)
    }


    def apply[B,K]
      (filename: String, get: B => K)
      (implicit ord: Ord[K]): OFile[B,K] =
    {
      OFile(Key.asc(get)(ord), 
            filename, 
            defaultParser[B] _, 
            defaultFormatter[B] _)
    }


    /** Function for deleting an underlying file. Shallow deletion is 
     *  implemented here.
     */

    def destructorOFile[B](ofile: OFile[B,_]): Unit = {
      import dbmodel.DBModel.DEBUG.message
      val filename = ofile.filename
      val protect  = ofile.protect
      val err = "**** OFile.destruct: Cannot delete file " + filename
      if (filename != "") 
        try { if (!protect) Files.deleteIfExists(Paths.get(filename)) } 
        catch { case _: Throwable => message(err) }
    }


    
    /** Function for making filenames in a consistent manner.
     */

    def mkFileName(
      name: String = "", 
      folder: String = TMP): java.nio.file.Path =
    {
      val dir: java.nio.file.Path = folder match {
        case "/tmp"  => Paths.get(System.getProperty("java.io.tmpdir")) 
        case ""      => Paths.get(System.getProperty("java.io.tmpdir")) 
        case "."     => Paths.get(".").toAbsolutePath.getParent
        case d       => Files.createDirectories(Paths.get(d))
      }
      val fname = (name == "") match {
        case true  => Files.createTempFile(dir, "ofile", ".tmp").toString
        case false => name
      }
      dir.resolve(fname)
    }


    /** Default parser and unparser. These are generic and inefficient.
     *  User should supply parsers and unparsers for their files.
     *  The default parser/unparser ignore position info.
     */

    def defaultFormatter[B]
      (options: String)
      (b: B, position: Int = 0): String =
    {
      val stream = new ByteArrayOutputStream()
      val ees = new ObjectOutputStream(stream)
      ees.writeObject(b)
      ees.close()
      new String(Base64.getEncoder().encode(stream.toByteArray), UTF_8)
    }
 
    def defaultParser[B]
      (options: String)
      (es: Iterator[String], position: Int = 0): B =
    {
      if (es.hasNext) {
        val encoded = es.next() 
        val bytes = Base64.getDecoder().decode(encoded.trim.getBytes(UTF_8))
        val ees = new ObjectInputStream(new ByteArrayInputStream(bytes))
        ees.readObject.asInstanceOf[B]
      }
      else throw { new EOFException }
    }

  } // End object OFile



  object implicits 
  {
    import scala.language.implicitConversions

    implicit def OSeqToCBI[B,K](ocoll: OSeq[B,K]): CBI[B]  = ocoll.cbi

    implicit def OSeqFromIT[B](it: IterableOnce[B]): OSeq[B,Unit] = 
      OSeq[B,Unit](it, (x:B)=>())(Ordering[Unit])

    implicit def OFileToCBI[B,K](ofile: OFile[B,K]): CBI[B] = ofile.cbi
  }


} // End object DBFile




/** Examples *****************************************************


import dbmodel.DBModel.{ OrderedCollection, Join, Predicates }
import OrderedCollection._
import Predicates._
import Join._
import dbmodel.DBFile._
import dbmodel.DBFile.implicits._


// Create some data... phone models

case class Phone(model: String, price: Int, brand: String)

val s21   = Phone("S21", 1000, "Samsung")
val a52   = Phone("A52", 550, "Samsung")
val a32   = Phone("A32", 350, "Samsung")
val n10   = Phone("N10", 360, "OnePlus")
val a94   = Phone("A94", 400, "Oppo")
val m40   = Phone("Mate 40", 1200, "Huawei")
val pix5  = Phone("Pixel 5", 1300, "Google")
val pix4  = Phone("Pixel 4", 500, "Google")


// Phone models can be ordered by brand or by price

val kPrice = Key.asc[Phone,Int](x => x.price)
val kBrand = Key.asc[Phone,String](x => x.brand)


// Store into a tmp file, sorted by brand
// using default formatter and parser:

val formatter = (_: String) => (x:Phone, n: Int) => 
      OFile.defaultFormatter("")(Phone.unapply(x).get, n)

val parser = (_: String) => (xs: Iterator[String], n: Int) => 
  Phone tupled OFile.defaultParser("")(xs)

val fname = {
  val vs = Vector(s21, a52, a32, n10,a94, m40, pix5, pix4)
  vs orderedBy kBrand
}.tofile(formatter)("wlstest")

// Read the file back in

val OByBrand = OFile(fname, parser, formatter, (x:Phone) => x.brand)


// Phones grouped by brands

for ((b, ps) <- OByBrand.clustered) 
{
  println(s"brand = ${b}\n  ${ps}\n")
}


// Phones, re-sorted by price 

val OByPrice = OByBrand orderedBy kPrice

OByPrice.protection(true)  // Protect file from deletion

// Phones and their price competitors from
// other brands within +/- $150.
// Non-equijoin alert!

for ((p, cs) <- OByPrice join OByPrice on dleq(150);
     ob = cs.filter(_.brand != p.brand)) 
{ 
  println(s"${p}\n  ${ob}\n") 
}


// Samsung phones and their price competitors 
// from Google, within 20%.
// Non-equijoin alert!

for ((p, cs) <- OByPrice.filter(_.brand == "Samsung")
                  join OByPrice.filter(_.brand == "Google")
                  on szpercent(1.2))
{ 
  println(s"${p}\n  ${cs}\n") 
}


// Clean up

OByBrand.protection(false)    // Allow file to be deleted.
OByPrice.protection(false)    // Allow file to be deleted.
OByPrice.close()              // Delete both files. Note that
OByBrand.close()


*****************************************************************/




