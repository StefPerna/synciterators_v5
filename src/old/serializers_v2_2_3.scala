
package synchrony.iterators

  /** Provide serialization and deserialization for EFiles
   *
   * Wong Limsoon
   * 23 May 2020
   */



object Serializers {

  import java.util.Base64

  import java.nio.charset.StandardCharsets.UTF_8

  import java.io.{
    PrintWriter, File, EOFException,
    ObjectInputStream, ByteArrayInputStream,
    ByteArrayOutputStream, ObjectOutputStream 
  }

  import synchrony.iterators.SyncCollections.EIterator


  var DEBUG = true
  var PEEKAHEAD = 10



  /** Serialization is roughly divided into two parts: 
   *  - Formatter of an item in an EFile, and
   *  - Serializer iterates a Formatter on all items in an EFile.
   */



  /** Formatter
   *
   *  @param a is an item to format.
   *  @return  item a formatted as a String
   */

  trait Formatter[A] {
    def format(a: A): String
    def apply(a: A): String = format(a)
  }


  class ItemFormatter[A] extends Formatter[A] {

    override def format(a: A): String = ItemFormatter.format[A](a)

  }  // End class ItemFormatter



  /** Default item formatter is the Base64 encoder.
   */

  object ItemFormatter {

    def apply[A](a: A): String = format[A](a)

    def format[A](a: A): String = {
      val stream = new ByteArrayOutputStream()
      val ees = new ObjectOutputStream(stream)
      ees.writeObject(a)
      ees.close()
      new String(Base64.getEncoder().encode(stream.toByteArray), UTF_8)
    }

  }  // End object ItemFormatter



  /** Serializer.
   *
   * @val    formatter is the formatter to be used.
   * @param  it        is an EIterator of an EFile to be serialized.
   * @param  filename  is name of the output file.
   * @effect           items on EIterator it are written by formatter
   *                   to filename.
   */

  trait Serializer[A] {
    val formatter: Formatter[A]
    def serialize(it: EIterator[A], filename: String): Unit
    def apply(it: EIterator[A], filename: String): Unit 
      = serialize(it, filename)
  }



  class FileSerializer[A](override val formatter: Formatter[A])
  extends Serializer[A] {

    override def serialize(it: EIterator[A], filename: String): Unit = {

      var n = 0

      val oos = new PrintWriter(new File(filename))
      if (DEBUG) println(s"** Serializing file = ${filename} ...")

      while (it.hasNext) {
        oos.println(formatter(it.next()))
        n = n + 1
        if (n % 10000 == 0) {
          oos.flush()
          // Print some tracking info every few thousand items.
          if (DEBUG) println(s"... Writing file = ${filename}")
          if (DEBUG) println(s"... # of items written so far = ${n}")
        }
      }

      oos.flush()
      oos.close()
      if (DEBUG) println(s"** Finished writing file = ${filename}")
      if (DEBUG) println(s"** Total # of items written = ${n}")
    }

  }  // End class Serializer



  class IncrementalSerializer[A](filename: String, format: Formatter[A]) {

    private var n: Int = 0

    private var oos: PrintWriter = null

    def open(): Unit = {
      oos = new PrintWriter(new File(filename)) 
      n = 0
      if (DEBUG) println(s"** Serializing file = ${filename} ...")
    }


    def close(): Unit = { 
      oos.flush()
      oos.close()
      if (DEBUG) println(s"** Finished writing file = ${filename}")
      if (DEBUG) println(s"** Total # of items written = ${n}")
    }


    def append(a: A): Unit = {
      oos.println(format(a))
      n = n + 1
      if (n % 10000 == 0) {
        oos.flush()
        // Print some tracking info every few thousand items.
        if (DEBUG) println(s"... Writing file = ${filename}")
        if (DEBUG) println(s"... # of items written so far = ${n}")
      }
    }

  }  // End class IncrementalSerializer



  /** Deserialization is roughly divided into two parts:
   *  - Parser to parse an item in a serialized EFile.
   *  - Deserializer to iterate Parser on all items in a serialized EFile.
   */



  /** Parser.
   *
   *  @param ois is an Eiterator[String] representing an item
   *  @return the parsed item.
   */

  trait Parser[A] {
    def parse(ois: EIterator[String]): A
    def apply(ois: EIterator[String]): A = parse(ois)
  }

  
  class ItemParser[A] extends Parser[A] {

    override def parse(ois: EIterator[String]): A = {

      val encoded = 
        try ois.next() 
        catch { case _: Throwable => ois.close(); throw new EOFException("") }

      val bytes = Base64.getDecoder().decode(encoded.trim.getBytes(UTF_8))
      val ees = new ObjectInputStream(new ByteArrayInputStream(bytes))

      val decoded = 
        try ees.readObject.asInstanceOf[A]
        catch { case e: Throwable => ois.close(); throw(e) }
        finally { ees.close() }

      decoded
    }

  }  // End class ItemParser




  /** Deserializer.
   *
   *  @param filename is the file to write to.
   *  @param slurp    slurp is None means filename is a file.
   *                  slurp is Some(f) means filename is actually a string
   *                  which is the content of the file f.
   *  @return         an EIterator of items in the EFile.
   */ 

  trait Deserializer[A] {
    def parse(filename: String, slurp: Option[String] = None): EIterator[A]
    def apply(filename: String, slurp: Option[String] = None): EIterator[A] = {
      parse(filename, slurp)
    }
  }


  /** FileDeserializer realises a simple Deserializer.
   * 
   *  @val itemParser is parser to read an item in the file.
   *  @val guard      is predicate to select lines to parse.
   *  @return         a deserializer, which given a filename, opens it,
   *                  constructs an EIterator on items in the file,
   *                  and closes the file when all items are produced
   *                  by the EIterator.
   */

  class FileDeserializer[A](
    itemParser: Parser[A],
    guard:      String => Boolean)
  extends Deserializer[A] {

    override def parse(filename: String, slurp: Option[String] = None): 
    EIterator[A] = new EIterator[A] {

      private val ois = slurp match {
        
        // This is a regular file.
        case None =>  
          if (DEBUG) { println(s"** Deserializing file = ${filename}") }
          EIterator.StringfromFile(filename, guard)

        // Assume "filename" is a slurped copy of the file f.
        case Some(f) => 
          EIterator.fromVector(filename.split("\n"))
      }


      private def getNext() =
        try Some(itemParser(ois))
        catch {
          case e: EOFException           =>  close(); None 
          case e: NoSuchElementException =>  close(); None 
          case e: Throwable              =>  
            if (DEBUG) println(s"Caught unexpected exception: ${e}")
            close(); None
        }
  

      private var nextEntry = getNext() 

      override protected def myHasNext = nextEntry != None 

      override protected def myNext() = try {
        nextEntry match {
          case Some(e) => e
          case None => throw new EOFException()
        }
      }
      finally { nextEntry = getNext() } 

      override protected def myClose() = ois.close()
    }

  }  // End class FileDeserializer




  class FileDeserializerWithPosition[A](
    rawParser:  String => A,
    itemParser: Parser[A],
    guard:      String => Boolean)
  extends Deserializer[A] {


    override def parse(filename: String, slurp: Option[String] = None): 
    EIterator[A] = new EIterator[A] {

      private val ois = slurp match {
        
        // This is a regular file.
        case None =>  
          if (DEBUG) { println(s"** Deserializing file = ${filename}") }
          EIterator.StringfromFile(filename, guard)

        // Assume "filename" is a slurped copy of the file f.
        case Some(f) => 
          EIterator.fromVector(filename.split("\n"))
      }


      override def position[B]
        (isBefore: (A, B) => Boolean,
         canSee:   (A, B) => Boolean)
        (b: B):
      Unit = {
        do {  }
        while (ois.peekahead(PEEKAHEAD) match {
          case None       => false
          case Some(line) =>
            val a    = rawParser(line)
            (isBefore(a, b), canSee(a, b)) match {
               case (true, false) => ois.buffer.dropInPlace(PEEKAHEAD); true
               case _ => false
            }
          }
        )
      }


      private def getNext() =
        try Some(itemParser(ois))
        catch {
          case e: EOFException           =>  close(); None 
          case e: NoSuchElementException =>  close(); None 
          case e: Throwable              =>  
            if (DEBUG) println(s"Caught unexpected exception: ${e}")
            close(); None
        }
  

      private var nextEntry = getNext() 

      override protected def myHasNext = nextEntry != None 

      override protected def myNext() = try {
        nextEntry match {
          case Some(e) => e
          case None => throw new EOFException()
        }
      }
      finally { nextEntry = getNext() } 

      override protected def myClose() = ois.close()
    }

  }  // End class FileDeserializerWithPosition


}  // End object Serializers
    

