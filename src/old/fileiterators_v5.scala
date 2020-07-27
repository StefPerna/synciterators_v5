
package synchrony
package iterators


//
// Wong Limsoon
// 12 April 2020
//


//
// Serializable iterators on very large files
// Provides for external sorting as well.
//


object FileCollections {
  
  import java.io._
  import java.nio.file.{Files, Paths, StandardCopyOption}
  import synchrony.iterators.MiscCollections._
  import synchrony.iterators.AggrCollections._

  var DEBUG = true


  case class FileNotFound(ms:String) extends Throwable
  case class FileClosed(ms:String) extends Throwable
  case class FileEnded(ms:String) extends Throwable
  case class FileCannotSave(ms:String) extends Throwable


  //
  // Iterators on elems in a file. These iterators 
  // need to allow users to close the files midway
  // during an iteration.
  //

  trait EIterator[+A] extends Iterator[A] with Shadowing[A] with Aggregates[A]
  {
    // Endow EIterator with aggregate functions
    // by overriding Aggregates.itC

    override def itC = elems

    
    // Endow EIterator with shadowing functions
    // by overriding Shadowing.elems

    override val elems = new Iterator[A] 
    {
      override def hasNext:Boolean = 
        if (closed) false
        else if (myHasNext) true 
        else { close(); false}

      override def next():A =
        if (!closed) myNext()
        else { throw FileClosed("") }
    }


    //
    // The three main EIterator functions...
    //

    protected var closed:Boolean = false

    def close():Unit = { myClose(); closed = true }

    override def hasNext = primary.hasNext

    override def next() = primary.next()


    //
    // Functions user must provide to implement EIterator.
    //

    protected def myHasNext:Boolean

    protected def myNext():A

    protected def myClose():Unit
  }

  
  object EIterator 
  {
    def apply[A](entries:Iterator[A]) = fromIterator(entries)

    def apply[A](entries:Vector[A]) = fromVector(entries)

    def apply[A](filename:String, deserializer:String => EIterator[A]) = 
    {
      fromFile(filename, deserializer)
    }


    def fromIterator[A](entries:Iterator[A]) = new EIterator[A]
    {
      override protected def myHasNext = entries.hasNext

      override protected def myNext() = entries.next()

      override protected def myClose() = { }
    }


    def fromVector[A](entries:Vector[A]) = fromIterator(entries.iterator)


    def fromFile[A](filename:String, deserializer:String => EIterator[A]) = 
    {
      deserializer(filename)
    }


    //
    // Function to produce parsers for user's own formats
    //

    def makeParser[B,A](
      openFile:String=>B,
      parseItem:B=>A,
      closeFile:B=>Unit) = (filename:String) => new EIterator[A] 
    {
      private val ois = openFile(filename)

      private def getNext() =
        try   { Some(parseItem(ois)) }
        catch { case e:EOFException => { close(); None }
                case e:FileEnded => { close(); None } 
                case e:NoSuchElementException => { close(); None }
                case e:Throwable => { 
                  println(s"Caught unexpected exception: ${e}")
                  close(); None } }
  
      private var nextEntry = getNext() 

      override protected def myHasNext = nextEntry != None 

      override protected def myNext() = 
      {
        try
        {
          nextEntry match 
          {
            case Some(e) => e
            case None => throw FileEnded("")
          }
        }
        finally { nextEntry = getNext() } 
      }

      override protected def myClose() = closeFile(ois)
    }

  }

  

  //
  // Serialized EIterator files
  //

  sealed trait EFileState[A]
  {
    val settings: EFileSettings[A]
    def toEFile:EFile[A] = EFile(this)
  }


  // This EFile is on disk

  final case class OnDisk[A](
    filename: String, 
    override val settings: EFileSettings[A])
  extends EFileState[A]


  // This EFile is in memory

  final case class InMemory[A](
    entries: Vector[A], 
    override val settings: EFileSettings[A])
  extends EFileState[A]


  // This EFile is transient. I.e., it may be a temporary result,
  // and it can only be used once, unless it gets converted to
  // on-disk or in-memory EFile.
   
  final case class Transient[A](
    entries:Iterator[A], 
    override val settings:EFileSettings[A])
  extends EFileState[A]
    


  case class EFileSettings[A](
    prefix: String,         // Prefix for filename
    suffixtmp: String,      // Suffix for temp file
    suffixsav: String,      // Suffix for saved file
    aveSz: Int,             // Estimated size of an EFile entry.
    cardCap: Int,           // Cardinality limit to force serialization
    ramCap: Long,           // RAM to use for internal sorting
    cap: Int,               // Cardinality limit for internal sorting
    doSampling: Boolean,    // Use sampling to guess cap
    samplingSz: Int,        // # of items to sample
    alwaysOnDisk: Boolean,  // Force serialization at all sizes;

    serializer:(String, EIterator[A]) => Unit,
                            // serializer to be used for writing 
                            // this EFile out.

    deserializer:String => EIterator[A])
                            // deserializer to be used for reading
                            // this EFile in.



  case class EFile[A](efile: EFileState[A])
  {
    val settings = efile.settings
    val serializeIt = settings.serializer
    val deserializeIt = settings.deserializer 
    val temp_dir = Paths.get("temp/")


    //
    // Function to make EIterator for the EFile
    //

    def iterator: EIterator[A] = efile match 
    {
      case ef: InMemory[A] => EIterator[A](ef.entries)

      case ef: Transient[A] => EIterator[A](ef.entries)

      case ef: OnDisk[A] =>
      {
        def isFile(f:String) = Files.isRegularFile(Paths.get(f))

        val tmpf = ef.filename + settings.suffixtmp
        val savf = ef.filename + settings.suffixsav

        deserializeIt(
          if (isFile(filename)) filename
          else if (isFile(tmpf)) tmpf
          else if (isFile(savf)) savf
          else { throw FileNotFound("") })
      }
    }



    //
    // Function to fetch the n-th element in the EFile
    //

    def apply(n:Int):A = 
    {
      val it = iterator
      try { it.drop(n - 1); it.next() }
      finally { it.close() }
    }


    //
    // Functions for checking filesize and filename
    // of EFile
    //

    def filesize: Long = efile match
    {
      case ef: InMemory[A] => ef.entries.length * settings.aveSz

      case ef: Transient[A] => 0

      case ef: OnDisk[A] =>
        try { Files.size(Paths.get(ef.filename)) }
        catch { case _ : Throwable => throw FileNotFound(ef.filename) }
    }


    def filename: String = efile match
    {
      case ef: InMemory[A] => ""

      case ef: Transient[A] => ""

      case ef: OnDisk[A] => ef.filename
    }
    


    //
    // If efile is transient, depending on its
    // estimated size, store it in memory or on disk.  
    //

    def stored: EFile[A] = { println("stored!"); efile match
    { 
      case ef: InMemory[A] => this

      case ef: OnDisk[A] => this

      case ef: Transient[A] =>
      {
        val it = iterator
        val sampling = it.shadow().take(settings.cardCap).toVector

        if (!settings.alwaysOnDisk && sampling.length < settings.cardCap) 
        { //
          // EFile appears small; keep in memory.

          InMemory(sampling, settings).toEFile
        }
 
        else 
        { //
          // EFile appears a bit large; keep on disk

          val filename = Files
                         .createTempFile(temp_dir,settings.prefix, settings.suffixtmp)
                         .toString
          serializeIt(filename, it) 
          OnDisk(filename, settings).toEFile
        }
      }
    }
  }


    //
    // Serialize the Efile to disk
    //
  
    def serialized: EFile[A] = {println("serialised!"); efile match
    { 
      // EFile is already on disk. Do nothing 

      case ef: OnDisk[A] => this 

      // EFile is in memory or is transient.
      // Store to disk

      case ef: InMemory[A] =>
      { 
        val it = iterator
        val filename = Files
                       .createTempFile(temp_dir,settings.prefix, settings.suffixtmp)
                       .toString
        serializeIt(filename, it) 
        OnDisk(filename, settings).toEFile
      }

      case ef: Transient[A] =>
      { 
        val it = iterator
        val filename = Files
                      .createTempFile(temp_dir,settings.prefix, settings.suffixtmp)
                      .toString
        serializeIt(filename, it) 
        OnDisk(filename, settings).toEFile
      }
    }
  }
      

    //
    // Delete the EFile, if it is on disk.
    //

    def destruct():Unit = efile match 
    { 
      case ef: OnDisk[A] => Files.deleteIfExists(Paths.get(ef.filename))

      case _ => { }
    }


    //
    // Save the EFile to a file with a new name.
    // May need to recursively save its elements;
    // this is done by iterating the preparations 
    // on these elements.
    //
      
    def saveAs(name:String, preparations: Option[A => A] = None): EFile[A] = 
    { 
      val prepared = preparations match 
      {
        case Some(prepare) => Transient(for(e <- iterator) yield prepare(e),
                                        settings)
                              .toEFile

        case None => this
      }
      
      val saved = prepared.serialized

      if (saved.filename != "")
      {
        val newname = name + (if (name endsWith settings.suffixsav) ""
                              else settings.suffixsav)
        Files.move(
          Paths.get(saved.filename),
          Paths.get(newname),
          StandardCopyOption.REPLACE_EXISTING)

        OnDisk(newname, settings).toEFile
      }

      else throw FileCannotSave(name)
    }


    //
    // Function to merge this EFile with other EFiles.
    // Assume elements on all EFiles are sorted.
    //

    def mergedWith(
      that:EFile[A]*)(implicit
      onDisk:Boolean = false, 
      cmp:Ordering[A]): EFile[A] =
    {
      val efiles = this +: (that.toVector)

      EFile.merge[A](efiles :_*)(onDisk = onDisk, cmp = cmp)
    }


    //
    // Functions for sorting EFile
    //

    def sortedWith(
      cmp:(A,A)=>Boolean,
      capacity:Int = settings.cap,
      onDisk:Boolean = false):EFile[A] =
    {
      EFile.sortWith(this)(cmp = cmp, capacity = capacity, onDisk = onDisk)
    }



    def sorted(implicit
      cmp:Ordering[A],
      capacity:Int = settings.cap,
      onDisk:Boolean = false):EFile[A] = 
    {
      EFile.sort[A](this)(cmp = cmp, capacity = capacity, onDisk = onDisk)
    }

  }



  object EFile
  {

    def setDefaults[A<:Serializable](): EFileSettings[A] = EFileSettings[A](
      prefix = "synchrony-",
      suffixtmp = ".eftmp",
      suffixsav = ".efsav",
      aveSz = 500,
      cardCap = 2000,
      ramCap = 100000000L,
      cap = 200000,         // = (ramCap/aveSz).toInt
      doSampling = true,
      samplingSz = 100,
      alwaysOnDisk = false,
      serializer = serializeEFile _,
      deserializer = deserializeEFile _) 


    def apply[A](efile: EFileState[A]):EFile[A] = new EFile[A](efile)


    def inMemoryEFile[A<:Serializable](
      entries: Vector[A],
      settings: EFileSettings[A] = setDefaults[A]()) =
    {
      EFile[A](InMemory(entries, settings))
    }


    def transientEFile[A<:Serializable](
      entries: Iterator[A],
      settings: EFileSettings[A] = setDefaults[A]()) =
    {
      EFile[A](Transient(entries, settings))
    }


    def onDiskEFile[A<:Serializable](
      filename: String,
      settings: EFileSettings[A] = setDefaults[A]()) =
    {
      EFile[A](OnDisk(filename, settings))
    }



    //
    // Default serializer and deserializer. Not very efficient, 
    // as they write out and read back everything. User should
    // provide own customized ones.
    //

    def serializeEFile[A<:Serializable](
      filename:String,
      it:EIterator[A]):Unit =
    { 
      var n = 0
      val oos = new ObjectOutputStream(new FileOutputStream(filename))
      for(e <- it) {
        n = n + 1
        oos.writeObject(e)
        if (n % 3000 == 0) { 
          oos.flush()
          oos.reset() 
          if (DEBUG) println(s"*** n = ${n}")
        }
      }
      it.close()
      oos.flush()
      oos.close()

      // The above resets oos after writing every few 1000 items.
      // Otherwise, oos accumulates lots of stuff, and blows the 
    }


    def deserializeEFile[A<:Serializable](filename:String) = 
    {
      def openFile(f:String) = new ObjectInputStream(new FileInputStream(f))

      def parseItem(ois:ObjectInputStream) = 
        try { ois.readObject.asInstanceOf[A] }
        catch { case e: Throwable => { ois.close(); throw(e) } }

      def closeFile(ois:ObjectInputStream) = ois.close()

      EIterator.makeParser(openFile, parseItem, closeFile)(filename)
    }


    //
    // Merging and sorting EFiles.
    //

/*
 *
    def merge[A]( x : EFile[A], y : EFile[A])(
    implicit onDisk : Boolean = false, cmp : Ordering[A]) : EFile[A] =
    { //
      // Assume x and y are both sorted.

      val it = new Iterator[A]
      {
        val itX = x.iterator.buffered
        val itY = y.iterator.buffered

        override def hasNext = itX.hasNext || itY.hasNext

        override def next() = 
          if (! itX.hasNext) itY.next() 
          else if (! itY.hasNext) itX.next() 
          else if (cmp.lteq(itX.head, itY.head)) itX.next()
          else itY.next() 
      }

      if (onDisk) Transient(it, x.settings).toEFile.serialized
      else Transient(it, x.settings).toEFile.stored
    }

 *
 */

  
    def merge[A](efiles: EFile[A]*)(
    implicit onDisk : Boolean = false, cmp : Ordering[A]) : EFile[A] =
    {
      if ((efiles.length == 0) || (!efiles(0).iterator.hasNext))
      {
        throw FileNotFound("First EFile must be non-empty")
      }

      else
      { 
        import scala.collection.BufferedIterator

        type BI = BufferedIterator[A]

        val settings = efiles(0).settings

        val ordering = new Ordering[BI]
        {
          override def compare(x:BI, y:BI) = 
          {
            (cmp.lteq(x.head,y.head), cmp.lteq(y.head,x.head)) match
            {
              case (true, false) => -1
              case (false, true) => 1
              case _ => 0
            }
          }
        }

        var efs: Vector[BufferedIterator[A]] =
          efiles
          .toVector
          .filter(ef => ef.iterator.hasNext)
          .map(ef => if (onDisk) ef.serialized else ef.stored) 
          .map(ef => ef.iterator.buffered)
          .sorted(ordering)

        val it = new Iterator[A]
        {
          override def hasNext = (efs.length > 0) && (efs(0).hasNext)

          override def next() =
          {
            val here = efs(0)
            val rest = efs.drop(1)
            val curr = here.next()
 
            efs = if (here.hasNext)
                  {
                    val i = rest.indexWhere(ef => cmp.gteq(ef.head, here.head))
                    val (before, after) = rest.splitAt(i)
                    if (i < 0) { after :+ here }
                    else { before ++ Vector(here) ++ after }
                  }
                  else rest

            curr
          }
        }

        if (onDisk) Transient(it, settings).toEFile.serialized
        else Transient(it, settings).toEFile.stored
      }
    }



    def sortWith[A](efile: EFile[A])(
      cmp:(A,A)=>Boolean,
      capacity:Int = efile.settings.cap,
      onDisk:Boolean = false):EFile[A] =
    {
      val ordering = new Ordering[A]
      {
        override def compare(x:A, y:A) = (cmp(x,y), cmp(y,x)) match
        {
          case (true, false) => -1
          case (false, true) => 1
          case _ => 0
        }
      }

      EFile.sort[A](efile)(cmp = ordering, capacity = capacity, onDisk= onDisk)
    }



    def sort[A](efile:EFile[A])(implicit
      cmp:Ordering[A],
      capacity:Int = efile.settings.cap,
      onDisk:Boolean = false):EFile[A] = 
    {
      import scala.collection.mutable.Queue

      val it = efile.iterator
      val settings = efile.settings

      val estimatedCap =
        if (!settings.doSampling ||
           (capacity != settings.cap)) 
        {
          capacity
        }
        else
        { 
          val sampling = it.shadow().take(settings.samplingSz).toVector
          val tmp = InMemory(sampling, settings).toEFile.serialized
          val sz = tmp.filesize
          val fac = 20   // multiplier to account for overheads when
                         // external data is deserialized into memory.
          val aveSz = (sz / sampling.length) * fac
          val cap = (settings.ramCap / aveSz).toInt
          tmp.destruct()
          println("destruct")
           
          if (DEBUG) 
            println(s"*** cap = ${cap}, sz = ${sz}," +
                    s" aveSz = ${aveSz}, n = ${sampling.length}")

          cap
        }

      var m = 0

      val groups =
      {
        for(g <- it.grouped(estimatedCap)) yield
        {
          m = m + 1
          if (DEBUG) println(s"*** splitting, m = ${m}")

          Transient(g.sorted(cmp).iterator, settings).toEFile.serialized
        }
      }

      merge[A](groups.toVector :_*)
    
/*
 *
      val toMerge = new Queue[EFile[A]](50)
    
      for(g <- groups)
      {
        m = m + 1
        if (DEBUG) println(s"*** splitting, m = ${m}")

        toMerge enqueue (Transient(g.sorted(cmp).iterator, settings)
                         .toEFile
                         .serialized)
      }

      while (toMerge.length > 1) 
      {
        val x = toMerge.dequeue()
        val y = toMerge.dequeue()
        if (DEBUG) println(s"*** merging, m = ${m}")
        m = m - 1

        toMerge enqueue (EFile.merge[A](x, y)(cmp = cmp, onDisk = onDisk))
        x.destruct()
        y.destruct()
      }

      toMerge.dequeue()
 *
 */
    }

  }


  //
  // Scala requires serializable classes to be pre-declared.
  // So we create an generic serializable class, Entry,
  // with which you can wrap things you want to serialize.
  //

  @SerialVersionUID(999L)
  case class Entry[A](e:A) extends Serializable

  implicit def entryOrdering[A](implicit ord:Ordering[A]):Ordering[Entry[A]] = 
    Ordering.by((x:Entry[A]) => x.e)



  //
  // Make implicit conversion to endow EIterators
  // with aggregate functions.
  //

  object implicits {

    import scala.language.implicitConversions

    implicit def EFile2EIter[A<:Serializable](efobj:EFile[A]):EIterator[A] =
      efobj.iterator

    implicit def EFileLiftInMemory[A](ef:InMemory[A]) = ef.toEFile

    implicit def EFileLiftTransient[A](ef:Transient[A]) = ef.toEFile

    implicit def EFileLiftOnDisk[A](ef:OnDisk[A]) = ef.toEFile

  }

}


/*
 *

import synchrony.iterators.FileCollections._
import synchrony.iterators.FileCollections.EFile._
import synchrony.iterators.FileCollections.implicits._


val a = Entry(10)
val b = Entry(20)
val c = Entry(30)
val ABC = Vector(a,c,b,c,b,a,a,b,c,a)

val ef = EFile.inMemoryEFile(ABC)

val gh = ef.sorted(cmp = Ordering.by((x:Entry[Int]) => x.e))

ef.sorted

EFile.merge(ef, ef, ef)


gh.iterator.toVector


val ij = gh.serialized

ij.filesize

ij.iterator.toVector

ij.saveAs("xy")

val mn = EFile.onDiskEFile[Entry[Int]]("xy")

mn.iterator.toVector

mn.length

onDiskEFile[Entry[Int]]("xy").sorted

 *
 */


