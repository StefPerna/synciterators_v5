
package synchrony
package iterators


//
// Wong Limsoon
// 15 April 2020
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
    // The three main EIterator functions a loan pattern
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
    customization: A => A,
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

    serializer: (EFile[A]) => String => Unit,
                            // serializer to be used for writing 
                            // this EFile out.

    deserializer:(A => A) => String => EIterator[A])
                            // deserializer(customization) to be used 
                            // for reading an EFile in. The function
                            // customization is for transforming or
                            // initializing each deserialized element
                            // properly.


  case class EFile[A](efile: EFileState[A])
  {
    //
    // Function to make EIterator for the EFile
    //

    def iterator: EIterator[A] = EFile.iteratorOf(this)


    //
    // Function to fetch the n-th element in the EFile
    //

    def apply(n:Int):A = EFile.getElement(this, n) 


    //
    // Functions for checking filesize and filename of EFile
    //

    def filesize: Long = EFile.filesizeOf(this)


    def filename: String = EFile.filenameOf(this)
    


    //
    // If efile is transient, depending on its
    // estimated size, store it in memory or on disk.  
    //

    def stored: EFile[A] = EFile.store(this)


    //
    // Serialize the Efile to disk
    //
  
    def serialized: EFile[A] = EFile.serialize(this)
      

    //
    // Delete the EFile, if it is on disk.
    //

    def destruct():Unit = EFile.destroy(this)


    //
    // Save the EFile to a file with a new name.
    // May need to recursively save its elements;
    // this is done by iterating the specified
    // preparations on these elements.
    //
      
    def savedAs(name:String, preparations: Option[A => A] = None): EFile[A] = 
    { 
      EFile.saveAs(this, name, preparations)
    }


    //
    // Function to merge this EFile with other EFiles.
    // Assume elements on all EFiles are sorted.
    //

    def mergedWith
       (that:EFile[A]*)
       (implicit onDisk:Boolean = false, cmp:Ordering[A])
    : EFile[A] =
    {
      val efobjs = this +: (that.toVector)
      EFile.merge[A](efobjs :_*)(onDisk = onDisk, cmp = cmp)
    }


    //    
    // Functions for sorting EFile
    //

    def sortedWith(
      cmp:(A,A)=>Boolean,
      capacity:Int = efile.settings.cap,
      onDisk:Boolean = false)
    :EFile[A] =
    {
      EFile.sortWith(this)(cmp = cmp, capacity = capacity, onDisk = onDisk)
    }



    def sorted(implicit
      cmp:Ordering[A],
      capacity:Int = efile.settings.cap,
      onDisk:Boolean = false)
    :EFile[A] = 
    {
      EFile.sort[A](this)(cmp = cmp, capacity = capacity, onDisk = onDisk)
    }

  }



  object EFile
  {

    def setDefaultsEFile[A<:Serializable](): EFileSettings[A] =
      EFileSettings[A](
        prefix = "synchrony-",
        suffixtmp = ".eftmp",
        suffixsav = ".efsav",
        aveSz = 500,
        cardCap = 2000,
        ramCap = 50000000L,
        cap = 200000,         // = (ramCap/aveSz).toInt
        doSampling = true,
        samplingSz = 30,
        alwaysOnDisk = false,
        serializer = defaultSerializerEFile _,
        deserializer = defaultDeserializerEFile _) 


    def apply[A](efile: EFileState[A]):EFile[A] = new EFile[A](efile)


    def inMemoryEFile[A<:Serializable](
      entries: Vector[A],
      settings: EFileSettings[A] = setDefaultsEFile[A]()) =
    {
      new EFile[A](InMemory(entries, settings))
    }


    def transientEFile[A<:Serializable](
      entries: Iterator[A],
      settings: EFileSettings[A] = setDefaultsEFile[A]()) =
    {
      new EFile[A](Transient(entries, settings))
    }


    def nullCustomizationEFile[A](x:A) = x


    def onDiskEFile[A<:Serializable](
      filename: String,
      customization: A => A = nullCustomizationEFile[A] _,
      settings: EFileSettings[A] = setDefaultsEFile[A]()) =
    {
      new EFile[A](OnDisk(filename, customization, settings))
    }



    //
    // Default serializer and deserializer. Not very efficient, 
    // as they write out and read back everything using the 
    // default encoder of Scala. User should provide own 
    // customized ones.
    //

    def defaultSerializerEFile[A<:Serializable]
        (efobj:EFile[A])
        (filename:String)
    :Unit =
    { 
      if (DEBUG) println(s"** Serializing file = ${filename} ...")

      var n = 0
      val oos = new ObjectOutputStream(new FileOutputStream(filename))
      val it = efobj.iterator

      for(e <- it) {
        n = n + 1
        oos.writeObject(e)

        // Need to reset oos after every few thousand items.
        // Otherwise, oos accumulates stuff, and blows the heap.

        if (n % 5000 == 0) { 
          oos.flush()
          oos.reset() 
          if (DEBUG) println(s"... # of items written so far = ${n}")
        }
      }

      it.close()
      oos.flush()
      oos.close()

      if (DEBUG) println(s"... Total # of items written = ${n}")
    }


    def defaultDeserializerEFile[A<:Serializable]
        (customization: A => A)
        (filename:String) 
    : EIterator[A] = 
    {
      def openFile(f:String) = new ObjectInputStream(new FileInputStream(f))

      def parseItem(ois:ObjectInputStream) = 
        try customization(ois.readObject.asInstanceOf[A]) 
        catch { case e: Throwable => { ois.close(); throw(e) } }

      def closeFile(ois:ObjectInputStream) = ois.close()

      EIterator.makeParser(openFile, parseItem, closeFile)(filename)
    }


    //
    // The settings of an EFile
    //

    def settingsOf[A](efobj:EFile[A]) = efobj.efile.settings

    def serializerOf[A](efobj:EFile[A]) = efobj.efile.settings.serializer

    def deserializerOf[A](efobj:EFile[A]) = 
    {
      val deserializer = efobj.efile.settings.deserializer

      val customization = efobj.efile match
      {
        case ef:OnDisk[A] => ef.customization
        case _ => nullCustomizationEFile[A] _
      }

      deserializer(customization)
    }



    // 
    // Create an EIterator on an EFile.
    //

    def iteratorOf[A](efobj:EFile[A]): EIterator[A] = efobj.efile match 
    {
      case ef: InMemory[A] => EIterator[A](ef.entries)

      case ef: Transient[A] => EIterator[A](ef.entries)

      case ef: OnDisk[A] =>
      {
        def isFile(f:String) = Files.isRegularFile(Paths.get(f))
        val tmpf = ef.filename + ef.settings.suffixtmp
        val savf = ef.filename + ef.settings.suffixsav

        deserializerOf(efobj)(
          if (isFile(ef.filename)) ef.filename
          else if (isFile(tmpf)) tmpf
          else if (isFile(savf)) savf
          else { throw FileNotFound("") })
      }
    }


    def run[A,B](efobj:EFile[A])(f: EIterator[A] => B):B =
    { 
      val it = iteratorOf(efobj)
      try f(it) finally it.close() 
    }


    //
    // Fetch the n-th element in the EFile
    //

    def getElement[A](efobj:EFile[A], n:Int):A = run(efobj) { it => 
      it.drop(n)
      it.next() 
    }


    //
    // Check filesize and filename of EFile
    //

    def filesizeOf[A](efobj:EFile[A]): Long = efobj.efile match
    {
      case ef: InMemory[A] => ef.entries.length * ef.settings.aveSz

      case ef: Transient[A] => 0

      case ef: OnDisk[A] =>
        try { Files.size(Paths.get(ef.filename)) }
        catch { case _ : Throwable => throw FileNotFound(ef.filename) }
    }


    def filenameOf[A](efobj:EFile[A]): String = efobj.efile match
    {
      case ef: InMemory[A] => ""

      case ef: Transient[A] => ""

      case ef: OnDisk[A] => ef.filename
    }
    


    //
    // If an EFile is transient, depending on its
    // estimated size, store it in memory or on disk.  
    //

    def store[A](efobj:EFile[A]): EFile[A] = efobj.efile match
    { 
      case ef: InMemory[A] => efobj

      case ef: OnDisk[A] => efobj

      case ef: Transient[A] => run(efobj) { it =>
      
        val sampling = it.shadow().take(ef.settings.cardCap).toVector

        if (!efobj.efile.settings.alwaysOnDisk && 
            sampling.length < ef.settings.cardCap) 
        { //
          // EFile appears small; keep in memory.

          InMemory(sampling, ef.settings).toEFile
        }
 
        else 
        { //
          // EFile appears a bit large; keep on disk

          val filename = Files
                         .createTempFile(
                            ef.settings.prefix, 
                            ef.settings.suffixtmp)
                         .toString
          serializerOf(efobj)(efobj)(filename) 
          OnDisk[A](filename, nullCustomizationEFile[A] _, ef.settings).toEFile
        }
      }
    }



    //
    // Serialize an Efile to disk
    //
  
    def serialize[A](efobj:EFile[A]): EFile[A] = efobj.efile match
    { 
      // EFile is already on disk. Do nothing 

      case ef: OnDisk[A] => efobj 

      // EFile is in memory or is transient.
      // Store to disk

      case _ =>
      {
        val settings = efobj.efile.settings

        val filename = Files
                       .createTempFile( settings.prefix, settings.suffixtmp)
                       .toString
        
        serializerOf(efobj)(efobj)(filename) 

        OnDisk[A]( filename, nullCustomizationEFile[A] _, settings) .toEFile
      }
    }
      


    //
    // Delete the EFile, if it is on disk.
    //

    def destroy[A](efobj:EFile[A]):Unit = efobj.efile match 
    { 
      case ef: OnDisk[A] => Files.deleteIfExists(Paths.get(ef.filename))

      case _ => { }
    }



    //
    // Save the EFile to a file with a new name.
    // May need to recursively save its elements;
    // this is done by iterating the specified
    // preparations on these elements.
    //
     
    def saveAs[A](
      efobj:EFile[A], 
      name:String, 
      preparations: Option[A => A] = None)
    : EFile[A] = 
    {
      val settings = efobj.efile.settings
 
      val prepared = preparations match 
      {
        case Some(prepare) => 
          Transient(
            run(efobj){ it => for(e <- it) yield prepare(e)},
            settings)
          .toEFile

        case None => efobj
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

        OnDisk[A]( newname, nullCustomizationEFile[A] _, settings) .toEFile
      }

      else throw FileCannotSave(name)
    }



    //
    // Merging and sorting EFiles.  
    //
    // onDisk is set to true to make merge and sort
    // become pure external merging and sorting.
    //
    // Capacity limits the number of items that can be
    // kept in memory when doing sorting.
    //

    def merge[A](efobjs: EFile[A]*)(
    implicit onDisk : Boolean = false, cmp : Ordering[A]) : EFile[A] =
    {
      if ((efobjs.length == 0) || (!efobjs(0).iterator.hasNext))
      {
        throw FileNotFound("First EFile for merging must be non-empty")
      }

      else
      { 
        import scala.collection.BufferedIterator

        type BI = BufferedIterator[A]

        val settings = efobjs(0).efile.settings

        val ordering = new Ordering[BI]
        {
          override def compare(x:BI, y:BI) = cmp.compare(x.head, y.head) 
        }

        var efs: Vector[BufferedIterator[A]] =
          efobjs
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



    def sortWith[A](efobj: EFile[A])(
      cmp:(A,A)=>Boolean,
      capacity:Int = efobj.efile.settings.cap,
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

      EFile.sort[A](efobj)(cmp = ordering, capacity = capacity, onDisk= onDisk)
    }



    def sort[A](efobj:EFile[A])
        (implicit cmp:Ordering[A],
                  capacity:Int = efobj.efile.settings.cap,
                  onDisk:Boolean = false)
    :EFile[A] = 
    {
      val alwaysOnDisk = efobj.efile match { 
        case ef:OnDisk[A] => true
        case _ => onDisk
      }

      run(efobj) { it =>
      
        val settings = efobj.efile.settings

        val estimatedCap =
          if (!settings.doSampling ||
             (capacity != settings.cap)) 
          {
            capacity
          }
          else
          { 
            import java.util.Base64
            import java.nio.charset.StandardCharsets.UTF_8

            val sampling = it.shadow().take(settings.samplingSz).toVector
            val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
            val oos = new ObjectOutputStream(stream)
            oos.writeObject(sampling)
            oos.close
            val tmp = new String(Base64
                                 .getEncoder()
                                 .encode(stream.toByteArray),
                                 UTF_8)

            val fac = 0.2  // multiplier to account for inaccuracies
                           // of object size estimate.

/*
 *
            val tmp = InMemory(sampling, settings).toEFile.serialized
            val sz = tmp.filesize
            tmp.destruct()
            val aveSz = (sz / sampling.length) * fac
 *
 */

            val sz = tmp.length 
            val aveSz = (sz / sampling.length) * fac
            val cap = (settings.ramCap / aveSz).toInt
           
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
            val gsorted = Transient(g.sorted(cmp).iterator, settings).toEFile
            if (alwaysOnDisk) gsorted.serialized else gsorted.stored
          }
        }

        merge[A](groups.toVector :_*)(onDisk = alwaysOnDisk, cmp = cmp)
      }
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

EFile.merge(gh, gh, gh)


gh.iterator.toVector


val ij = gh.serialized

ij.filesize

ij.iterator.toVector

ij.savedAs("xy")

val mn = EFile.onDiskEFile[Entry[Int]]("xy")

mn.iterator.toVector

mn.length

onDiskEFile[Entry[Int]]("xy").sorted

 *
 */


