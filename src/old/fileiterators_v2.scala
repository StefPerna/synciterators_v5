
package synchrony
package iterators


//
// Wong Limsoon
// 5 April 2020
//


//
// Serializable iterators on very large files
// Provides for external sorting as well.
//


object FileCollections {
  
  import java.io._
  import java.nio.file.{Files, Paths, StandardCopyOption}


  case class FileNotFound(ms:String) extends Throwable
  case class FileClosed(ms:String) extends Throwable
  case class FileEnded(ms:String) extends Throwable


  //
  // Iterators on elems in a file. These iterators 
  // need to allow users to close the files midway
  // during an iteration.
  //

  trait EIterator[+A] extends Iterator[A]
  {
    override def hasNext:Boolean = 
        if (closed) false
        else if (myHasNext) true 
        else { close(); false}

    override def next():A = if (!closed) myNext() else { throw FileClosed("") }

    def close():Unit = { myClose(); closed = true }

    //
    // Functions user must provide to implement EIterator.
    //

    protected var closed:Boolean = false

    protected def myHasNext:Boolean

    protected def myNext():A

    protected def myClose():Unit

  }

  
  object EIterator 
  {
    def apply[A](it:Iterator[A]) = fromIterator(it)

    def apply[A](it:Vector[A]) = fromVector(it)

    def apply[A](f:String) = fromSerializedFile(f)


    def fromIterator[A](it:Iterator[A]) = new EIterator[A]
    {
      override protected def myHasNext = it.hasNext

      override protected def myNext() = it.next()

      override protected def myClose() = { }
    }


    def fromVector[A](it:Vector[A]) = fromIterator(it.iterator)


    def fromSerializedFile[A](f:String) = 
    {
      def openFile(f:String) = new ObjectInputStream(new FileInputStream(f))

      def parseItem(ois:ObjectInputStream) = ois.readObject.asInstanceOf[A]

      def closeFile(ois:ObjectInputStream) = ois.close()

      makeParser(openFile, parseItem, closeFile)(f)
    }


    //
    // Function to produce parsers for user's own formats
    //

    def makeParser[B,A](
      openFile:String=>B,
      parseItem:B=>A,
      closeFile:B=>Unit) = (f:String) => new EIterator[A] 
    {
      private val ois = openFile(f)

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
        val currEntry = nextEntry match {
          case Some(e) => e
          case None => throw FileEnded("")
        }
        nextEntry = getNext() 
        currEntry
      }

      override protected def myClose() = closeFile(ois)
    }
  }

  

  //
  // Serialized EIterator files
  //


  trait EFile[A<:Serializable] 
  {
    //
    // An EIterator (itGood) over this EFile.
    // This is the only thing that must be 
    // provided when creating EFile. 
    //

    protected def itGood:EIterator[A]
 
    protected def itBad:EIterator[A] = new EIterator[A]
    {
      override protected def myHasNext = false
      override protected def myNext() = throw FileNotFound("")
      override protected def myClose() = { }
    } 

    def iterator:EIterator[A] = 
      if (Files.exists(Paths.get(tmpfile))) itGood 
      else itBad


    //
    // Name and size of this EFile when it gets serialized.
    //

    protected var tmpfile = ""

    def filename:String = 
      if (tmpfile != "") tmpfile 
      else { tmpfile = EFile.createFileName(); tmpfile }
    
    def filesize:Long = 
      if (tmpfile == "") 0L
      else EFile.getFileSz(filename) 


    //
    // Function to serialize this iterator
    //

    protected var serializedAlready = false

    def serialized:EFile[A] = 
      if (!serializedAlready || !Files.exists(Paths.get(filename))) 
        try { EFile.serialize[A](filename, iterator) }
        finally { serializedAlready = true }
      else SerializedEFile[A](filename)


    //
    // Function to destroy the serialized copy,
    // but only when it is a tmp copy.
    //

    def destroy():Unit = EFile.destroyTmpFile(this)

 
    //
    // Function to save the serialized copy by renaming it.
    //

    def saveAs(name:String):EFile[A] = EFile.saveTmpFile(name, this) 
      
 
    //
    // A flag to indicate whether this EFile should
    // perform most of its methods in a disk-based manner. 
    //

    var onDisk = true


    // Function to merge this EFile with another EFile
    //

    def mergedWith[B>:A](that:EFile[A])(implicit cmp:Ordering[B]):EFile[A] =
      EFile.merge[A,B](this, that, this.onDisk || that.onDisk)(cmp)


    //
    // Function to sort this EFile
    //

    def sorted[B>:A](implicit cmp:Ordering[B]):EFile[A] = 
      EFile.sort[A,B](file = this, onDisk = onDisk)(cmp)

    def sortedWith[B>:A](cmp:(B,B)=>Boolean):EFile[A] =
      EFile.sortWith[A,B](file = this, cmp = cmp, onDisk = onDisk)
  }


  
  case class UserEFile[A<:Serializable](
    file:String,
    parser:String=>EIterator[A])
  extends EFile[A]
  { //
    // The entries are in a file of their own format;
    // i.e. not a previously serialized file. So need
    // user to provide a parser which reads the file
    // to produce an iterator on the file.

    override def iterator = if (Files.exists(Paths.get(file))) itGood else itBad

    override def itGood = parser(file)

    override def filesize = EFile.getFileSz(file)
    //
    // Use the original file to define file size,
    // rather than the serialized EFile.
  }


  case class InMemoryEFile[A<:Serializable](entries:Vector[A]) extends EFile[A]
  { //
    // The entries are not stored on any file yet.
    // This is the usual case for newly created data.
  
    override def filesize = entries.length * EFile.aveEntrySz

    override def iterator = EIterator(entries)

    override def itGood = iterator

    onDisk = false

  }
 

  case class SerializedEFile[A<:Serializable](file:String) extends EFile[A]
  { //
    // The entries are on a previously created EFile.

    tmpfile = file
 
    override def filename = file

    override def itGood = EIterator[A](file)

    serializedAlready = true
    override def serialized = this
    //
    // This iterator is already serialized. 
  }

    

  object EFile 
  {
    //
    // Function to get tmp file name and size
    //

    def createFileName() = Files.createTempFile(prefix, suffixtmp).toString

    def getFileSz(file:String) = Files.size(Paths.get(file))


    //
    // Functions to create, destroy, or save a temporary EFile.
    // Creation is via serializing to disk or in-memory,
    // depending on estimated file size.
    //

    var szThreshold = 50000L    // force serialization at this size
    var aveEntrySz = 2000       // a rough guess of size per item
    var cap = 100000            // cardinality threshold for internal sorting
    var onDisk = false          // force serialization at all sizes
    val prefix = "synchrony-"
    val suffixtmp = ".eftmp"
    val suffixsav = ".efsaved"


    def createTmpFile[A<:Serializable](
      it:Iterator[A], 
      sz:Long = 0L,
      onDisk:Boolean = onDisk):EFile[A] =
    {
      if (sz < szThreshold && !onDisk) InMemoryEFile(it.toVector)
      else serialize(Files.createTempFile(prefix, suffixtmp).toString, it)
    }


    def destroyTmpFile[A<:Serializable](file:EFile[A]) =
      if (file.filename endsWith suffixtmp) {
        Files.deleteIfExists(Paths.get(file.filename))
        file.serializedAlready = false
        file.tmpfile = ""
      }


    def saveTmpFile[A<:Serializable](name:String, file:EFile[A]):EFile[A] =
    {
      val newname = name + (if (name endsWith suffixsav) "" else suffixsav)

      file.serialized

      Files.move(
        Paths.get(file.filename),
        Paths.get(newname),
        StandardCopyOption.REPLACE_EXISTING)

      file.serializedAlready = false
      file.tmpfile = ""

      SerializedEFile[A](newname)
    }
 

    //
    // Function to serialize an iterator to a file.
    //

    def serialize[A<:Serializable](file:String, it:Iterator[A]):EFile[A] =
    {
      val oos = new ObjectOutputStream(new FileOutputStream(file))
      it.foreach(y => oos.writeObject(y))
      oos.close()

      SerializedEFile[A](file)
    }
  

    //
    // Default ways to open or create an EFile
    //

    def apply[A<:Serializable](file:String):EFile[A] =
    { //
      // file is a previously saved EFile

      def isFile(f:String) = Files.isRegularFile(Paths.get(f))

      val tmpf = file + suffixtmp
      val savf = file + suffixsav

      SerializedEFile[A](
        if (isFile(file)) file
        else if (isFile(tmpf)) tmpf
        else if (isFile(savf)) savf
        else { throw FileNotFound("") })
    }

 
    def apply[A<:Serializable](
      file:String, 
      parser:String=>EIterator[A]):EFile[A] =
    { //
      // file is a user-formatted file. So needs a parser from user.

      UserEFile[A](file, parser)
    }


    def apply[A<:Serializable](
      it:Iterator[A], 
      sz:Long = 0L, 
      onDisk:Boolean = onDisk):EFile[A] =
    { //
      // Data is on an iterator of estimated size sz.

      createTmpFile[A](it, sz, onDisk)
    }


    def fromVector[A<:Serializable](
      entries:Vector[A], 
      onDisk:Boolean = onDisk):EFile[A] = 
    { //
      // Data is an in-memory vector. 

      if (!onDisk) InMemoryEFile[A](entries)
      else InMemoryEFile[A](entries).serialized
    }


    def fromSeq[A<:Serializable](
      entries:Seq[A],
      onDisk:Boolean = onDisk):EFile[A] = 
    { //
      // Data is in an in-memory sequence.

      if (!onDisk) InMemoryEFile[A](entries.toVector)
      else InMemoryEFile[A](entries.toVector).serialized
    }


    //
    // Merging two EFiles, assume their elements are sorted.
    //

    def merge[A<:Serializable,B>:A](
      x:EFile[A],
      y:EFile[A],
      onDisk:Boolean = onDisk)(implicit cmp:Ordering[B]):EFile[A] =
    {
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

      EFile[A](it, x.filesize + y.filesize, onDisk)
    }


    //
    // Sorting an EFile
    //

    def sortWith[A<:Serializable,B>:A](
      file:EFile[A],
      cmp:(B,B)=>Boolean,
      capacity:Int = cap,
      onDisk:Boolean = onDisk):EFile[A] =
    {
      val ordering = new Ordering[A] {
        override def compare(x:A, y:A) = (cmp(x,y), cmp(y,x)) match {
          case (true, false) => -1
          case (false, true) => 1
          case _ => 0
      }  }

      sort(file, capacity, onDisk)(ordering)
    }


    def sort[A<:Serializable,B>:A](
      file:EFile[A],
      capacity:Int = cap,
      onDisk:Boolean = onDisk)(implicit cmp:Ordering[B]):EFile[A] =
    {
      import scala.collection.mutable.Queue

      val groups = file.iterator.grouped(capacity)
      val toMerge = new Queue[EFile[A]](100)
      groups.foreach { g => 
        toMerge enqueue (fromSeq[A](g.sorted(cmp), onDisk))
      }
      
      while (toMerge.length > 1) 
      {
        val x = toMerge.dequeue()
        val y = toMerge.dequeue()
        toMerge enqueue (merge[A,B](x, y, onDisk)(cmp))
        destroyTmpFile(x)
        destroyTmpFile(y)
      }

      toMerge.dequeue()
    }
  }


  //
  // Scala requires serializable classes to be pre-declared.
  // So we create an generic serializable class, Entry,
  // with which you can wrap things you want to serialize.
  //

  @SerialVersionUID(999L)
  case class Entry[A](e:A) extends Serializable

}


/*
 *

import synchrony.iterators.FileCollections._

val a = Entry(10)
val b = Entry(20)
val c = Entry(30)
val ABC = Vector(a,c,b,c,b,a,a,b,c,a)
val ef = EFile(ABC)
val gh = ef.sorted(Ordering.by((x:Entry[Int]) => x.e))
gh.iterator.toVector

val ij = gh.serialized
ij.filesize
ij.iterator.toVector

ij.saveAs("xy")
val mn = EFile[Entry[Int]]("xy")
mn.iterator.toVector

 *
 */


