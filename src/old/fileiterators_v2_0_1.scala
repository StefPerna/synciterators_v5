


package synchrony.iterators

/** Provide EFile to represent and process files on disk.
 *
 *  An EFile is intended to represent a large file on disk.
 *  It provides functionalities for manipulating, serializing,
 *  deserializing, and sorting the file.  It provides 
 *  Synchrony iterators and aggregate functions on file.
 *
 *
 * Wong Limsoon
 * 26 April 2020
 */



object FileCollections {
  
  import synchrony.iterators.ShadowCollections.Shadowing
  import java.nio.file.{Files, Paths, StandardCopyOption}

  var DEBUG = true


  case class FileNotFound(ms:String) extends Throwable
  case class FileCannotSave(ms:String) extends Throwable


  // Aggregates/OpG provides many commonly used aggregate functions.
  // EIterator provides iterators on large files.
  // LmTrack and ExTrqack provides Synchrony iterators.
  // Put a copy of them here so that user of this EFile
  // module can easily find them.
   
  type Aggregates[A] = synchrony.iterators.AggrCollections.Aggregates[A]
  val OpG = synchrony.iterators.AggrCollections.OpG

  val EIterator = synchrony.iterators.SyncCollections.EIterator
  type EIterator[A] = synchrony.iterators.SyncCollections.EIterator[A]

  val LmTrack = synchrony.iterators.SyncCollections.LmTrack
  type LmTrack[A] = synchrony.iterators.SyncCollections.LmTrack[A]

  val ExTrack = synchrony.iterators.SyncCollections.ExTrack
  type ExTrack[A] = synchrony.iterators.SyncCollections.ExTrack[A]
  

  /** EFileState keeps track of the state of an EFile.
   *
   *  EFile can be on disk, in memory, or is transient.
   *  These states are captured by three subtypes of EFileState.
   */

  sealed trait EFileState[A] {
    val settings: EFileSettings[A]
    def toEFile: EFile[A] = EFile(this)
  }


  /** EFileState when the EFile is on disk.
   *
   *  @param filename is name of the file.
   *  @param customization is a function to customize the entries
   *     of the EFile as they are deserialized from disk.
   *  @param settings records various settings of the EFile.
   */

  final case class OnDisk[A](
    filename: String, 
    customization: A => A,
    override val settings: EFileSettings[A]
  ) extends EFileState[A]


  /** EFileState when the EFile is in memory.
   *
   *  @param entries are the entries of the EFile, loaded in memory.
   *  @param settings records various settings of the EFile.
   */

  final case class InMemory[A](
    entries: Vector[A], 
    override val settings: EFileSettings[A]
  ) extends EFileState[A]


  /** EFileState when the EFile is transient. I.e., the EFile may 
   *  be a temporary result, and it can only be used once, unless 
   *  it gets converted to on-disk or in-memory EFile.
   *
   *  @param entries are the entries of the EFile, to be realized
   *     from an iterator one at a time.
   *  @param settings records various settings of the EFile.
   */
   
  final case class Transient[A](
    entries: Iterator[A], 
    override val settings: EFileSettings[A]
  ) extends EFileState[A]
    


  /** EFileSettings keep various settings of an EFile.
   */

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

    totalsizeOnDisk: EFile[A] => Double,
                            // compute size of EFile, including sub files.

    serializer: (EIterator[A]) => String => Unit,
                            // serializer to be used for writing 
                            // this EFile out.

    deserializer:(A => A) => String => EIterator[A]
                            // deserializer(customization) to be used 
                            // for reading an EFile in. The function
                            // customization is for transforming or
                            // initializing each deserialized element
                            // properly.
  )



  /** Constructor for EFile.
   *
   *  An EFile is intended to represent a large file on disk.
   *  It provides functionalities for manipulating, serializing,
   *  deserializing, and sorting the file.  It provides 
   *  Synchrony iterators and aggregate functions on file.
   *
   *  @param efile is the EFileState of this EFile.
   */

  case class EFile[A](efile: EFileState[A])
  extends Aggregates[A] {


  /** Inherit aggregate functions by defining itC from Aggregates.
   */

    override def itC: Iterator[A] = eiterator



  //
  // Endowing this EFile with Synchrony iterators
  //


  /** @return an EIterator on this EFile.
   */
    def eiterator: EIterator[A] = EFile.eiteratorOf(this)


  /** @return an LmTrack on this EFile.
   */

    def lmTrack: LmTrack[A] = LmTrack(eiterator)


  /** @return an ExTrack of this EFile.
   */

    def exTrack: ExTrack[A] = ExTrack(eiterator)



  //
  // Methods for manipulating and processing EFiles.
  //


  /** Return the n-th element in this EFile.
   *
   *  @param n is position of the element to fetch, starting from 0.
   *  @return the n-th element in this EFile.
   */

    def apply(n: Int): A = EFile.getElement(this, n) 


  /** Select some elements in this EFile.
   *
   *  @param f is the selection predicate.
   *  @return the selected elements of this EFile.
   */

    def filtered(f: A => Boolean): EFile[A] = EFile.filter(this, f)


  /** Process all elements in this EFile using a given function.
   *
   *  @param f is the processing function.
   *  @return the processed result.
   */

    def processedWith[B](f: EIterator[A] => B): B = EFile.run(this)(f)


  /** @return name of this EFile.
   */

    def filename: String = EFile.filenameOf(this)


  /** @return size of this EFile. If this EFile has components that
   *  point to other files, the size of these other files
   *  are ignored.
   */

    def filesize: Long = EFile.filesizeOf(this)


  /** @return size of this EFile on disk, inclusive of subcomponents
   *  kept in separate files.
   */
 
    def totalsizeOnDisk: Long = EFile.totalsizeOnDiskOf(this)


  /** If this EFile is transient, depending on its estimated size,
   *  store it in memory or on disk.  
   *
   *  @return the EFile stored in memory or on disk.
   */

    def stored: EFile[A] = EFile.store(this)


  /** Serialize this EFile to disk.
   *
   *  @return the EFile on disk.
   */
  
    def serialized: EFile[A] = EFile.serialize(this)
      

  /** Delete the EFile, if it is on disk.
   */

    def destruct(): Unit = EFile.destroy(this)


  /** Save the EFile to a file with a new name.
   *
   *  May need to recursively save its elements; this is done by
   *  iterating the specified preparations on these elements.
   *
   *  @param name is the new name.
   *  @param preparations specifies any special processing needed
   *     for saving the elements in this EFile.
   *  @return the EFile with the new name. 
   */
      
    def savedAs(name: String, preparations: Option[A => A] = None): EFile[A] = {

      EFile.saveAs(this, name, preparations)

    }



  /** Merge this EFile with other EFiles, assuming elements on
   *  all the EFiles are already sorted in the same ordering.
   *
   *  @param that* are the other EFiles.
   *  @param onDisk indicates whether to always store the resulting
   *     EFile on disk.
   *  @param cmp is the ordering on elements.
   *  @return the merged EFile.
   */

    def mergedWith
       (that:EFile[A]*)
       (implicit onDisk:Boolean = false, cmp:Ordering[A])
    : EFile[A] = {

      val efobjs = this +: (that.toVector)
      EFile.merge[A](efobjs :_*)(onDisk = onDisk, cmp = cmp)

    }


  /** Sort this EFile.
   *
   * @param cmp is the ordering on elements.
   * @param capacity is the # of elements to keep in memory during the sort.
   * @param onDisk indicates whether to always sort on disk, even small files.
   * @return the sorted EFile.
   */

    def sortedWith(
      cmp: (A, A) => Boolean,
      capacity: Int = efile.settings.cap,
      onDisk: Boolean = false
    ): EFile[A] = {

      EFile.sortWith(this)(cmp = cmp, capacity = capacity, onDisk = onDisk)

    }



  /** Sort this EFile.
   *
   * @param cmp is the ordering on elements.
   * @param capacity is the # of elements to keep in memory during the sort.
   * @param onDisk indicates whether to always sort on disk, even small files.
   * @return the sorted EFile.
   */

    def sorted(
      implicit
        cmp: Ordering[A],
        capacity: Int = efile.settings.cap,
        onDisk: Boolean = false
    ): EFile[A] = {

      EFile.sort[A](this)(cmp = cmp, capacity = capacity, onDisk = onDisk)

    }

  }  // End class EFile




  /** Constructors for EFiles and mmplementation of various EFile methods.
   */
 
  object EFile {

  /** @return default settings for EFiles.
   */

    def setDefaultsEFile[A](
      prefix: String        = "synchrony-",
      suffixtmp: String     = ".eftmp",
      suffixsav: String     = ".efsav",
      aveSz: Int            = 500,
      cardCap: Int          = 2000,
      ramCap: Long          = 100000000L,
      cap: Int              = 200000,         // = (ramCap/aveSz).toInt
      doSampling: Boolean   = true,
      samplingSz: Int       = 30,
      alwaysOnDisk: Boolean = false,

      totalsizeOnDisk: EFile[A] => Double = 
        filesizeOf[A] _ andThen ((x:Long) => x.toDouble),

      serializer: EIterator[A] => String => Unit =
        defaultSerializerEFile[A] _,

      deserializer: (A => A) => String => EIterator[A] =
        defaultDeserializerEFile[A] _ )

    : EFileSettings[A] = {

      EFileSettings[A](
        prefix          = prefix,
        suffixtmp       = suffixtmp,
        suffixsav       = suffixsav,
        aveSz           = aveSz,
        cardCap         = cardCap,
        ramCap          = ramCap,
        cap             = cap,
        doSampling      = doSampling,
        samplingSz      = samplingSz,
        alwaysOnDisk    = alwaysOnDisk,
        totalsizeOnDisk = totalsizeOnDisk,
        serializer      = serializer,
        deserializer    = deserializer
      ) 

    }



  /** Construct an EFile with a given EFileState.
   *
   *  @param efile is the EFileState.
   *  @return the constructed EFile.
   */

    def apply[A](efile: EFileState[A]): EFile[A] = new EFile[A](efile)


  /** Construct an in-memory EFile.
   *
   *  @param entries are the entries of the EFile.
   *  @param settings are the settings of the EFile.
   *  @return the constructed EFile.
   */

    def inMemoryEFile[A](
      entries: Vector[A],
      settings: EFileSettings[A])
    : EFile[A] = {

      new EFile[A](InMemory(entries, settings))

    }
    

    def inMemoryEFile[A](entries: Vector[A]): EFile[A] = {

      new EFile[A](InMemory(entries, setDefaultsEFile[A]()))

    }

    

  /** Construct a transient EFile.
   *
   *  @param entries are the entries of the EFile.
   *  @param settings are the settings of the EFile.
   *  @return the constructed EFile.
   */

    def transientEFile[A](
      entries: Iterator[A],
      settings: EFileSettings[A])
    : EFile[A] = {

      new EFile[A](Transient(entries, settings))

    }


    def transientEFile[A](entries: Iterator[A]): EFile[A] = {

      new EFile[A](Transient(entries, setDefaultsEFile[A]()))

    }



  /** Post-deserialization customization that does nothing.
   *
   *  @param x is an element just deserialized from disk.
   *  @return x without doing anything to it.
   */

    def nullCustomizationEFile[A](x:A) = x


  /** Construct an on-disk EFile.
   *
   *  @param filename is name of the EFile on disk.
   *  @param customization specifies customization needed when the
   *     the elements of this EFile are deserialized from disk.
   *  @param settings are the settings of the EFile.
   *  @return the constructed EFile.
   */

    def onDiskEFile[A](
      filename: String,
      customization: A => A,
      settings: EFileSettings[A])
    : EFile[A] = {

      new EFile[A](OnDisk(filename, customization, settings))

    }


    def onDiskEFile[A](
      filename: String,
      customization: A => A = nullCustomizationEFile[A] _ )
    : EFile[A] = {

      new EFile[A](OnDisk(filename, customization, setDefaultsEFile[A]()))

    }




  /** Default serializer.
   *
   * @param it is an EIterator of an EFile.
   * @param filename is name of the output file.
   *
   * Not very efficient, as the default serializer writes out everything
   * using the default encoder of Scala. User should provide customized 
   * serializer and deserializer for his own data.
   */

    def defaultSerializerEFile[A]
      (it: EIterator[A])
      (filename: String)
    : Unit = { 

      import java.io.{
        ByteArrayOutputStream, ObjectOutputStream, PrintWriter, File
      }
      import java.util.Base64
      import java.nio.charset.StandardCharsets.UTF_8

      if (DEBUG) println(s"** Serializing file = ${filename} ...")

      var n = 0
      val file = new File(filename)
      file.createNewFile()
      val oos = new PrintWriter(file)

      for (e <- it) {
        n = n + 1
        val stream = new ByteArrayOutputStream()
        val ees = new ObjectOutputStream(stream)
        ees.writeObject(e)
        ees.close()
        val encoded = new String(
          Base64.getEncoder().encode(stream.toByteArray), 
          UTF_8
        )
        oos.write(encoded)
        oos.write("\n")

        if (n % 5000 == 0) { 
          oos.flush()
          if (DEBUG) println(s"... # of items written so far = ${n}")
        }
      }

      oos.flush()
      oos.close()

      if (DEBUG) println(s"Total # of items written = ${n}")
    }



  /** Default deserializer.
   *
   *  @param customization specifies customization needed after an
   *     element of this EFile is deserialized from disk.
   *  @param filename is name of the input file.
   *  @return the deserialized EFile.
   *
   *  Not very efficient, as the default serializer and deserializer 
   *  write out and read back everything using the default encoder
   *  of Scala. User should provide own customized ones.
   */

    def defaultDeserializerEFile[A]
        (customization: A => A)
        (filename: String) 
    : EIterator[A] = {

      import scala.io.{ Source, BufferedSource }
      import java.io.{ ObjectInputStream, ByteArrayInputStream, EOFException }
      import java.util.Base64
      import java.nio.charset.StandardCharsets.UTF_8

      def openFile(f: String) = {
        val file = Source.fromFile(f)
        if (DEBUG) { println(s"** Deserializing file = ${f}") }
        (file, file.getLines)
      }

      def parseItem(ois: (BufferedSource, Iterator[String])) = {
        val encoded = 
          try ois._2.next()
          catch { case _ : Throwable => 
                    ois._1.close()
                    throw new EOFException("")
          }
        val bytes = Base64.getDecoder().decode(encoded.trim.getBytes(UTF_8))
        val ees = new ObjectInputStream(new ByteArrayInputStream(bytes))
        val decoded = ees.readObject
        ees.close()
        try customization(decoded.asInstanceOf[A]) 
        catch { 
          case e: Throwable => { ois._1.close(); throw(e) }
        }
      }

      def closeFile(ois: (BufferedSource, Iterator[String])) = ois._1.close()

      EIterator.makeParser(openFile, parseItem, closeFile)(filename)

    }



  /** @param efobj is an EFile.
   *  @return the settings of efobj.
   */

    def settingsOf[A](efobj:EFile[A]) = efobj.efile.settings


  /** @param efobj is an EFile.
   *  @return the serializer of efobj.
   */

    def serializerOf[A](efobj:EFile[A]) = efobj.efile.settings.serializer


  /** @param efobj is an EFile.
   *  @return the customized deserializer of efobj.
   */

    def deserializerOf[A](efobj:EFile[A]) = {

      val deserializer = efobj.efile.settings.deserializer

      val customization = efobj.efile match {
        case ef:OnDisk[A] => ef.customization
        case _ => nullCustomizationEFile[A] _
      }

      deserializer(customization)

    }


  /** @param efobj is an EFile.
   *  @return an EIterator on efobj.
   */

    def eiteratorOf[A](efobj:EFile[A]): EIterator[A] = efobj.efile match {

      case ef: InMemory[A] => EIterator[A](ef.entries)

      case ef: Transient[A] => EIterator[A](ef.entries)

      case ef: OnDisk[A] => {

        def isFile(f:String) = Files.isRegularFile(Paths.get(f))

        val tmpf = ef.filename + ef.settings.suffixtmp
        val savf = ef.filename + ef.settings.suffixsav
        val f = if (isFile(ef.filename)) ef.filename
                else if (isFile(tmpf)) tmpf
                else if (isFile(savf)) savf
                else { throw FileNotFound("") }
        deserializerOf(efobj)(f)
      }

    }


 
  /** Process all elements of an EFile using a given function.
   *
   *  @param efobj is the EFile.
   *  @param f is the processing function.
   *  @return the processed result.
   */

    def run[A, B](efobj: EFile[A])(f: EIterator[A] => B): B = { 

      val it = eiteratorOf(efobj)
      try f(it) finally it.close() 

    }



  /** Return the n-th element of an EFile.
   *
   *  @param efobj is the EFile.
   *  @param n is position of the element to fetch, starting from 0.
   *  @return the n-th element in efobj.
   */

    def getElement[A](efobj: EFile[A], n: Int):A = run(efobj) { it => 

      it.drop(n)
      it.next() 

    }


  /** Select some elements in this EFile.
   *
   *  @param f is the selection predicate.
   *  @return the selected elements of this EFile.
   */

    def filter[A](efobj: EFile[A], f: A => Boolean): EFile[A] = {

      EFile.transientEFile[A](efobj.eiterator.filter(f), efobj.efile.settings)

    }



  /** @param efobj is an EFile.
   *  @return name of this EFile.
   */

    def filenameOf[A](efobj:EFile[A]): String = efobj.efile match {

      case ef: InMemory[A] => ""

      case ef: Transient[A] => ""

      case ef: OnDisk[A] => ef.filename

    }
    


  /** @param efobj is an EFile.
   *  @return size of this EFile. If this EFile has components that
   *  point to other files, the size of these other files are ignored.
   */

    def filesizeOf[A](efobj:EFile[A]): Long = efobj.efile match {

      case ef: InMemory[A] => ef.entries.length * ef.settings.aveSz

      case ef: Transient[A] => 0L

      case ef: OnDisk[A] =>
        try Files.size(Paths.get(ef.filename))
        catch { case _ : Throwable => throw FileNotFound(ef.filename) }

    }



  /** @param efobj is an EFile.
   *  @return size of this EFile on disk, inclusive of subcomponents
   *  kept in separate files.
   */
 
    def totalsizeOnDiskOf[A](efobj: EFile[A]): Long = {

      efobj.efile.settings.totalsizeOnDisk(efobj).toLong

    }


 
  /** If an EFile is transient, depending on its estimated size,
   *  store it in memory or on disk.  
   *
   *  @param efobj is the EFile.
   *  @return the stored EFile.
   */ 

    def store[A](efobj:EFile[A]): EFile[A] = efobj.efile match { 

      case ef: InMemory[A] => efobj

      case ef: OnDisk[A] => efobj

      case ef: Transient[A] => run(efobj) { it =>
      
        val sampling = it.shadow().take(ef.settings.cardCap).toVector

        if (!efobj.efile.settings.alwaysOnDisk && 
            sampling.length < ef.settings.cardCap) { 

          // EFile appears small; keep in memory.

          InMemory(sampling, ef.settings).toEFile

        }
 
        else {

          // EFile appears a bit large; keep on disk

          val filename = Files
                         .createTempFile(
                            ef.settings.prefix, 
                            ef.settings.suffixtmp)
                         .toString
          serializerOf(efobj)(it)(filename)
          OnDisk[A](filename, nullCustomizationEFile[A] _, ef.settings).toEFile
        }
      }
    }



  /** Serialize an Efile to disk
   *
   *  @param efobj is the EFile.
   *  @return the serialized EFile.
   */
  
    def serialize[A](efobj: EFile[A]): EFile[A] = efobj.efile match { 

      // EFile is already on disk. Do nothing 

      case ef: OnDisk[A] => efobj 

      // EFile is in memory or is transient. Store to disk

      case _ => {

        val settings = efobj.efile.settings

        val filename = Files
                       .createTempFile( settings.prefix, settings.suffixtmp)
                       .toString
        
        run(efobj)(serializerOf(efobj)(_)(filename)) 
        OnDisk[A]( filename, nullCustomizationEFile[A] _, settings) .toEFile
      }
    }
      


  /** Delete an EFile, if it is on disk.
   *
   *  @param efobj is the EFile.
   */

    def destroy[A](efobj: EFile[A]): Unit = efobj.efile match { 

      case ef: OnDisk[A] => Files.deleteIfExists(Paths.get(ef.filename))

      case _ => { }

    }



  /** Save an EFile.
   *
   *  Save an EFile to a file with a new name. May need to recursively
   *  save its elements; this is done by iterating some specified 
   *  preparations on these elements.
   *
   *  @param efobj is the File. 
   *  @param name is the new filename.
   *  @param preparations is the preparations needed.
   *  @return the saved EFile.
   */
     
    def saveAs[A](
      efobj: EFile[A], 
      name: String, 
      preparations: Option[A => A] = None)
    : EFile[A] = {

      val settings = efobj.efile.settings

      val prepared = preparations match {

        case Some(prepare) => 
          Transient(
            run(efobj){ it => for(e <- it) yield prepare(e)},
            settings)
          .toEFile

        case None => 
          if (efobj.filename endsWith settings.suffixtmp) efobj
          else Transient(efobj.eiterator, settings).toEFile 

      }
      
      val saved = prepared.serialized

      if (saved.filename != "") {

        val newname = name + (if (name endsWith settings.suffixsav) ""
                              else settings.suffixsav)

        Files.move(
          Paths.get(saved.filename),
          Paths.get(newname),
          StandardCopyOption.REPLACE_EXISTING
        )

        OnDisk[A]( newname, nullCustomizationEFile[A] _, settings) .toEFile
      }

      else throw FileCannotSave(name)

    }



  /** Merging EFiles.
   *
   *  @param efobjs are the EFiles to merge. Their elements are assumed
   *     to be already correctly ordered/sorted.
   *  @param onDisk is set to true to force pure merging on disk.
   *  @param cmp is the ordering on elements.
   *  @return the merged EFile.
   */

    def merge[A]
      (efobjs: EFile[A]*)
      (implicit onDisk: Boolean = false, cmp: Ordering[A])
    : EFile[A] = {

      if (efobjs.length == 0) {
        if (DEBUG) println("**There must be at least one EFile for merging.")
        throw FileNotFound("There must be at least one EFile for merging.")
      }

      else { 
        import scala.collection.BufferedIterator

        type BI = BufferedIterator[A]

        val settings = efobjs(0).efile.settings

        val ordering = new Ordering[BI] {
          override def compare(x:BI, y:BI) = cmp.compare(x.head, y.head) 
        }

        var efs: Vector[BufferedIterator[A]] =
          efobjs
          .toVector
          .filter(ef => ef.eiterator.hasNext)
          .map(ef => if (onDisk) ef.serialized else ef.stored) 
          .map(ef => ef.eiterator.buffered)
          .sorted(ordering)

        val it = new Iterator[A] {

          override def hasNext = (efs.length > 0) && (efs(0).hasNext)

          override def next() = {

            val here = efs(0)
            val rest = efs.drop(1)
            val curr = here.next()
 
            efs = if (here.hasNext) {
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



  /** Sort an EFile.
   *
   *  @param efobj is the EFile to merge.
   *  @param cmp is the ordering on elements.
   *  @param capacity is the # of elements in memory during sorting.
   *  @param onDisk is set to true to force sorting on disk.
   *  @return the sorted EFile.
   */

    def sortWith[A]
      (efobj: EFile[A])
      (cmp: (A, A) => Boolean,
       capacity: Int = efobj.efile.settings.cap,
       onDisk: Boolean = false)
    : EFile[A] = {

      val ordering = new Ordering[A] {
        override def compare(x:A, y:A) = (cmp(x,y), cmp(y,x)) match {
          case (true, false) => -1
          case (false, true) => 1
          case _ => 0
        }
      }

      EFile.sort[A](efobj)(cmp = ordering, capacity = capacity, onDisk= onDisk)
    }



  /** Sort an EFile.
   *
   *  @param efobj is the EFile to merge.
   *  @param cmp is the ordering on elements.
   *  @param capacity is the # of elements in memory during sorting.
   *  @param onDisk is set to true to force sorting on disk.
   *  @return the sorted EFile.
   */

    def sort[A]
      (efobj: EFile[A])
      (implicit 
         cmp: Ordering[A],
         capacity: Int = efobj.efile.settings.cap,
         onDisk: Boolean = false)
    : EFile[A] = {

      //
      // EFile is non-empty. Sort it.
      //

      if (efobj.eiterator.hasNext) {

        val alwaysOnDisk = efobj.efile match { 
          case ef:OnDisk[A] => true
          case _ => onDisk
        }

        run(efobj) { it =>
      
          val settings = efobj.efile.settings

          val estimatedCap =
            if (!settings.doSampling || (capacity != settings.cap)) capacity
            else { 
              import java.util.Base64
              import java.nio.charset.StandardCharsets.UTF_8
              import java.io.{ByteArrayOutputStream, ObjectOutputStream}
              val sampling = it.shadow().take(settings.samplingSz).toVector
              val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
              val oos = new ObjectOutputStream(stream)
              oos.writeObject(sampling)
              oos.close
              val tmp = new String(Base64
                                   .getEncoder()
                                   .encode(stream.toByteArray),
                                   UTF_8)
              val sz = tmp.length 
              val aveSz = sz / (sampling.length max 1)
              val cap = (settings.ramCap / (aveSz max 1)).toInt 
           
              if (DEBUG) 
                println(s"*** cap = ${cap}, sz = ${sz}," +
                        s" aveSz = ${aveSz}, n = ${sampling.length}")

              cap
            }

          var m = 0
  
          val groups =
            for(g <- it.grouped(estimatedCap))
            yield {
              m = m + 1
              if (DEBUG) println(s"*** splitting, m = ${m}") 
              val gsorted = Transient(g.sorted(cmp).iterator, settings).toEFile
              if (alwaysOnDisk) gsorted.serialized else gsorted.stored
            }

          merge[A](groups.toVector :_*)(onDisk = alwaysOnDisk, cmp = cmp)
        }
      }

      //
      // EFile is empty. No need to sort.
      //

      else efobj
    }

    
  } // End object EFile.




  //
  // Make implicit conversion to endow EIterators
  // with aggregate functions.
  //

  object implicits {

    import scala.language.implicitConversions
    import synchrony.iterators.AggrCollections._

    implicit def EFile2EIter[A](efobj: EFile[A]): EIterator[A] =
      efobj.eiterator

    implicit def EFileLiftInMemory[A](ef: InMemory[A]) = ef.toEFile

    implicit def EFileLiftTransient[A](ef: Transient[A]) = ef.toEFile

    implicit def EFileLiftOnDisk[A](ef: OnDisk[A]) = ef.toEFile

    implicit def EIter2AggrIter[A](it: EIterator[A]): AggrIterator[A] =
      AggrIterator[A](it)

    implicit def Vec2AggrIter[A](it: Vector[A]): AggrIterator[A] =
      AggrIterator[A](it)

    implicit def Iter2AggrIter[A](it: Iterator[A]): AggrIterator[A] =
      AggrIterator[A](it)

  }

} // End object FileCollections




/** Examples
 *

  {{{

import synchrony.iterators.FileCollections._
import synchrony.iterators.FileCollections.EFile._
import synchrony.iterators.FileCollections.OpG._
// import synchrony.iterators.FileCollections.implicits._

case class Entry[A](e:A)
implicit def entryOrdering[A](implicit ord:Ordering[A]):Ordering[Entry[A]] = 
Ordering.by((x:Entry[A]) => x.e)


val a = Entry(10)
val b = Entry(20)
val c = Entry(30)
val ABC = Vector(a,c,b,c,b,a,a,b,c,a)

val ef = EFile.inMemoryEFile(ABC)

val gh = ef.sorted(cmp = Ordering.by((x:Entry[Int]) => x.e))

val ij = gh.serialized

ij.filesize

ij.eiterator.toVector

ij.savedAs("xy")

val mn = EFile.onDiskEFile[Entry[Int]]("xy")

mn.eiterator.toVector

mn.eiterator.length

onDiskEFile[Entry[Int]]("xy").sorted

mn.flatAggregateBy(count)


    }}}

 *
 */


