
package synchrony
package genomeannot 

//
// Wong Limsoon
// 4 April 2020
//
// The "bed" format is often used in bioinformatics pipelines
// processing genome annotations in e.g. ChIP-seq datasets.
// The BED format consists of one line per feature, each 
// containing 3-12 columns of data, plus optional track
// definition lines.  Here we provide support for a simplified
// format which ignores track lines and display-related fields
// in bed files.
//
// The format is described in 
//   https://asia.ensembl.org/info/website/upload/bed.html
// which is reproduced here.
//
// The first three fields in each feature line are required:
//  1. chrom      - name of the chromosome or scaffold.
//  2. chromStart - Start position of the feature in
//                  chromosomal coordinates (i.e. first base is 0).
//  3. chromEnd   - End position of the feature in chromosomal coordinates
// 
// Nine additional fields are optional. Note that columns cannot 
// be empty - lower-numbered fields must always be populated if
// higher-numbered ones are used.
//  4. name       - Label to be displayed under the feature
//  5. score      - A score between 0 and 1000. 
//  6. strand     - defined as + (forward) or - (reverse).
//  7. thickStart - coordinate at which to start drawing the feature
//                  as a solid rectangle
//  8. thickEnd   - coordinate at which to stop drawing the feature
//                  as a solid rectangle
//  9. itemRgb    - an RGB colour value (e.g. 0,0,255). Only used 
//                  if there is a track line with the value of itemRgb
//                  set to "on" (case-insensitive).
// 10. blockCount - the number of sub-elements (e.g. exons) within the feature
// 11. blockSizes - the size of these sub-elements
// 12. blockStarts- the start coordinate of each sub-element
//

object BedWrapper {
  
  import genomeannot.GenomeAnnot._
  import iterators.SyncCollections._
  import co.SimpleCOFile._

  case class BedError(ms:String) extends Throwable


  //
  // Simple bed object
  //

  @SerialVersionUID(666L)
  case class SimpleBedEntry(
    override val chrom: String,
    override val chromStart: Int,
    override val chromEnd: Int,
    name: String,
    score: Int, 
    strand: String,
    misc:Map[String,Any]) 
  extends LocusLike with Serializable
  {
    val locus = GenomeLocus(chrom, chromStart, chromEnd)

    // override def toString =
    //   s"Bed{ chrom=${chrom}, chromStart=${chromStart}, chromEnd=${chromEnd}," +
    //   s"name=${name}, score=${score}, strand=${strand}, map=${misc} }"

    override def toString =
      s"${chrom}\t${chromStart}\t${chromEnd}\t${name}\t${score}\t${strand}\t${misc}"

    //
    // Use maps to store miscellanenous info
    // that users want to associate to a Bed entry.

    def addMisc(k:String, v:Any) =
    {
      val ch = (k, v) match {
                        case ("chrom", v:String) => v 
                        case _ => chrom }

      val cs = (k, v) match {
                        case ("chromStart", v:Int) => v
                        case _ => chromStart }

      val ce = (k, v) match {
                        case ("chromEnd", v:Int) => v
                        case _ => chromEnd}

      val nm = (k, v) match {
                        case ("name", v:String) => v
                        case _ => name}

      val sc = (k, v) match {
                        case ("score", v:Int) => v
                        case _ => score}
     
      val st = (k, v) match {
                        case ("strand", v:String) => v
                        case _ => strand}
     
      val mi = (k, v) match {
                        case ("chrom", v:String) => misc
                        case ("chromStart", v:Int) => misc
                        case ("chromEnd", v:Int) => misc
                        case ("name", v:String) => misc
                        case ("score", v:Int) => misc
                        case ("strand", v:String) => misc
                        case _ => misc + (k -> v) }
      
      SimpleBedEntry(ch, cs, ce, nm, sc, st,  mi)
    }


    def hasMisc(k:String) = misc.contains(k)


    def delMisc(k:String) = 
      SimpleBedEntry(
         chrom, chromStart, chromEnd, name,
         score, strand,  misc - k)     


    def getMisc[A](k:String) = k match {
      case "chrom" => chrom.asInstanceOf[A]
      case "chromStart" => chromStart.asInstanceOf[A]
      case "chromEnd" => chromEnd.asInstanceOf[A]
      case "name" => name.asInstanceOf[A]
      case "score" => score.asInstanceOf[A]
      case "strand" => name.asInstanceOf[A]
      case _ => misc(k).asInstanceOf[A]
    }


    def checkMisc[A](k:String, chk:A=>Boolean) =
      misc.get(k) match {
        case None => k match {
          case "chrom" => chk(chrom.asInstanceOf[A])
          case "chromStart" => chk(chromStart.asInstanceOf[A])
          case "chromEnd" => chk(chromEnd.asInstanceOf[A])
          case "name" => chk(name.asInstanceOf[A])
          case "score" => chk(score.asInstanceOf[A])
          case "strand" => chk(strand.asInstanceOf[A])
          case _ => false } 
        case Some(a) => chk(a.asInstanceOf[A]) }

    
    def eraseMisc() = 
      SimpleBedEntry(
         chrom, chromStart, chromEnd, name,
         score, strand,  misc.empty)

    def mergeMisc(m:Map[String,Any]) =
      SimpleBedEntry(
         chrom, chromStart, chromEnd, name,
         score, strand,  misc ++ m)

    def overwriteMisc(m:Map[String,Any]) =
      SimpleBedEntry(
         chrom, chromStart, chromEnd, name,
         score, strand,  m ++ misc)

    def renameMisc(f:String=>String) =
      SimpleBedEntry(
        chrom, chromStart, chromEnd, name, score, strand,
        for((k->v) <- misc) yield f(k)->v)
  }


  object SimpleBedEntry {
    def apply(g: GenomeLocus) = new SimpleBedEntry(
      chrom= g.chrom,
      chromStart= g.chromStart,
      chromEnd= g.chromEnd,
      strand= "+",
      name= "",
      score= 0,
      misc= Map[String,Any]())

    
    def apply(
      chrom:String,
      chromStart:Int,
      chromEnd:Int,
      strand:String = "+",
      name:String = "",
      score:Int = 0,
      misc:Map[String,Any] = Map[String,Any]()) = 
    {
      new SimpleBedEntry(
        chrom= chrom, chromStart= chromStart, chromEnd= chromEnd,
        strand= strand, name= name, score= score, misc= misc)
    }


    type Bed = SimpleBedEntry


    //
    // Strand can be positive (+), negative (-), or dont care (.).
    // A . strand can be matched with + or - strand. Therefore,
    // . strand cannot be clustered only with + or only with - strand.
    // We have to let all three types to mix. Hence, strand should
    // be the last attribute in genomic ordering.
    //
    
    implicit def ordering[A<:Bed]:Ordering[A] = orderByChromStart

    def orderByChromEnd[A<:Bed]:Ordering[A] =
      Ordering.by(l => (l.chrom, l.chromEnd, l.chromStart, l.strand))

    def orderByChromStart[A<:Bed]:Ordering[A] =
      Ordering.by(l => (l.chrom, l.chromStart, l.chromEnd, l.strand))


    def isBefore(x:Bed, y:Bed) = ordering.lt(x, y)
    def isBeforeByChromEnd(x:Bed, y:Bed) = orderByChromEnd.lt(x,y)
    def isBeforeByChromStart(x:Bed, y:Bed) = orderByChromStart.lt(x,y)

    def canSee(n:Int = 1000)(x:Bed, y:Bed) = cond(GenomeLocus.DLE(n))(x,y)

    def cond(ps:GenomeLocus.LocusPred*)(x:Bed, y:Bed) = 
    { //
      // Ensure x and y are on the same strand and chrom.

      (x.chrom==y.chrom) && SameStrand(x, y) && ps.forall(p => p(x,y))
    }


    case object SameStrand extends GenomeLocus.LocusPred {
      def apply(x:LocusLike, y:LocusLike) = (x,y) match
        {case (u:Bed,v:Bed)=> (u.strand == v.strand) || 
                              (u.strand == ".") || (v.strand == ".")
         case _ => true }
    }


    // 
    // Define the "leftmost" and "rightmost" possible Bed entries.

    val leftSentinel = SimpleBedEntry(GenomeLocus.leftSentinel)
    val rightSentinel = SimpleBedEntry(GenomeLocus.rightSentinel)
  }


  
  //
  // Bed-formatted or serialized Bed-iterator files
  //


  trait BedFile 
  {
    //
    // An iterator over this BedFile.
    // This is the only thing that must 
    // be provided when creating BedFile. 
    //

    def iterator: Iterator[SimpleBedEntry]

    //
    // Name and size of this BedFile when it gets serialized.
    //

    val filename = BedFile.createFileName()
    
    def filesize = BedFile.getFileSz(filename) 


    //
    // Function to serialize this or other iterators
    //

    private var serializedAlready = false

    def serialized = 
    {
      if (serializedAlready) SerializedBedFile(filename)
      else {
        val bedfile = BedFile.serialize(filename, iterator)
        serializedAlready = true
        bedfile
      }
    }
  }


  
  case class SimpleBedFile(file:String) extends BedFile
  {
    private def entryParser(e: String): SimpleBedEntry = 
    { //
      // Parse a bed formatted entry into a simple bed object.
      // TODO: Complain if the string is not bed formatted.
    
      val entry = e.trim.split("\t")
      val chrom = entry(0)
      val chromStart = entry(1).toInt
      val chromEnd = entry(2).toInt
      val name = entry(3)
      val score = entry(4).toInt
      val strand = entry(5) 
      return SimpleBedEntry(
        chrom, chromStart, chromEnd, name, score, strand, Map())
    }

    private def skipTrack(e: String) = !(e startsWith "track")

    override def iterator:Iterator[SimpleBedEntry] = 
      UntypedCOFile(
        file,
        (e: String) => entryParser(e),
        (e: String) => skipTrack(e))
      .iterator 

    override def filesize = BedFile.getFileSz(file)
    //
    // Use the original bed file to define file size,
    // rather than the serialized BedFile.
  }


  case class InMemoryBedFile(entries:Vector[SimpleBedEntry]) extends BedFile
  {
    override def iterator:Iterator[SimpleBedEntry] = entries.iterator
  }
 

  case class SerializedBedFile(file:String) extends BedFile
  {
    import java.io._

    override val filename = file

    override def iterator = new Iterator[SimpleBedEntry]
    {
      private val ois = new ObjectInputStream(new FileInputStream(file))

      private var nextEntry = 
        try   { Some(ois.readObject.asInstanceOf[SimpleBedEntry]) }
        catch { case e:Throwable => { ois.close; None } }

      override def hasNext = nextEntry != None 

      override def next() = 
      {
        val currEntry = nextEntry match {
          case Some(e) => e
          case None => throw BedError("BedWrapper.SerializedBedFile: EoF.")
        }

        nextEntry = 
          try   { Some(ois.readObject.asInstanceOf[SimpleBedEntry]) }
          catch { case e:Throwable => { ois.close; None } }

        currEntry
      }
    }

    override def serialized = this
    //
    // This iterator is already serialized. 
  }

    

  object BedFile 
  {
    //
    // Function to get tmp file name and size
    //

    def createFileName() =
    {
      import java.nio.file.Files
      Files.createTempFile("synchrony-", ".bedserialized").toString
    }

    def getFileSz(file:String) =
    {
      import java.nio.file.Files
      import java.nio.file.Paths
      Files.size(Paths.get(file))
    }


    //
    // Function to produce a new temporary BedFile,
    // either via serializing to disk or in-memory
    // depending on estimated file size.
    //

    val szThreshold = 5000000L
    val prefix = "synchrony-"
    val suffix = ".bedserialized"

    def createTmpFile(it:Iterator[SimpleBedEntry], estimatedSz:Long = 0L) =
    {
      import java.nio.file.Files

      if (estimatedSz < szThreshold) InMemoryBedFile(it.toVector)
      else serialize(Files.createTempFile(prefix, suffix).toString, it)
    }


    //
    // Function to serialize an iterator to a file.
    //

    def serialize(file:String, it:Iterator[SimpleBedEntry]) =
    {
      import java.io._

      val oos = new ObjectOutputStream(new FileOutputStream(file))
      it.foreach(oos.writeObject(_))
      oos.close

      SerializedBedFile(file)
    }
  

    //
    // Default ways to open a BedFile
    //

    def apply(file:String) =
    {
      if (file endsWith suffix) SerializedBedFile(file)
      else SimpleBedFile(file)
    } 


    def apply(it:Iterator[SimpleBedEntry], estimatedSz:Long = 0L) =
      createTmpFile(it, estimatedSz)
  }

}



/*
 * Example
 *

import synchrony.genomeannot.GenomeAnnot._
import synchrony.genomeannot.BedWrapper._
import synchrony.iterators.SyncCollections._

type Bed = SimpleBedEntry

def genes = LmTrack(SimpleBedFile("ncbiRefSeqCurated.txt").iterator)
def peaks = SimpleBedFile("88934_sort_peaks.narrowPeak.bed").iterator
//
// NOTE: It is impt to use "def" above. This way, peaks and genes
// are instantiated fresh each time their are invoked. This way,
// we get fresh iterators on the two files at each invocation. 
// If "val" is used instead, peaks and genes are bound immediately
// to the respective iterators (which are traversable once only);
// this is incorrect if you want to use genes and peaks several times.


def connect(ex:Boolean = true): (LmTrack[Bed],SyncedTrack[Bed,Bed])  = {
  val bf = GenomeLocus.isBefore _
  val cs = GenomeLocus.startNotFar _
  val mygenes = genes
  val mypeaks = mygenes.sync(peaks, bf, cs, ex)
  return (mygenes, mypeaks) }


def eg1 = {
  val (mygenes, mypeaks) = connect()
  for(g <- mygenes; p = mypeaks.syncWith(g) if p != Nil) yield (p, g) }


def eg2 = { 
  val (mygenes, mypeaks) = connect()
  for(g <- mygenes; q <- mypeaks.syncWith(g)) yield (q, g) }


def eg3(num:Int) = {
  val (mygenes, mypeaks) = connect(false)
  for(g <- mygenes.take(num); 
      p = mypeaks.syncWith(g) if p != Nil) 
  yield (p, g) }


def result[A](eg:Iterator[A]) = {
  var count = 0
  eg foreach (x => { count += 1; println(s"${count}:  ${x}") })
  count
}
 

result(eg1)
result(eg2)
result(eg3(5000))

 *
 */


