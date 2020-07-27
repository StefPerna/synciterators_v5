
package synchrony
package genomeannot 

//
// Wong Limsoon
// 11 April 2020
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
  import iterators.FileCollections._
  import java.io._
  import java.nio.file.{Files, Paths, StandardCopyOption}

  var DEBUG = true


  case class BedError(ms:String) extends Throwable


  //
  // Simple bed object
  //

  @SerialVersionUID(999L)
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


    def bedFormat =
      s"${chrom}\t${chromStart}\t${chromEnd}\t" +
      s"${name}\t${score}\t${strand}\t" +
      { 
        if (misc.isEmpty) ""
        else "Misc\t" + misc.map(fv => s"${fv._1}=${fv._2}").mkString("\t")
      }


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
         score, strand,  m ++ misc)

    def overwriteMisc(m:Map[String,Any]) =
      SimpleBedEntry(
         chrom, chromStart, chromEnd, name,
         score, strand,  misc ++ m)

    def renameMisc(f:String=>String) =
      SimpleBedEntry(
        chrom, chromStart, chromEnd, name, score, strand,
        for((k->v) <- misc) yield f(k)->v)
  }



  type Bed = SimpleBedEntry



  object SimpleBedEntry {

    def apply(g: GenomeLocus) = 
    {
      new SimpleBedEntry(
        chrom= g.chrom, chromStart= g.chromStart, chromEnd= g.chromEnd,
        strand= "+", name= "", score= 0, misc= Map[String,Any]())
    }

    
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


    case object SameStrand extends GenomeLocus.LocusPred
    {
      def apply(x:LocusLike, y:LocusLike) = (x,y) match
        {case (u:Bed,v:Bed)=> (u.strand == v.strand) || 
                              (u.strand == ".") || 
                              (v.strand == ".")
         case _ => true }
    }


    // 
    // Define the "leftmost" and "rightmost" possible Bed entries.

    val leftSentinel = SimpleBedEntry(GenomeLocus.leftSentinel)
    val rightSentinel = SimpleBedEntry(GenomeLocus.rightSentinel)
  }


  //
  // Set up EFile and EIterator for Bed files
 //

  type BedEIterator = EIterator[Bed]

  type BedFile = EFile[Bed]



  object BedFile
  {
    // Bed-formatted file has suffix ".bed".  
    // Change suffix of serialized and saved files to ".bed". 
    // Change serializer and deserializer to the Bed ones.

    val settingsBedFile: EFileSettings[Bed] = EFileSettings[Bed](
      prefix = "synchrony-",
      suffixtmp = ".bed",
      suffixsav = ".bed",
      aveSz = 500,
      cardCap = 2000,
      ramCap = 100000000L,
      cap = 200000,         // = (ramCap/aveSz).toInt
      doSampling = true,
      samplingSz = 100,
      alwaysOnDisk = false,
      serializer = serializeBedFile _,
      deserializer = deserializeBedFile _) 



    //
    // Serializer and deserializer for Bed-formatted files
    //

    def deserializeBedFile(filename:String): BedEIterator =
    {
      import scala.io.{Source, BufferedSource}

      def openFile(f:String) =
      {
        val file = Source.fromFile(f)
        def skipTrack(e: String) = !(e startsWith "track")
        (file, file.getLines.filter(skipTrack))
      }


      def closeFile(ois:(BufferedSource, Iterator[String])) =
      {
        ois._1.close()
      }


      def parseBed(ois:(BufferedSource, Iterator[String])):Bed =
      {
        // Get the next entry to be parsed

        val e = try { ois._2.next() } 
                catch { 
                  case _ :Throwable => { 
                    ois._1.close() 
                    throw FileEnded("")
                  }
                }

        // Parse a bed formatted entry into a simple bed object.
        // TODO: Complain if the string is not bed formatted.
    
        val entry = e.trim.split("\t")
        val chrom = entry(0)
        val chromStart = entry(1).toInt
        val chromEnd = entry(2).toInt
        val name = entry(3)
        val score = entry(4).toInt
        val strand = entry(5) 

        // In GMQL, Bed entries can have some region meta
        // attributes associated with them. I modified
        // Bed format to use the 6th column onwards for
        // encoding these.

        val misc:Map[String,Any] =
        {
          if ((entry.length > 6) && (entry(6) == "Misc"))
          {
            val fields = 
               for(m <- entry
                        .drop(7)    // Drop entry(0-6); entry(6) is "Misc" 
                        .map(_.split("=")))
               yield
               { 
                 val m0:String = m(0)
                 val m1:Any = 
                   try { m(1).toDouble } catch { case _ : Throwable => 
                   try { m(1).toInt } catch { case _ : Throwable =>
                   m(1) } }
                 (m0, m1)
               }
              Map(fields.toSeq :_*)
            } 
            else Map()
          }

        SimpleBedEntry(chrom, chromStart, chromEnd, name, score, strand, misc)
      }

      EIterator.makeParser(openFile, parseBed, closeFile)(filename)
    }


    def serializeBedFile(filename:String, it:BedEIterator) =
    {
      var n = 0
      val oos = new PrintWriter(new File(filename))

      for(e <- it) 
      {
        n = n + 1
        oos.write(e.bedFormat)
        oos.write("\n")

        if (n % 3000 == 0) 
        { //
          // Print some tracking message every few thousand items.
 
          oos.flush()
          if (DEBUG) println(s"*** n = ${n}")
        }
      }

      oos.flush()
      oos.close()
    }


    // 
    // Function for creating BedFile objects.
    // Use these instead of the EFile ones.
    //


    def apply(efile:EFileState[Bed]) = new BedFile(efile) 
    
    
    def inMemoryBedFile(
      entries: Vector[Bed],
      settings: EFileSettings[Bed] = settingsBedFile) =
    {
      apply(InMemory(entries, settings))
    }


    def transientBedFile(
      entries: Iterator[Bed],
      settings: EFileSettings[Bed] = settingsBedFile) =
    {
      apply(Transient(entries, settings))
    }


    def onDiskBedFile(
      filename: String,
      settings: EFileSettings[Bed] = settingsBedFile) =
    {
      apply(OnDisk(filename, settings))
    }
  }

}




/*
 * Examples to test BedFile
 *
 

import synchrony.genomeannot.BedWrapper._
import synchrony.genomeannot.BedWrapper.BedFile._
import synchrony.iterators.FileCollections._

val dir = "../synchrony-1/"
val fileA = dir + "test/test-join/TFBS/TFBS-short/files/ENCFF188SZS.bed"
val fileB = dir + "test/test-join/HG19_BED_ANNOTATION/files/TSS.bed"

val bfA = onDiskBedFile(fileA)
val bfB = onDiskBedFile(fileB)

bfA.filesize
bfA.iterator.length
bfA.iterator.toVector

for(x <- bfA.iterator) println(x.chrom)


import synchrony.iterators.FileCollections.implicits._

for(x <- bfA) println(x.chrom)  // implicit conversion of bfA to bfA.iterator


import synchrony.iterators.AggrCollections._
import synchrony.iterators.AggrCollections.OpG._

bfA.flatAggregateBy(biggest((x:Bed) => x.score))

bfA.flatAggregateBy[Bed,Double](count)

bfA(6)

bfA(4)


bfB.filesize
bfB.iterator.length

val xx = bfB.sorted


xx.flatAggregateBy(biggest((x:Bed) => x.score))

xx.iterator.flatAggregateBy(biggest((x:Bed) => x.score))



 *
 */

