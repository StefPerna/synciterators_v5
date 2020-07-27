
package synchrony.genomeannot 

/** Provide for Bed-formatted files
 *
 *  The "bed" format is often used in bioinformatics pipelines
 *  processing genome annotations in e.g. ChIP-seq datasets.
 *  The BED format consists of one line per feature, each 
 *  containing 3-12 columns of data, plus optional track
 *  definition lines.  Here we provide support for a simplified
 *  format which ignores track lines and display-related fields
 *  in bed files.
 * 
 *  The format is described in 
 *    https://asia.ensembl.org/info/website/upload/bed.html
 *  which is reproduced here.
 * 
 *  The first three fields in each feature line are required:
 *   1. chrom      - name of the chromosome or scaffold.
 *   2. chromStart - Start position of the feature in
 *                   chromosomal coordinates (i.e. first base is 0).
 *   3. chromEnd   - End position of the feature in chromosomal coordinates
 *  
 *  Nine additional fields are optional. Note that columns cannot 
 *  be empty - lower-numbered fields must always be populated if
 *  higher-numbered ones are used.
 *   4. name       - Label to be displayed under the feature
 *   5. score      - A score between 0 and 1000. 
 *   6. strand     - defined as + (forward) or - (reverse).
 *   7. thickStart - coordinate at which to start drawing the feature
 *                   as a solid rectangle
 *   8. thickEnd   - coordinate at which to stop drawing the feature
 *                   as a solid rectangle
 *   9. itemRgb    - an RGB colour value (e.g. 0,0,255). Only used 
 *                   if there is a track line with the value of itemRgb
 *                   set to "on" (case-insensitive).
 *  10. blockCount - the number of sub-elements (e.g. exons) within the feature
 *  11. blockSizes - the size of these sub-elements
 *  12. blockStarts- the start coordinate of each sub-element
 * 
 *  However, the various ENCODE Bed formats dont follow the above
 *  strictly. Sigh....
 * 
 *
 * Wong Limsoon
 * 27 April 2020
 */



object BedWrapper {
  
  import java.io.{PrintWriter, File, EOFException}

  import synchrony.genomeannot.GenomeAnnot.{LocusLike, GenomeLocus}

  import synchrony.iterators.FileCollections.{
    EIterator, EFile, EFileState, EFileSettings,
    InMemory, Transient, OnDisk}

  var DEBUG = true


  case class BedError(ms:String) extends Throwable



  /** Represent an entry in a Bed-format file.
   *
   *  The six fixed fields correspond to the fixed Bed fields.
   *  A Map[String,Any] is use for the optional/variable Bed fields.
   */

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

    // Default way to display a Bed entry.

    // override def toString: String =
    //   s"Bed{ chrom=${chrom}, chromStart=${chromStart}, chromEnd=${chromEnd}," +
    //   s"name=${name}, score=${score}, strand=${strand}, map=${misc} }"


    override def toString: String = this.bedFormat


    // Default way to serialize a Bed entry.

    def bedFormat: String =
      s"${chrom}\t${chromStart}\t${chromEnd}\t" +
      s"${name}\t${score}\t${strand}" +
      { if (misc.isEmpty) "" else "\t" } +
      misc.map(fv => s"${fv._1}=${fv._2}").mkString("\t")
     


    //
    // Use misc: Map[String,Any] to store miscellanenous info
    // that users want to associate to a Bed entry.
    //


  /** Add a field to misc.
   *
   *  Adding a field to misc. However if the field has the same name
   *  as any of the six fixed fields, update the fixed field instead.
   *
   *  @param k is the field name.
   *  @param v is the field value.
   *  @return the new Bed entry.
   */

    def addMisc(k:String, v:Any): SimpleBedEntry = {
      val ch = (k, v) match {
        case ("chrom", v: String) => v 
        case _ => chrom 
      }
      val cs = (k, v) match {
        case ("chromStart", v: Int) => v
        case _ => chromStart 
      }
      val ce = (k, v) match {
        case ("chromEnd", v: Int) => v
        case _ => chromEnd
      }
      val nm = (k, v) match {
        case ("name", v: String) => v
        case _ => name
      }
      val sc = (k, v) match {
        case ("score", v: Int) => v
        case _ => score
      }
      val st = (k, v) match {
        case ("strand", v: String) => v
        case _ => strand
      }
      val mi = (k, v) match {
        case ("chrom", v: String)   => misc
        case ("chromStart", v: Int) => misc
        case ("chromEnd", v: Int)   => misc
        case ("name", v: String)    => misc
        case ("score", v: Int)      => misc
        case ("strand", v: String)  => misc
        case _                      => misc + (k -> v)
      }
      SimpleBedEntry(ch, cs, ce, nm, sc, st,  mi)
    }



  /** Check whether a field is present in misc.
   *
   *  @param k is the field name to check.
   *  @return whether this field is found in misc.
   */

    def hasMisc(k:String): Boolean = misc.contains(k)



  /** Delete a field.
   *
   *  @param k is the field to delete.
   *  @return the new Bed entry.
   */

    def delMisc(k:String): SimpleBedEntry = 
      SimpleBedEntry(chrom, chromStart, chromEnd, name, score, strand, misc - k)


  /** Retrieve the value of a field. The field is assumed to be present.
   *
   *  @param k is the field to retrieve.
   *  @return the value of the field.
   */
 
    def getMisc[A](k:String): A = { k match {
      case "chrom"      => chrom
      case "chromStart" => chromStart
      case "chromEnd"   => chromEnd
      case "name"       => name
      case "score"      => score
      case "strand"     => name
      case _            => misc(k)
    } }.asInstanceOf[A]



  /** Check whether a field satisfies a given predicate.
   *
   *  @param k is the field to check.
   *  @param chk is the predicate.
   *  @return whether chk(k) holds; false if k is not present.
   */

    def checkMisc[A](
      k: String,
      chk: A => Boolean,
      whenNotFound: Boolean = false)
    : Boolean = misc.get(k) match {

      case None => k match {
        case "chrom"      => chk(chrom.asInstanceOf[A])
        case "chromStart" => chk(chromStart.asInstanceOf[A])
        case "chromEnd"   => chk(chromEnd.asInstanceOf[A])
        case "name"       => chk(name.asInstanceOf[A])
        case "score"      => chk(score.asInstanceOf[A])
        case "strand"     => chk(strand.asInstanceOf[A])
        case _            => whenNotFound 
      } 

      case Some(a) => chk(a.asInstanceOf[A])
    }

    
  /** Erase misc
   *
   *  @return a new Bed entry with all optional fields dropped.
   */

    def eraseMisc(): SimpleBedEntry = 
      SimpleBedEntry(
         chrom, chromStart, chromEnd, name,
         score, strand,  misc.empty)


  /** Overwrite the fields of this Bed entry. If there are
   *  conflicts, the new fields/values take priority.
   *
   *  @param m is the new fields/values.
   *  @return the new Bed entry.
   */

    def overwriteMisc(m: Map[String, Any]): SimpleBedEntry = {

      val ch = m.getOrElse("chrom", chrom).asInstanceOf[String]
      val cs = m.getOrElse("chromStart", chromStart).asInstanceOf[Int]
      val ce = m.getOrElse("chromEnd", chromEnd).asInstanceOf[Int]
      val nm = m.getOrElse("name", name).asInstanceOf[String]
      val sc = m.getOrElse("score", score).asInstanceOf[Int]
      val st = m.getOrElse("strand", strand).asInstanceOf[String]
      val mi = m -- Vector("chrom", "chromStart", "chromEnd",
                          "name", "score", "strand")
  
      SimpleBedEntry(ch, cs, ce, nm, sc, st, misc ++ mi)
    }


  /** Overwrite the fields of this Bed entry. If there are
   *  conflicts, the new fields/values take priority.
   *
   *  @param kf is a function which produces new fields/values.
   *  @return the new Bed entry.
   */

    def overwriteMiscWith(kf: (String, SimpleBedEntry => Any)*)
    : SimpleBedEntry = {

      overwriteMisc { Map(kf:_*) map { case (k, f) => k -> f(this) } }
    }


  /** Merge some new fields into this Bed entry. Note that
   *  when there are conflicts, the fields in this Bed entry
   *  take priority.
   *
   *  @param m is the new fields.
   *  @return the new Bed entry.
   */

    def mergeMisc(m: Map[String, Any]): SimpleBedEntry = {

      val mi = m -- Vector("chrom", "chromStart", "chromEnd",
                          "name", "score", "strand")

      SimpleBedEntry(
         chrom, chromStart, chromEnd, name,
         score, strand,  mi ++ misc)
    }


  /** Merge some new fields into this Bed entry. Note that
   *  when there are conflicts, the fields in this Bed entry
   *  take priority.
   *
   *  @param kf is a function which produces the new fields.
   *  @return the new Bed entry.
   */

    def mergeMiscWith(kf:(String, SimpleBedEntry => Any)*): SimpleBedEntry =
      mergeMisc(Map(kf:_*) map { case (k, f) => k -> f(this) })



  /** Rename the fields of this Bed entry.
   *
   *  @param f is the renaming function
   *  @return the new Bed entry.
   */

    def renameMisc(f: String => String): SimpleBedEntry =
      SimpleBedEntry(
        chrom, chromStart, chromEnd, name, score, strand,
        for((k->v) <- misc) yield f(k)->v
      )


  /** Transform the fields of this Bed entry.
   *
   *  @param f is the transformation function.
   *  @return the new Bed entry.
   */

    def transformMisc(f: Map[String, Any] => Map[String, Any]): SimpleBedEntry =
      SimpleBedEntry(
        chrom, chromStart, chromEnd, name,
        score, strand, f(misc)
      )

  } // End class SimpleBedEntry




  type Bed = SimpleBedEntry



  /** Provide lternative constructors for Bed entries,
   *  as well as orderings on Bed entries.
   */

  object SimpleBedEntry {


  /** Construct a Bed entry from a GenomeLocus.
   *
   * @param g is the GenomeLocus.
   * @return the Bed entry.
   */

    def apply(g: GenomeLocus): SimpleBedEntry = new SimpleBedEntry(

      chrom      = g.chrom,
      chromStart = g.chromStart, 
      chromEnd   = g.chromEnd,
      strand     = "+", 
      name       = "", 
      score      = 0, 
      misc       = Map[String,Any]()
    )

    

  /** Construct a Bed entry, fill in unspecified parameters
   *  using default values.
   *
   *  @param chrom
   *  @param chromStart
   *  @param ChromEnd
   *  @param strand
   *  @param name
   *  @param score are the standard required parameters for Bed entries.
   *  @param misc a Map[String,Any] for optional fields.
   *  @return the Bed entry.
   */

    def apply(
      chrom:      String,
      chromStart: Int,
      chromEnd:   Int,
      strand:     String = "+",
      name:       String = "",
      score:      Int = 0,
      misc:       Map[String,Any] = Map[String,Any]())
    : SimpleBedEntry = new SimpleBedEntry(

      chrom      = chrom, 
      chromStart = chromStart, 
      chromEnd   = chromEnd,
      strand     = strand, 
      name       = name, 
      score      = score, 
      misc       = misc
    )



  /** Provide several orderings on Bed entries.
   *
   *  Strand can be positive (+), negative (-), or dont care (.).
   *  A . strand can be matched with + or - strand. Therefore,
   *  . strand cannot be clustered only with + or only with - strand.
   *  We have to let all three types to mix. Hence, strand should
   *  be the last attribute in genomic ordering.
   */
 
    implicit def ordering[A<:Bed]: Ordering[A] = orderByChromStart

    def orderByChromEnd[A<:Bed]: Ordering[A] =
      Ordering.by(l => (l.chrom, l.chromEnd, l.chromStart, l.strand))

    def orderByChromStart[A<:Bed]: Ordering[A] =
      Ordering.by(l => (l.chrom, l.chromStart, l.chromEnd, l.strand))



  /** Default isBefore predicates for Synchrony iterators have to
   *  be consistent with these default orderings.
   *
   *  @param x
   *  @param y are two loci
   *  @return whether x is before y according to the corresponding ordering.
   */

    def isBefore(x: Bed, y: Bed) = ordering.lt(x, y)
    def isBeforeByChromEnd(x: Bed, y: Bed) = orderByChromEnd.lt(x,y)
    def isBeforeByChromStart(x: Bed, y: Bed) = orderByChromStart.lt(x,y)


  /** Default canSee predicate for Synchrony iterator.
   *
   *  @param n distance constraint
   *  @param x
   *  @param y are two loci
   *  @return x's distance to y is no more than n bp.
   */

    def canSee(n: Int = 1000)(x: Bed, y: Bed) = cond(GenomeLocus.DLE(n))(x,y)


  /** Check whether two given loci satisfy all the given predicates.
   *
   *  @param ps is the list of predicates.
   *  @param x
   *  @param y are the two loci
   *  @return whether x and y are on the same chrom and strand, and
   *     satisfy all the predicates in ps.
   */

    def cond(ps: GenomeLocus.LocusPred*)(x: Bed, y: Bed) = 
      (x.chrom == y.chrom) && SameStrand(x, y) && ps.forall(p => p(x, y))



  /** Predicate to test whether two loci are on the same strand.
   *
   *  @param x 
   *  @param y are the two loci.
   *  @return the predicate
   */

    case object SameStrand extends GenomeLocus.LocusPred {
      def apply(x: LocusLike, y: LocusLike) = (x, y) match {
        case (u: Bed,v: Bed)=>
          (u.strand == v.strand) || (u.strand == ".") || (v.strand == ".")
        case _ => true
      }
    }



  /** The "leftmost" and "rightmost" possible Bed entries.
   */

    val leftSentinel = SimpleBedEntry(GenomeLocus.leftSentinel)
    val rightSentinel = SimpleBedEntry(GenomeLocus.rightSentinel)

  }  // End object SimpleBedEntry



  //
  // Set up  EIterator and EFile for Bed files
  //

  type BedEIterator = EIterator[Bed]

  type BedFile = EFile[Bed]



  /** Constructor for BedFile. Serializer and deserializer for Bed files.
   */

  object BedFile {


    // OpG provides many commonly used aggregate functions.
    // Put a copy here for convenience.

    val OpG = synchrony.iterators.AggrCollections.OpG


    // Bed-formatted file has suffix ".bed".  
    // Change suffix of serialized and saved files to ".bed". 
    // Change serializer and deserializer to the Bed ones.

    val defaultSettingsBedFile = EFile.setDefaultsEFile[Bed]( 
        suffixtmp = ".bed",
        suffixsav = ".bed",
        aveSz = 1000,
        cap = 100000,  
        totalsizeOnDisk = totalsizeOnDiskBedFile _,
        serializer = defaultSerializerBedFile _,
        deserializer = defaultDeserializerBedFile _
    ) 
    


  /** Deserializer for Bed file.
   *
   *  Different Bed files may use the optionally fields differently.
   *  So need a "customization" from user to tell us what to do with
   *  the optional fields.
   *
   *  @param customization is the customization function. 
   *  @param filename is the file to read.
   *  @return the deserialized BedFile.
   */

    def defaultDeserializerBedFile(customization: Bed => Bed)(filename: String)
    : BedEIterator = {

      import scala.io.{Source, BufferedSource}

      def openFile(f: String) = {
        val file = Source.fromFile(f)
        def skipTrack(e: String) = !(e startsWith "track")
        (file, file.getLines.filter(skipTrack))
        
        // Need to return the file handler, so that the file
        // can be closed properly later.
      }

      def closeFile(ois: (BufferedSource, Iterator[String])) = ois._1.close()

      def parseBed(ois: (BufferedSource, Iterator[String])): Bed = {

        // Get the next entry to be parsed

        val e = try ois._2.next()  
                catch {
                  case _ : Throwable => 
                    ois._1.close()
                    throw new EOFException("")
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

        customization(
          SimpleBedEntry(
            chrom, chromStart, chromEnd, name, score, strand, parseMisc(entry)
          )
        )
      }

      EIterator.makeParser(openFile, parseBed, closeFile)(filename)
    }


  /** Parse the optional fields.
   *
   *  In GMQL, Bed entries can have some region meta attributes
   *  associated with them. I modified Bed format to use the 6th
   *  column onwards for encoding these.
   *
   *  @param entry the tab-delimited fields extracted from Bed file.
   *  @return a Map[String,Any] representing these fields and their
   *     values.
   */

    protected def parseMisc(entry: Array[String]): Map[String, Any] = { 

      var n = 5

      val fields = 
        for (m <- entry.drop(6))    // entry(0-5) already processed
        yield { 
          n = n + 1
          val m = entry(n).indexWhere(_ == '=')
          if (m == -1) (n.toString, entry(n))
          else { 
            val m0 = entry(n).take(m)
            val m1 = entry(n).drop(m + 1)
            (m0, try m1.toDouble catch { case _ :Throwable => m1 })
          }
        }

      Map(fields.toIndexedSeq :_*)
    }


  /** Customization for extracting ENCODE narrow peak attributes
   *  from the misc: Map[String,Any] of a Bed entry. Encode Narrow Peak
   *  files have the standard 6 Bed fields, followed by 4 ENCODE narrow
   *  peak-specific fields.
   *
   *  @param bed is the Bed entry that needs customization.
   *  @return the customized Bed entry. Basically, fields "6" to
   *     "9" get renamed to their ENCODE name.
   */

    def toEncodeNPBedFile(bed:Bed): Bed = { 

      def optField(field: => Any): Double = 
        try field.asInstanceOf[String].toDouble 
        catch {case _:Throwable => -1.0 }
      

      val nullEncodeMisc = Map[String,Any]("6" -> -1.0,
                                           "7" -> -1.0,
                                           "8" -> -1.0,
                                           "9" -> -1.0)

      def encodeMisc(misc:Map[String, Any]) = 
        for((k->v) <- nullEncodeMisc ++ misc)
        yield  k match { case "6" => ("signalval", optField(v))
                         case "7" => ("pval", optField(v))
                         case "8" => ("qval", optField(v))
                         case "9" => ("peak", optField(v))
                         case _   => (k, v)
                       }

      bed.transformMisc(encodeMisc)
    }



  /** Default serializer for Bed files.
   *
   *  @param it is the EIterator of the Bed file.
   *  @param filename is name of the output file.
   */

    def defaultSerializerBedFile(it:BedEIterator)(filename:String) = {

      if (DEBUG) println(s"** Serializing Bed file, ${filename}, ...")

      var n = 0
      val oos = new PrintWriter(new File(filename))

      for (e <- it) {
        n = n + 1
        oos.write(e.bedFormat)
        oos.write("\n")

        if (n % 5000 == 0) {

          // Print some tracking message every few thousand items.
 
          oos.flush()
          if (DEBUG) println(s"... # of items written so far = ${n}")
        }
      }

      oos.flush()
      oos.close()

      if (DEBUG) println(s"Total # of items written = ${n}")
    }



  /** Constructor a BedFile from an EFileState.
   *
   *  @param efile is the EFileState.
   *  @return the constructed BedFile.
   */

    def apply(efile:EFileState[Bed]): BedFile = new BedFile(efile) 
    

  /** Construct an in-memory BedFile.
   *
   *  @param entries is the entries of the BedFile.
   *  @param settings is the settings of the BedFile.
   *  @return the constructed BedFile.
   */

    def inMemoryBedFile(
      entries: Vector[Bed],
      settings: EFileSettings[Bed] = defaultSettingsBedFile
    ): BedFile = apply(InMemory(entries, settings))
    


  /** Construct a transient BedFile.
   *
   *  @param entries is the entries of the BedFile.
   *  @param settings is the settings of the BedFile.
   *  @return the constructed BedFile.
   */

    def transientBedFile(
      entries: Iterator[Bed],
      settings: EFileSettings[Bed] = defaultSettingsBedFile
    ): BedFile = apply(Transient(entries, settings))
    


  /** Construct an on-disk BedFile.
   *
   *  @param filename is name of the input file.
   *  @param customization is the customization function,
   *    after deserializing the file.
   *  @param settings is the settings of the BedFile.
   *  @return the constructed BedFile.
   */

    def onDiskBedFile(
      filename: String,
      customization: Bed => Bed = (x:Bed) => x,
      settings: EFileSettings[Bed] = defaultSettingsBedFile
    ): BedFile = apply(OnDisk[Bed](filename, customization, settings))
    


  /** Construct an on-disk ENCODE Narrow Peak BedFile.
   *
   *  @param filename is name of the input file.
   *  @param settings is the settings of the BedFile.
   *  @return the constructed BedFile.
   */

    def onDiskEncodeNPBedFile(
      filename: String,
      settings: EFileSettings[Bed] = defaultSettingsBedFile
    ): BedFile = onDiskBedFile(filename, toEncodeNPBedFile _, settings)
    

 
  /** Total size of BedFile on disk.
   *
   *  @param efobj is the BedFile.
   *  @return its size on disk.
   */

    def totalsizeOnDiskBedFile(efobj:BedFile):Double = efobj.efile match {
      case ef:OnDisk[Bed] => efobj.filesize.toDouble
      case _ => 0
    }

  }  // End object BedFile


  // Put a copy of FileCollections.implicits here for convenience.
   
  val implicits = synchrony.iterators.FileCollections.implicits


} // End object BedWrapper



/**
 * Examples to test BedFile
 *
 
  {{{


import synchrony.genomeannot.BedWrapper._
import synchrony.genomeannot.BedWrapper.BedFile._
import synchrony.genomeannot.BedWrapper.BedFile.OpG._
// import synchrony.iterators.FileCollections._

val dir = "../../synchrony-1/"
val fileA = dir + "test/test-join/TFBS/TFBS-short/files/ENCFF188SZS.bed"
val fileB = dir + "test/test-join/HG19_BED_ANNOTATION/files/TSS.bed"

val bfA = onDiskEncodeNPBedFile(fileA)
val bfB = onDiskBedFile(fileB)

bfA.filesize
bfA.eiterator.length
bfA.eiterator.toVector

for(x <- bfA.eiterator) println(x.chrom)

for(x <- bfA.eiterator) println(x.getMisc[Double]("peak"))


bfA.flatAggregateBy(biggest(_.score))

bfA.flatAggregateBy(count)

bfA(6)

bfA(4)


bfB.filesize
bfB.eiterator.length

val xx = bfB.sorted

xx(6)
xx(4)



xx.flatAggregateBy(biggest(_.score))

   }}}

 *
 */

