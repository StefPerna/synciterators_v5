

package gmql

/** Version 5
 *
 *  Wong Limsoon
 *  30 Jan 2022
 */


object BEDModel {

/** Organization:
 *
 *  object BEDEntry
 *  // Contains: Bed
 *
 *  object BEDFile  = gmql.BEDFile         
 *  // Contains: BedFile, BEDFILE, OCollToBedFile
 */

  import scala.language.implicitConversions
  import gmql.GenomeLocus.Locus


  type Bool = Boolean            // Shorthand
  type META = Map[String,Any]    // Shorthand


  def TMP = dbmodel.DBFile.TMP   // Folder for temporary files



  object BEDEntry {

    /** The "BED" format is often used in bioinformatics pipelines processing
     *  genome annotations in e.g. ChIP-seq datasets. The BED format consists
     *  of one line per feature, each containing 3-12 columns of data, plus
     *  optional track definition lines.  Here we provide support for a
     *  simplified format which ignores track lines and display-related
     *  fields in bed files.
     * 
     *  The BED format is described in 
     *    https://asia.ensembl.org/info/website/upload/bed.html
     *  which is reproduced here.
     * 
     *  The first three fields in each feature line are required:
     *   1. chrom      - name of the chromosome or scaffold.
     *   2. chromStart - Start position of the feature in
     *                   chromosomal coordinates (i.e. first base is 0).
     *   3. chromEnd   - End position of the feature in chromosomal coordinates
     *  
     *  Nine additional fields are optional. Note that columns cannot be empty;
     *  lower-numbered fields must always be populated if higher-numbered ones
     *  are used.
     *   4. name       - Label to be displayed under the feature
     *   5. score      - A score between 0 and 1000. 
     *   6. strand     - defined as + (forward) or - (reverse).
     *   7. thickStart - coordinate at which to start drawing the feature
     *                   as a solid rectangle
     *   8. thickEnd   - coordinate at which to stop drawing the feature
     *                   as a solid rectangle
     *   9. itemRgb    - an RGB colour value (e.g. 0,0,255). Only used 
     *                   if there is a track line with the value of item
     *                   set to "on" (case-insensitive).
     *  10. blockCount - the # of sub-elements (e.g. exons) within the feature
     *  11. blockSizes - the size of these sub-elements
     *  12. blockStarts- the start coordinate of each sub-element
     * 
     *  However, various ENCODE BED formats don't follow the above strictly.
     *  Sigh....
     */


    case class Bed(
      loc:    Locus,
      name:   String,
      score:  Int,
      strand: String,
      misc:   META)
    {

      @inline def chrom = loc.chrom
      @inline def start = loc.start
      @inline def end   = loc.end


      /** Default way to display a Bed entry
       */

      override def toString: String =
        s"Bed{ chrom=${chrom}, start=${start}, end=${end}," +
        s"name=${name}, score=${score}, strand=${strand}, misc=${misc} }"


      /** Update fields.
       */

      def newLoc(chrom: String, start: Int, end: Int): Bed =
        Bed(Locus(chrom, start, end), name, score, strand, misc)
  
      def newEnd(end: Int) = newLoc(chrom, start, end)

      def newStart(start: Int) = newLoc(chrom, start, end)

      def newName(name: String): Bed      = Bed(loc, name, score, strand, misc)

      def newScore(score: Int): Bed       = Bed(loc, name, score, strand, misc)

      def newStrand(strand: String): Bed  = Bed(loc, name, score, strand, misc)

      def newMeta(k: String, v: Any): Bed = (k, v) match {
        case ("chrom", v: String)  => newLoc(v, start, end)
        case ("start", v: Int)     => newLoc(chrom, v, end)
        case ("end", v: Int)       => newLoc(chrom, start, v)
        case ("name", v: String)   => newName(v)
        case ("score", v: Int)     => newScore(v)
        case ("strand", v: String) => newStrand(v)
        case _ => Bed(loc, name, score, strand, misc + (k -> v))
      }

      def ++(fields: (String, Any)*): Bed = {
        val kv = Map(fields: _*)
        val lc = {
          val ch = kv.getOrElse("chrom", chrom).asInstanceOf[String]
          val st = kv.getOrElse("start", start).asInstanceOf[Int]
          val en = kv.getOrElse("end", end).asInstanceOf[Int]
          Locus(ch, st, en)
        }
        val nm = kv.getOrElse("name", name).asInstanceOf[String]
        val sc = kv.getOrElse("score", score).asInstanceOf[Int]
        val st = kv.getOrElse("strand", strand).asInstanceOf[String]
        val mi = kv -- Vector("chrom", "start", "end", "name", "score", "strand")
        Bed(lc, nm, sc, st, misc ++ mi) 
      }
  
      def ++(meta: META): Bed = Bed(loc, name, score, strand, misc ++ meta)



      /** Check whether a field is present.
       */

      def hasMeta(k: String): Bool = misc.contains(k)


      /** Retrieve a field.
       */

      def apply(k: String): Any     = getMeta[Any](k)

      def getInt(k: String): Int    = getMeta[Int](k)

      def getDbl(k: String): Double = getMeta[Double](k)

      def getStr(k: String): String = getMeta[String](k)

      def getMeta[A](k: String): A  = {
        val v = misc.get(k) match {
          case Some(a) => a
          case None    => k match {
            case "chrom"  => chrom
            case "start"  => start
            case "end"    => end
            case "name"   => name
            case "score"  => score
            case "strand" => strand
            case _ => throw new java.util.NoSuchElementException()
          }
        }
        v.asInstanceOf[A]
      }


      /** Check whether a field satisfies a given predicate.
       */

      def checkMeta[A](
        k:     String,
        check: A => Bool,
        error: Bool = false): Bool =
      {
        try check(getMeta(k)) catch { case _: Throwable => error }
      }
 

      /** Delete and replace fields.
       */

      def delMeta(k: String): Bed  = Bed(loc, name, score, strand, misc - k)

      def delMeta(): Bed = Bed(loc, name, score, strand, misc.empty)

      def replaceMeta(fields: META): Bed = Bed(loc, name, score, strand, fields)

      def renameMeta(rename: String => String): Bed =
        Bed(loc, name, score, strand, misc map { case (k,v) => rename(k) -> v })

      def renamedMeta(rename: String => String): META =
        misc map { case (k,v) => rename(k) -> v }


      /** Check whether this and another [[Bed]] are on the same strand.
       */

      def sameStrand(b: Bed): Bool = Bed.sameStrand(this, b)
      

      /** Check whether it is on positive/negative strand.
       *  Note that BED entries might use "." (or other letters) to
       *  mean there is no strand information. For these cases, the
       *  practice is to allow it to match both positive/negative strand.
       */

      def isPlusStrand: Bool = strand != "-"

      def isNegativeStrand: Bool = strand != "+"


      /** Start and end points represented as [[Bed]]
       */

      def chromStart: Bed = Bed(loc.chromStart, strand)

      def chromEnd: Bed   = Bed(loc.chromEnd, strand)

    }   // End case class Bed



    object Bed
    {

      /** Alternative constructor for [[Bed]], autofilling some fields.
       */

      def apply(loc: Locus, strand: String = "+", misc: META = Map()): Bed =
        Bed(loc = loc, name = "", score = 0, strand = strand, misc = misc)


      /** Test whether two [[Bed]] are on the same strand. A strand can
       *  be "+", "-", or something else. The something else (e.g. ".")
       *  can match both "+" and "-".
       */

      val sameStrand: (Bed,Bed) => Bool = {
        (a: Bed, b: Bed) => (a.chrom == b.chrom) && { 
                             (a.strand == b.strand) ||
                             (a.strand != "+" && b.strand != "-") ||
                             (b.strand != "+" && a.strand != "-") }
      }

      val sameLocusNStrand: (Bed,Bed) => Bool = {
        (a: Bed, b: Bed) => (a.loc sameLocus b.loc) && (a sameStrand b)
      }

      val isPlusStrand: (Bed,Bed) => Bool = {
        (a: Bed, b: Bed) => a.strand != "-" && b.strand != "-"
      }

      val isMinusStrand: (Bed,Bed) => Bool = {
        (a: Bed, b: Bed) => a.strand != "+" && b.strand != "+"
      }


      /** Lift predicates on [[Locus]] to [[Bed]]
       */

      def lift(pred: (Locus,Locus) => Bool*): (Bed,Bed) => Bool = 
        (a: Bed, b: Bed) => pred.forall(_(a.loc, b.loc))

      def requires(pred: (Bed,Bed) => Bool*): (Bed,Bed) => Bool = 
        (a: Bed, b: Bed) => pred.forall(_(a, b))
    

      /** Ordering on [[Bed]] is based on [[Locus]],
       *  viz. [[(chrom, start, end)]].
       */

      implicit val ordering: Ordering[Bed] = Ordering.by(_.loc)


    }  // End object Bed


    /** Function for implicit conversion from [[Bed]] to [[Locus]]
     */

    implicit def BedToLocus(b: Bed): Locus = b.loc

  }  // End object BEDEntry





  object BEDFile {

    import dbmodel.DBModel.CloseableIterator.CBI
    import dbmodel.DBModel.OrderedCollection.{ OColl, Key }
    import dbmodel.DBFile.{ OFile, OCollToFile, OTransient, PARSER, FORMATTER }
    import BEDEntry.Bed


    type BEDITERATOR = CBI[Bed]

    type BEDFILE = OFile[Bed,Locus]

    type SCHEMA  = Vector[(String, String => Any)]
                        // BED files are not self-describing.
                        // Schemas are a way for users to tell which
                        // fields correspond to what. An entry "f -> get"
                        // in a schema vector says the field name is "f",
                        // and a string representing a value of "f" can be
                        // parsed using the function "get". Schemas describe
                        // the 6th field onwards of a BED file, as the 0th
                        // to 5th fields have fixed meaning (and thus need
                        // not be specified in a schema vector); thus, the
                        // 0th entry in a schema vector correspond to the
                        // 6th field in a BED file.


    implicit class OCollToBedFile(entries: OColl[Bed,Locus])
    {
      def toBedFile: BEDFILE = BedFile.transientBedFile(entries.cbi)
    }



    object BedFile
    {
      /** Key for BedFile
       */

      val bedFileKey: Key[Bed,Locus] = Key(_.loc, Locus.ordering)


      /** Constructors for BedFile.
       */

      def encode(filename: String): BEDFILE = encode(filename, TMP)

      def encode(filename: String, folder: String): BEDFILE = 
      {
        // Assume filename is an ENCODE BED file.
        val fname = OFile.mkFileName(filename, folder).toString
        OFile(bedFileKey, fname, encodeParser, formatter)
      }


      def apply(filename: String): BEDFILE = BedFile(filename, TMP)

      def apply(filename: String, folder: String): BEDFILE = 
      {
        // Assume filename is a BED file. 
        val fname = OFile.mkFileName(filename, folder).toString
        OFile(bedFileKey, fname, parser, formatter)
      }


      /** Sometimes, it is handy to have a transient BED file for
       *  temporary use without writing it out to disk.
       */

      def transientBedFile(entries: IterableOnce[Bed]): BEDFILE = {
        OTransient[Bed,Locus](bedFileKey, CBI(entries), 
                              parser, formatter, OFile.destructorOFile _)
      }

      def emptyBedFile: BEDFILE = {
        OTransient[Bed,Locus](bedFileKey, CBI(), 
                              parser, formatter, OFile.destructorOFile _)
      }


      /** Copy a BED file. Provide a deep-copy option.
       */

      def deepCopy
        (bf: BEDFILE, filename: String, folder: String = TMP)
        (implicit deep: Boolean = true): BEDFILE =
      {
        if (deep) transientBedFile(bf.cbi).saveAs(filename, folder)
        else bf.saveAs(filename, folder)
      }

      /** Formatter for [[Bed]]. Normal BED format does not use explicit
       *  field name. A header is used here to introduce field name.
       */

      val formatter: FORMATTER[Bed] = (options: String) =>
      {
        var labels: Seq[String] = null
  
        def typeOf(e: Any): String = e match {
          case _: Int    => "Int"
          case _: Double => "Double"
          case _ => "String"
        }

        def bedEntry(b: Bed) =
          s"${b.chrom}\t${b.start}\t${b.end}\t" +
          s"${b.name}\t${b.score}\t${b.strand}" +
          { if (labels.isEmpty) "" else "\t" } +
          labels.map(l => b.misc(l).toString).mkString("\t")
  
        def bedHeader(b: Bed) =
          s"##wonglimsoon@nus##" +
          { if (labels.isEmpty) "" else "\t" } +
          labels.map(l => s"${l}:${typeOf(b.misc(l))}").mkString("\t")

        def format(b: Bed, position: Int = 1): String =
        {
          (position == 0) match {
            case true  =>                     // 1st entry. Need header.
              labels = b.misc.keys.toSeq.sorted
              s"${bedHeader(b)}\n${bedEntry(b)}"
            case false => bedEntry(b)         // Other entries. No need header.
          }
        }

        format _
      }
  

      /** Parser for [[Bed]].  BED files don't have standard schema and
       *  are not self describing.  So users need to know how to interpret
       *  the fields themselves.  The parser is designed to let users
       *  provide a schema.
       */
 
      private val getInt = (e: String) => e.toInt
      private val getDbl = (e: String) => e.toDouble
      private val getStr = (e: String) => e
      private val getAny = (e: String) => 
        try e.toInt catch { case _: Throwable =>
        try e.toDouble catch { case _: Throwable => e }
      }
      private val cvt = Map("Int"->getInt, "Double"->getDbl, "String"->getStr)


      private val emptySchema: Vector[(String, String => Any)] = null

      private val encodeSchema: Vector[(String, String => Any)] = Vector(
        "signalval" -> getDbl,
        "pval"      -> getDbl,
        "qval"      -> getDbl,
        "peak"      -> getInt)


      /** Parser for standard BED files.
       */

      val stdParser: PARSER[Bed] = parser(emptySchema)

      /** Parser for the ENCODE family of BED files.
       */

      val encodeParser: PARSER[Bed] = parser(encodeSchema)


      /** General schema-dependent parser.
       */

      def parser(implicit sch:SCHEMA = null): PARSER[Bed] = (options:String) =>
      {
        var schema: Vector[(String, String => Any)] = sch
        
        def parse(it: Iterator[String], position: Int = 1): Bed =
        {
          var line          = ""
          var parsed: Bed   = null
          var continue      = it.hasNext

          while (continue)
          {
            var line = it.next()

            if (line startsWith "track") {
              // This line is a comment. Skip.
              continue = it.hasNext 
            }

            else if (line startsWith "browser") {
              // This line is a comment. Skip.
              continue = it.hasNext 
            }

            else if (line startsWith "##wonglimsoon@nus##") {
              // This line is a header. Define new schema.
              val entry  = line.split("\t")
              val fields = entry.toVector.tail
              schema = for (fv <- fields; e = fv.split(":"))
                       yield (e(0) -> cvt(e(1)))
              continue = it.hasNext
            }  

            else if (line startsWith "#") {
              // This line is a comment. Skip.
              continue = it.hasNext
            }

            else {
              // This line is a BED entry.
              val entry  = line.split("\t")
              val max    = entry.length - 1

              val loc    = {
                val chrom  = getStr(entry(0))
                val start  = getInt(entry(1))
                val end    = getInt(entry(2))
                Locus(chrom, start, end)
              }

              val name   = getStr(entry(3))
              val score  = getInt(entry(4))
              val strand = getStr(entry(5))
  
              val misc   = {
                val fvs = (schema == null) match {
                  case true  => for (i <- 6 to max)
                              yield (i.toString -> getAny(entry(i)))
                  case false => for (i <- 6 to max; (l, get) = schema(i - 6))
                              yield (l -> get(entry(i)))
                }
                fvs.toMap
              } 

              parsed   = Bed(loc, name, score, strand, misc)
              continue = false
            }
          }

          if (parsed == null) { throw new java.io.EOFException("") }
          else { return parsed }
        }

        parse _
      }
  
    }  // End object BedFile


    val implicits = dbmodel.DBModel.OrderedCollection.implicits

  } // End object BEDFile


}  // End object BEDModel




