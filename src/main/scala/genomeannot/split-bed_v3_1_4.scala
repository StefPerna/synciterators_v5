

package synchrony.genomeannot

/** Provide for "split" Bed-formatted files.
 *
 *  A split Bed file (SBedFile) splits the Bed file by 
 *  chromsome, storing each chromosome as a separate
 *  Bed file.  In other words, physically, an SBedFile
 *  is actually stored as a SampleFile,
 *  wherein each sample is a chromosome.
 *
 * Wong Limsoon
 * 21 May 2021
 */


object SplitFiles {

  import java.nio.file.{ Files, Paths }
  import synchrony.iterators.Serializers.Deserializer
  import synchrony.iterators.SyncCollections.EIterator
  import synchrony.iterators.FileCollections.{ 
    OnDisk, Slurped, EFile, EFileState, EFileSettings }
  import synchrony.gmql.Samples.{ 
    Sample, BaseSampleFileSerializers }
  import synchrony.genomeannot.BedWrapper.{
    SimpleBedEntry => Bed, BedEIterator, BedFile, BedSerializers
  }

  import synchrony.pargmql.ExecutionManagers.{ IOHeavyMgr => ParGMQL }
//  import synchrony.pargmql.ExecutionManagers.{ ParGMQLMgr => ParGMQL }
//  import synchrony.pargmql.ExecutionManagers.{ FJMgr => ParGMQL }

//  private val runVec = ParGMQL.runGlobalVec[Sample] _
  private def runBig(v: Vector[() => Sample]) = ParGMQL.runBig(EIterator(v))

  //
  // Define serializers and deserializers for SBedFile.
  //

  object SBedSerializers {

  /** Setting up to use a basic SampleFile parser to
   *  handle the SampleFile representation of SBedFile.
   */

    val defaultSettingsBaseSampleFile = 
      BaseSampleFileSerializers.defaultSettingsSampleFile
      

  /** @param filename is name of the SampleFile
   *  representing an SBedFile.
   */

    def onDiskBaseSampleFile(filename: String): EFile[Sample] =
      EFile.onDiskEFile[Sample](filename)(defaultSettingsBaseSampleFile)

    def inMemoryBaseSampleFile(entries: Vector[Sample]): EFile[Sample] =
      EFile.inMemoryEFile[Sample](entries)(defaultSettingsBaseSampleFile)

    def transientBaseSampleFile(entries: Iterator[Sample]): EFile[Sample] =
      EFile.transientEFile[Sample](entries)(defaultSettingsBaseSampleFile)

    
  /** Deserializer for SBedFile. The deserializer must
   *  be able to parse both SBedFile and BedFile.
   */

    object SBedFileDeserializer extends Deserializer[Bed] {

      def isBedFile(f: String): Boolean = {
        // sTmp and sSav are file extensions when the file
        // is really an SBedFile
        val sTmp = defaultSettingsBaseSampleFile.suffixtmp
        val sSav = defaultSettingsBaseSampleFile.suffixsav

        // bTmp and bSav are file extensions when the file
        // is actually a normal BedFile
        val bTmp = BedFile.defaultSettingsBedFile.suffixtmp
        val bSav = BedFile.defaultSettingsBedFile.suffixsav

        if (f.endsWith(bTmp) || f.endsWith(bSav)) return true
        if (f.endsWith(sTmp) || f.endsWith(sSav)) return false

        // If file extension is not given, ...
        if (Files.isDirectory(Paths.get(f))) return false
        if (Files.isRegularFile(Paths.get(f + bTmp))) return true
        if (Files.isRegularFile(Paths.get(f + bSav))) return true
        else return false
      }

      override def parse(filename: String, slurp: Option[String] = None):
      EIterator[Bed] = {

        val f = slurp match {
          case None        => filename
          case Some(other) => other
        }

        isBedFile(f) match {
          case true  => return BedSerializers.deserializerBedFile.parse(f,slurp)
          case false => return SBedFile(f).bedEntries 
        }
      }
    }

  
    val defaultSettingsSBedFile = EFile.setDefaultsEFile[Bed](
      suffixtmp       = BedFile.defaultSettingsBedFile.suffixtmp,
      suffixsav       = BedFile.defaultSettingsBedFile.suffixsav,
      aveSz           = BedFile.defaultSettingsBedFile.aveSz,
      cap             = BedFile.defaultSettingsBedFile.cap,
      totalsizeOnDisk = BedFile.defaultSettingsBedFile.totalsizeOnDisk,
      serializer      = BedSerializers.serializerBedFile,
      deserializer    = SBedFileDeserializer
    )

  } // object SBedSerializers



  /** "Private" constructor for SBedFile
   *
   *  SBedFile should be created as
   *  ASBedFile(folder, OnDisk[Bed](folder, defaultSettingsSBedFile))
   */

  private case class ASBedFile(
    override val folder: String, 
    override val efile: EFileState[Bed])
  extends SBedFile


  trait SBedFile extends BedFile {

    // Inherit class should override folder here
    // and also efile in super[BedFile]
    val folder: String


    // Name of this SBedFile.
    // If really an SBedFile, name is that of its meta file.
    // If actually a BedFile, name is its filename
    val name: String = Files.isDirectory(Paths.get(folder)) match {
      case true  => folder + "/" + SBedFile.meta
      case false => folder
    }
    

    // The SampleFile representation of this SBedFile
    val sampleFile: EFile[Sample] = SBedSerializers.onDiskBaseSampleFile(name)


    // Map of chromosomes to their BedFile
    val chromMap: Map[String, BedFile] = {
      for (
        s <- sampleFile.eiterator;
        if s.hasM("chrom");
        ch = s.getM[String]("chrom");
        cf = s.bedFile
      ) yield ch -> cf
    }.toMap


    // The chromosomes in this SBedFile
    val chroms: Vector[String] = chromMap.keys.toVector.sorted


    // An eiterator on all Bed entries in this SBedFile
    def bedEntries: EIterator[Bed] = bedEntriesByChrom(chroms: _*)


    // Converts this SBedFile to a BedFile
    def bedFile: BedFile = BedFile.transientBedFile(bedEntries)


    /** Retrieves the BedFiles of the specified chromosomes.
     *
     *  @param chroms      - the specified chromosomes
     *  @param sortByChrom - keep the result sorted by chromosome
     *  @return the BedFiles of the specified chromosomes
     */

    def bedFilesByChrom(chroms: String*)(implicit sortByChrom: Boolean = true) =
      for (
        ch <- if (sortByChrom) chroms.sorted else chroms;
        bf <- chromMap.get(ch)
      ) yield bf


    /** Retrieves the Bed entries of the specified chromosomes
     *
     *  @param chroms      - the specified chromosomes
     *  @param sortByChrom - keep the result sorted by chromosome
     *  @return the Bed entries of the specified chromosomes
     */

    def bedEntriesByChrom(chroms: String*)(implicit sortByChrom: Boolean = true)
    = new EIterator[Bed] {
      private val ch:EIterator[BedFile] = 
        EIterator[BedFile](bedFilesByChrom(chroms:_*))
      private var bf: BedEIterator   = EIterator.empty[Bed]
      override def myClose() = { bf.close(); ch.close() }
      override def myNext()  = bf.next()
      override def myHasNext = (bf.hasNext, ch.hasNext) match {
        case (true, _)      => true
        case (false, false) => false
        case _ => bf = ch.next().eiterator; myHasNext
      }      
      myHasNext  // Initialize bf.
    } 


    /** Combine the specified chromosomes into one BedFile
     *
     *  @param chroms      - the specified chromosomes
     *  @param sortByChrom - keep the result sorted by chromosome
     *  @return a combined BedFile of the specified chromosomes
     */

    def bedFileByChrom(chroms: String*)(implicit sortByChrom: Boolean = true) =
      BedFile.transientBedFile {
        bedEntriesByChrom(chroms: _*)(sortByChrom)
    }


    /** Apply a specified function to each selected chromosome.
     *
     *  @param f     - the specified function
     *  @param cpred - the selection predicate on chromosomes
     *  @return an SBedFile containing the f-transformed chromosomes
     */

    def onChrom(f: BedFile => BedFile, cpred: String => Boolean = _ => true) =
      SBedFile.onChrom(f, cpred)(this)


    /** Make a copy of this SBedFile.
     *
     *  @param copyAs - new file name
     *  @param copyTo - new directory to put the new file in.
     *  @return the copy
     */

    def copied(implicit copyAs: String = "", copyTo: String = "") = 
      SBedFile(bedFile, copyAs, copyTo)


    /** Export this BedFile to as a normal BedFile on disk
     *
     *  @param exportAs - new file name
     *  @param exportTo - directory to put the new file in
     *  @return the exported BedFile
     */

    def exportToBedFile(exportAs: String, exportTo: String = ""): BedFile =
      bedFile.savedAs(exportAs, exportTo)


    /** Apply a specified function chromosome-wise
     *
     *  @param f - the specified function
     *  @return the SBedFile containing the f-transformed chromosomes
     */

    def lift(f: BedFile => BedFile): BedFile = SBedFile.lift(f)(this)


    /** Apply a specified function chromosome-wise on pair of BedFiles
     *
     *  @param f     - the specified function
     *  @param other - a BedFile 
     *  @return a BedFile formed by applying f chromsome-wise on
     *          this BedFile and other
     */

    def binlift(f: (BedFile, BedFile) => BedFile)(other: BedFile): BedFile = 
      SBedFile.binlift(f)(this, other)
  

    /** Apply a specified function chromosome-wise on selected
     *  chromosomes of this BedFile
     *
     *  @param f     - the specified function
     *  @param cpred - the selection predicate on chromosomes
     *  @return SBedFile containing the f-transformed chromosomes
     */

    def condlift(f: BedFile => BedFile, cpred: String => Boolean): BedFile =
      SBedFile.condlift(f, cpred)(this)


    /** Apply a specified function on a sequence of BedFiles
     *
     *  @param f      - the specified function
     *  @param others - a sequence of BedFiles
     *  @return result of applying f to this and others
     */

    def multilift(f: Seq[BedFile] => BedFile)(others: Seq[BedFile]): BedFile =
      SBedFile.multilift(f)(this +: others) 


    /** Apply a specified function on this and a sequence of BedFiles
     *
     *  @param f      - the specified function
     *  @param others - a sequence of BedFiles
     *  @return result of applying f to this and others
     */

    def binmultilift(f: (BedFile, Seq[BedFile]) => BedFile) =
    (others: Seq[BedFile]) =>
      SBedFile.binmultilift(f)(this, others)



    //
    // Override various BedFile operations to
    // operate on SBedFile instead.
    //

    override def eiterator: EIterator[Bed] = bedEntries


    override def totalsizeOnDisk: Long = sampleFile.totalsizeOnDisk
  

    override def destruct(): Unit = {
      sampleFile.eiterator.foreach(s => s.bedFile.destruct())
      sampleFile.destruct()
    }


    override def mergedWith(that: BedFile*): BedFile = {

      def merge(files: Seq[BedFile]) = files.isEmpty match {
        case true  => BedFile.inMemoryBedFile(Vector())
        case false => BedFile.merge(files: _*)
      }

      multilift(merge)(that)
    }


    override def serialized(implicit folder: String = ""): SBedFile = 
      super[EFile].serialized(folder).asInstanceOf[SBedFile]


    override def sorted: SBedFile = onChrom(bf => bf.sorted)


    override def sortedIfNeeded: SBedFile = onChrom(bf => bf.sortedIfNeeded)


    override def slurped: BedFile = SBedFile.slurpedSBedFile(this)


    override def savedAs(name: String, folder: String = ""): SBedFile =
      SBedFile(copied(name, folder))

  }



  /** Constructors for SBedFile
   */

  object SBedFile {

    import java.nio.file.{ Files, Paths }
    import synchrony.iterators.AggrCollections.OpG

    private val pfix = "synchrony-"
    private val meta = "chroms"

    
    val defaultSettingsSBedFile = SBedSerializers.defaultSettingsSBedFile


    /** Constructor to convert normal BedFile to SBedFile
     *
     *  @param bf       - the normal BedFile
     *  @param importAs - new file name
     *  @param importTo - director to put new file in
     *  @return the converted SBedFile
     */

    def apply(bf: BedFile, importAs: String = "", importTo: String = "")
    : SBedFile = (bf, importTo, importAs) match {

      // Do nothing if bf is actually an SBedFile and
      // no new file name/directory is specified.
      case (sf: SBedFile, "", "") => return sf

      // Otherwise, convert and copy as needed.
      case _ => 

        val ANS = synchrony.iterators.FileCollections.ANSDIR
        val ans = ANS.toString
        val name = (importTo, importAs) match {
          case ("", "") => Files.createTempDirectory(ANS, pfix).toString
          case ("", f)  => Files.createDirectories(Paths.get(ans + "/" + f)).toString
          case (".", f)  => Files.createDirectories(Paths.get(f)).toString
          case (d, "")  => 
            val dir = Files.createDirectories(Paths.get(d))
            Files.createTempDirectory(dir, pfix).toString
          case (d, f)   => 
            Files.createDirectories(Paths.get(d + "/" + f)).toString
        }

        val chromMap = {
          for (
            (k, v) <- bf.partitionby((u: Bed) => u.chrom)(OpG.keep);
            cf = BedFile.inMemoryBedFile(v).savedAs(k, name)
          ) yield k -> cf
        }.toMap

        val chroms = chromMap.keys.toVector.sorted

        val sf = SBedSerializers.inMemoryBaseSampleFile {
          for (
            ch <- chroms; 
            bf <- chromMap.get(ch) 
          ) yield Sample(Map("chrom" -> ch), bf)
        }.savedAs(meta, name)

        return onDiskSBedFile(sf.filename)
    }


    /** Construct SBedFile from a SampleFile representation.
     *
     *  Assume each Sample in SampleFile is a chromosome,
     *  and its meta attribute "chrom" tells us which
     *  chromosome it is. 
     *
     *  @param bf - the SampleFile representation
     *  @return the constructed SBedFile
     */

    def apply(bf: EFile[Sample]): SBedFile = 
      return onDiskSBedFile(bf.serialized.filename)

  
    /** Construct SBedFile from an eiterator of Bed entries
     *
     *  @param entries - the eiterator
     *  @return the constructed SBedFile
     */

    def apply(entries: BedEIterator): SBedFile = 
      return transientSBedFile(entries)


    /** Construct SBedFile from a file name
     *
     *  @param folder - a file name
     *  @return the constructed SBedFile
     */

    def apply(folder: String): SBedFile =
      return onDiskSBedFile(folder)


    /** Construct SBedFile from a vector of Bed entries
     *
     *  @param entries - the vector of Bed entries
     *  @result the constructed SBedFile
     */

    def inMemorySBedFile(entries: Vector[Bed]): SBedFile =
      return SBedFile(BedFile.inMemoryBedFile(entries))


    /** Construct SBedFile from an eiterator of Bed entries
     *
     *  @param entries - the eiterator
     *  @return the constructed SBedFile
     */

    def transientSBedFile(entries: Iterator[Bed]): SBedFile =
      return SBedFile(BedFile.transientBedFile(entries))


    /** Construct SBedFile from a file name
     *
     *  @param folder - a file name
     *  @return the constructed SBedFile
     */

    def onDiskSBedFile(filename: String): SBedFile = return ASBedFile(
      filename, 
      OnDisk[Bed](filename, defaultSettingsSBedFile)
    )


    /** Slurps an SBedFile
     *
     *  @param entries - the SBedFile to slurp
     *  @return a BedFile representing the slurped result.
     */

    def slurpedSBedFile(entries: SBedFile): BedFile = {
      val sf = entries.sampleFile.slurped
      sf.efile match {
        case s: Slurped[Sample] => 
          // An SBedFile is encoded as a SampleFile
          // where each sample is a chrom.
          val settings = defaultSettingsSBedFile
          EFile.slurpedEFile[Bed](s.raw, s.filename)(settings)
        case _ => 
          // This case should not happen.
          sf.asInstanceOf[BedFile]
      }
    }
        
    
    /** Apply a specified function to each selected chromosome.
     *
     *  @param f     - the specified function
     *  @param cpred - the selection predicate on chromosomes
     *  @return an SBedFile containing the f-transformed chromosomes
     */


    def onChrom
      (f: BedFile => BedFile, cpred: String => Boolean = _ => true)
      (us: SBedFile)
    : SBedFile = SBedFile {
      val ans = synchrony.iterators.FileCollections.ANSDIR 
      val dir = Files.createTempDirectory(ans, pfix).toString
      SBedSerializers.transientBaseSampleFile {
        val par =
          for (
            ch <- us.chroms if cpred(ch);
            bf <- us.chromMap.get(ch)
          ) yield () => {
              val bs = f(bf).serialized(dir)
              Sample(Map("chrom" -> ch), bs)
          }
        runBig(par)
      }.serialized(dir)
    }



    //
    // Lift functions defined on BedFile to SBedFile.
    //


    /** Apply a specified function chromosome-wise on an S/BedFile
     *
     *  @param f  - the specified function
     *  @param us - the S/BedFile
     *  @return the S/BedFile containing the f-transformed chromosomes
     */

    def lift(f: BedFile => BedFile) = (us: BedFile) => us match {
      case sf: SBedFile => sf.onChrom(f)
      case _ => f(us)
    }


    /** Apply a specified function chromosome-wise on pair of S/BedFiles
     *
     *  @param f      - the specified function
     *  @param us
     *  @param vs     - a pair of S/BedFile 
     *  @return n S/BedFile formed by applying f chromsome-wise on
     *          the pair of S/BedFiles
     */

    def binlift(f: (BedFile, BedFile) => BedFile) = 
    (us: BedFile, vs: BedFile) =>
    (us, vs) match {
      case (uf: SBedFile, vf: SBedFile) => SBedFile {
        val ans = synchrony.iterators.FileCollections.ANSDIR 
        val dir = Files.createTempDirectory(ans, pfix).toString
        SBedSerializers.transientBaseSampleFile {
          val par = 
            for (
              ch <- uf.chroms;
              ub <- uf.chromMap.get(ch);
              vb = vf.chromMap.getOrElse(ch, BedFile.inMemoryBedFile(Vector()))
            ) yield () => {
                val bf = f(ub, vb).serialized(dir)
                Sample(Map("chrom" -> ch), bf)
            }
          runBig(par)
        }.serialized(dir)
      }
      case _ => f(us, vs)
    }


    /** Apply a specified function chromosome-wise on selected
     *  chromosomes of a S/BedFile
     *
     *  @param f     - the specified function
     *  @param cpred - the selection predicate on chromosomes
     *  @param us    - the S/BedFile
     *  @return S/BedFile containing the f-transformed chromosomes
     */

    def condlift(f: BedFile => BedFile, cpred: String => Boolean) =
    (us: BedFile) => us match {
      case sf: SBedFile => SBedFile(sf.onChrom(f, cpred))
      case _ => f(us.filtered(r => cpred(r.chrom)))
    }


    /** Apply a specified function on a sequence of S/BedFiles
     *
     *  @param f  - the specified function
     *  @param vs - a sequence of BedFiles
     *  @return result of applying f to vs
     */

    def multilift(f: Seq[BedFile] => BedFile) = (vs: Seq[BedFile]) =>
    (!vs.forall(_.isInstanceOf[SBedFile])) match { 

      case true => f(vs) 
 
      case false =>

        val chroms = { 
          for (
            sf <- vs.toVector;
            ch <- sf.asInstanceOf[SBedFile].chroms
          ) yield ch
        }.sorted.distinct

        SBedFile {
          val ans = synchrony.iterators.FileCollections.ANSDIR 
          val dir = Files.createTempDirectory(ans, pfix).toString
          SBedSerializers.transientBaseSampleFile {
            val par =
              for (
                ch <- chroms;
                us = for (
                       sf <- vs; 
                       u  <- sf.asInstanceOf[SBedFile].chromMap.get(ch)
                     ) yield u
              ) yield () => {
                  val bf = f(us).serialized(dir)
                  Sample(Map("chrom" -> ch), bf)
              }
            runBig(par)
          }.serialized(dir)
        }
      }


    /** Apply a specified function on this and a sequence of BedFiles
     *
     *  @param f      - the specified function
     *  @param others - a sequence of BedFiles
     *  @return result of applying f to this and others
     */

    def binmultilift(f: (BedFile, Seq[BedFile]) => BedFile) =
    (us: BedFile, vs: Seq[BedFile]) =>
      (us, !vs.forall(_.isInstanceOf[SBedFile])) match {

        case (u: SBedFile, false) =>

          val chroms = u.chroms

          SBedFile {
            val ans = synchrony.iterators.FileCollections.ANSDIR 
            val dir = Files.createTempDirectory(ans, pfix).toString
            SBedSerializers.transientBaseSampleFile {
              val par =
                for (
                  ch <- chroms;
                  w  <- u.chromMap.get(ch);
                  ws = for (
                          sf <- vs.toVector;
                          w  <- sf.asInstanceOf[SBedFile].chromMap.get(ch)
                        ) yield w
                  ) yield () => {
                    val bf = f(w, ws).serialized(dir)
                    Sample(Map("chrom" -> ch), bf)
                  }
                runBig(par)
              }.serialized(dir)
            }
                     
        case _ => f(us, vs)
    } 

  }  // object SBedFile

}





