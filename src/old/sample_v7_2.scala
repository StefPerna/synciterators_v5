

package synchrony
package gmql


//
// Wong Limsoon
// 15/4/2020
//


object Samples {

  import synchrony.genomeannot.GenomeAnnot._
  import synchrony.genomeannot.GenomeAnnot.GenomeLocus._
  import synchrony.genomeannot.BedWrapper._
  import synchrony.genomeannot.BedWrapper.BedFile._
  import java.io._
  import scala.io.{Source, BufferedSource}
  import synchrony.iterators.FileCollections._


  type Meta = Map[String,Any]
  type BedFile = EFile[Bed]
  


  @SerialVersionUID(999L)
  case class Sample(meta:Meta, bedFile:BedFile) extends Serializable
  {
    def track = bedFile.iterator

    def trackSz = bedFile.filesize

    def cond(ps:Sample.SBPred*)(u:Bed) = Sample.cond(ps:_*)(this, u)

    def cond(ps:Sample.SPred*) = Sample.cond(ps:_*)(this)

    def hasM(f:String) = meta.contains(f)

    def getM[T](f:String) = meta(f).asInstanceOf[T]

    def checkM[T](f:String, chk:T=>Boolean) =
      meta.get(f) match { 
        case None => false
        case Some(a) => chk(a.asInstanceOf[T]) }

    def updateM(f:String, v:Any) = Sample(meta + (f -> v), bedFile) 

    def mergeM(m:Meta) = Sample(m ++ meta, bedFile)

    def overwriteM(m:Meta) = Sample(meta ++ m, bedFile)

    def delM(f:String) = Sample(meta - f, bedFile)

    def eraseM() = Sample(meta.empty, bedFile)
    
    def renameM(f:String=>String) = 
      Sample(for((k,v)<-meta) yield f(k)->v, bedFile)


    def sortT() = sortTBy(SimpleBedEntry.ordering)

    def sortTByChromEnd() = sortTBy(SimpleBedEntry.orderByChromEnd)

    def sortTByChromStart() = sortTBy(SimpleBedEntry.orderByChromStart)

    def sortTBy(ordering:Ordering[Bed]) = 
      Sample(meta, bedFile.sorted(cmp = ordering))

    def sortTWith(cmp:(Bed,Bed)=>Boolean)=
      Sample(meta, bedFile.sortedWith(cmp = cmp))
    


    def updateT(it:Iterator[Bed]) = updateBedFile(transientBedFile(it).stored)

    def updateBedFile(it:BedFile) = Sample(meta, it)

    def mergeT(it:Iterator[Bed]) = mergeBedFile(transientBedFile(it))

    def mergeBedFile(it:BedFile) = updateBedFile(bedFile.mergedWith(it).stored)

    def eraseTM() = updateT(for(r <- track) yield r.eraseMisc())

    def eraseMandTM() = eraseM().eraseTM()
  }



  object Sample
  {
    //
    // Placeholders for defining predicates
    // on a sample. These predicates are
    // defined elsewhere, e.g. predicates.scale.

    import synchrony.gmql.Predicates

    type SBPred = Predicates.SBPred
    type SPred = Predicates.SPred
      
    def cond(ps:SBPred*)(s:Sample, u:Bed) = ps.forall(p => p(s,u))
    def cond(ps:SPred*)(s:Sample) = ps.forall(p => p(s))
  }


  //
  // Set up customized EIterator and EFile for Sample.
  // Needed because SampleFile has its special formats
  // on disk.

  type SampleEIterator = EIterator[Sample]

  type SampleFile = EFile[Sample]


  object SampleFile
  {
    // Default settings for SampleFile. 
    // Change file suffixes to ".sftmp" and ".sfsav".
    // Change serializer and deserializer to the ones
    // customized for SampleFile.


    val defaultSettingsSampleFile =
      EFileSettings[Sample](
        prefix = "synchrony-",
        suffixtmp = ".sftmp",
        suffixsav = ".sfsav",
        aveSz = 1000,
        cardCap = 2000,
        ramCap = 100000000L,
        cap = 100000,         // = (ramCap/aveSz).toInt
        doSampling = false,
        samplingSz = 30,
        alwaysOnDisk = false,
        serializer = defaultSerializerSampleFile _,
        deserializer = defaultDeserializerSampleFile _)


    def altSettingsSampleFile(dbpath:String) =
      EFileSettings[Sample](
        prefix = "synchrony-",
        suffixtmp = ".sftmp",
        suffixsav = ".sfsav",
        aveSz = 1000,
        cardCap = 2000,
        ramCap = 100000000L,
        cap = 100000,         // = (ramCap/aveSz).toInt
        doSampling = false,
        samplingSz = 100,
        alwaysOnDisk = false,
        serializer = defaultSerializerSampleFile _,
        deserializer = altDeserializerSampleFile(dbpath) _)



    // altDeserializerSampleFile is the deserializer for reading
    // sample files prepared by Stefano Perna.
    //
    // Samples are assumed to be kept in a sample folder (dbpath).
    // Each sample is assumed to be kept in two files, one for
    // its associated bed file (sid), one  for its associated
    // meta data (sid.meta). And there is a file listing
    // the sid of samples to be deserialized.

    def nullCustomizationSampleFile(x:Sample) = x


    def toEncodeNPSampleFile(x:Sample):Sample = x.bedFile.efile match
    {
      // The Bed file should be on disk.

      case bf:OnDisk[Bed] => x.updateBedFile(onDiskEncodeNPBedFile(bf.filename))

      // It is not on disk; do nothing.

      case _ => x
    }


    def altDeserializerSampleFile
        (path:String)
        (customization: Sample => Sample = nullCustomizationSampleFile _)
    = (samplelist:String) =>
    {
      def openFile(f:String) =
      {
        val file  = try { Source.fromFile(f) }
                    catch { case _ : Throwable => throw FileNotFound(f) }

        try { file.getLines.toVector.iterator }
        finally { file.close() }
      }

      def closeFile(ois:Iterator[String]) = { }

      def parseSample
          (path:String, customization:Sample => Sample)
          (ois:Iterator[String]) =
      {
        val sid = ois.next().trim
        val mf = s"${path}/${sid}.meta"
        val bf = s"${path}/${sid}"

        val metafile = try Source.fromFile(mf)
                       catch { case _ : Throwable => throw FileNotFound(mf) }

        val fields = for(l <- metafile.getLines; e = l.trim.split("\t")) 
                     yield (e(0) -> e(1))

        try customization(
              Sample(Map(fields.toSeq :_*) + ("sid" -> sid),
                     onDiskBedFile(bf)))
        finally metafile.close() 
      }

      if ((samplelist endsWith defaultSettingsSampleFile.suffixtmp) ||
          (samplelist endsWith defaultSettingsSampleFile.suffixsav))
      { //
        // Hey, the file is in Limsoon's format,
        // use Limsoon's deserializer.
      
        defaultDeserializerSampleFile(customization)(samplelist)
      }

      else
      { //
        // OK, the file is in Stefano's format.

        EIterator.makeParser(openFile,
                             parseSample(path, customization),
                             closeFile)(samplelist)
      }
    }



    // defaultDeserializerSampleFile is deserializer for 
    // Limsoon's prefered format for representing samples.
    // All the samples are in one file, separated by single
    // blank lines. Each meta attribute is on separate line,
    // and has the form fieldname \t fieldvalue.  Moreover,
    // the first line is always the filename of the
    // associated Bed file.


    def defaultDeserializerSampleFile
        (customization: Sample => Sample)
        (samplelist: String)
    : SampleEIterator =
    {
      def openFile(f:String) =
      {
        val file = Source.fromFile(f)
        val lines = file.getLines.map(_.trim).toVector.iterator
        file.close()
        lines
      }

      def closeFile(ois:Iterator[String]) = { }

      def parseSample
          (customization:Sample => Sample)
          (ois:Iterator[String]) =
      {
        val sid = ois.next()

        val lines = ois.takeWhile(_ != "").toVector

        val meta: Meta = 
        {
          val fields =
            for(l <- lines; m = l.split("\t")) yield
            {
              val m0:String = m(0)
              val m1:Any = try m(1).toDouble catch { case _:Throwable => m(1) } 
              (m0, m1)
            }
          Map[String, Any](fields.toSeq :_*)
        } 

        customization(Sample(meta, onDiskBedFile(sid))) 
      }

      EIterator.makeParser(openFile, 
                           parseSample(customization),
                           closeFile)(samplelist) 
    }


    def defaultSerializerSampleFile(sfobj:SampleFile)(filename:String) =
    {
      val oos = new PrintWriter(new File(filename))
      val it = sfobj.iterator

      for(e <- it) {
        val sid = e.bedFile.serialized.filename
        oos.write(s"filename\t${sid}\n")
        for((f, v) <- e.meta) oos.write(s"${f}\t${v}\n")
        oos.write("\n")
        oos.flush()
      }

      it.close()
      oos.flush()
      oos.close()
    }


    //
    // Functions for creating SampleFile objects.
    // Use these instead of the EFile ones.
    //

   
    def apply(efile:EFileState[Sample]) = new SampleFile(efile)


    def inMemorySampleFile(
      entries: Vector[Sample],
      settings: EFileSettings[Sample] = defaultSettingsSampleFile) =
    {
      apply(InMemory(entries, settings))
    }


    def transientSampleFile(
      entries: Iterator[Sample],
      settings: EFileSettings[Sample] = defaultSettingsSampleFile) =
    {
      apply(Transient(entries, settings))
    }


    def onDiskSampleFile(
      filename: String,
      customization: Sample => Sample = nullCustomizationSampleFile _,
      settings: EFileSettings[Sample] = defaultSettingsSampleFile) =
    {
      apply(OnDisk[Sample](filename, customization, settings))
    }


    // For reading the files created by Stefano Perna.

    def altOnDiskSampleFile(dbpath: String) 
        (samplelist:String,
         customization: Sample => Sample = nullCustomizationSampleFile _,
         settings: EFileSettings[Sample] = altSettingsSampleFile(dbpath))
    : SampleFile =
    {
      apply(OnDisk[Sample](samplelist, customization, settings))
    }


    def altOnDiskEncodeNPSampleFile(dbpath: String)
        (samplelist:String,
         settings: EFileSettings[Sample] = altSettingsSampleFile(dbpath))
    : SampleFile =
    {
      apply(OnDisk[Sample](samplelist, toEncodeNPSampleFile _, settings))
    }

  }
}


/*
 *
 * Here are examples on using SampleFile...
 *
 *

import synchrony.gmql.Samples._
import synchrony.gmql.Samples.SampleFile._
import synchrony.genomeannot.BedWrapper._
import synchrony.genomeannot.BedWrapper.BedFile._
import synchrony.iterators.FileCollections.implicits._


// ctcfPath is the directory of ctcf samples.
// ctcfList is the list of ctcf samples.

val dir = "../synchrony-1/test/test-massive/"
val ctcfPath = dir + "cistrome_hepg2_narrowpeak_ctcf/files"
val ctcfList = dir + "ctcf-list.txt"

// ctcfFiles is an EFile of samples.
// i.e. ctcfFiles.iterator is an iterator on the ctcf samples.

val ctcfFiles = altOnDiskEncodeNPSampleFile(ctcfPath)(ctcfList)
// val ctcfFiles = altOnDiskSampleFile(ctcfPath)(ctcfList)

ctcfFiles.iterator.toVector

ctcfFiles.iterator.length

ctcfFiles.length


ctcfFiles(2).bedFile(10)  // get the 10th bed entry on the 2nd sample's track

val bf = Sample(
           Map("dummy" -> 123),
           transientBedFile(for(x <- ctcfFiles(2).bedFile.iterator;
                                if (x.chrom == "chr1"))
                            yield x )
           .stored)

bf.bedFile(10)

bf.getM[Int]("dummy")


import synchrony.iterators.AggrCollections.OpG._

bf.bedFile.flatAggregateBy(biggest((x:Bed) => x.score))

ctcfFiles.flatAggregateBy[Sample,Double](count)

 *
 *
 */



