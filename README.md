# Synchroiterators
# A repository for ongoing development of synchronized iterators in scala.

# Wong Limsoon
# 28/12/2019
#


PRELIMINARIES

I try to illustrate Synchrony queries using the GMQL examples 
I found in the following document,
http://www.bioinformatics.deib.polimi.it/genomic_computing/GMQL/doc/GMQLUserTutorial.pdf

Please consult the file gmql.scala for the assumed representation
of GMQL samples and sample databases.

The Synchrony queries in the examples below require the 
following imports:

import synchrony.demos.GMQL._
import synchrony.iterators.AggrCollections._
import synchrony.iterators.AggrCollections.OpG._
import synchrony.iterators.AggrCollections.implicits._
import synchrony.iterators.SyncCollections._
import synchrony.iterators.SyncCollections.implicits._
import synchrony.genomeannot.GenomeAnnot._
import synchrony.genomeannot.BedWrapper._


----------


SELECTION EXAMPLE #1

GMQL:

OUTPUT_DATASET = SELECT(region: score > 0.5) INPUT_DATASET;

This query selects, in all samples in INPUT_DATASET, those
regions which have a value greater than 0.5 for their attribute
score. The resulting OUTPUT_DATASET contains a copy of the 
samples of INPUT-DATASET, with the same metadata, but with
only the remaining regions.


Synchrony, based on gmql.scala:

This query directly corresponds to the SampleDB.regionSelect
function, like this:

(db: SampleDB[Sample]) => db.regionSelect(_.score > 0.5)


Synchrony, direct query (i.e. expanding out SampleDB.regionSelect):

(db: SampleDB[Sample]) =>
   for (s <- db.samples)
   yield {
        val t = (for(e <- s.track() if (e.score > 0.5)) yield e).toList
        s.copy2( nt = () => ExTrack(t.iterator)) }
        
Here, the "...toList" and the later "...t.iterator" is an idiomatic 
piece of code to first convert an iterator to a list, thereby saving
it so that it can be reused many times. The later "...t.iterator"
fetches this saved list and reproduce an iterator as needed. Instead
of toList, we can also save to a file when the list is big; this is
straightforward to arrange.



----------


SELECTION EXAMPLE #2

GMQL:

DATA = SELECT(cell == "Urothelia"; region: left > 100000) HG19_ENCODE_NARROW;

This GMQL statement creates a new output dataset DATA which only
includes samples from the input dataset HG19_ENCODE_NARROW that
present the metadata attribute-value pair (cell Urothelia). Moreover,
in each sample, only the regions whose left coordinate value is
greater than 100000 are included in the output dataset DATA.


Synchrony, based on gmql:

(db: SampleDB[Sample]) => db.regionSelect(
                    _.chromStart > 100000, 
                    _.meta("cell") == "Urothelia")


Synchrony, direct query:

(db: SampleDB[Sample]) =>
     for (s <- db.samples if (s.meta("cell") == "Urothelia"))
     yield {
        val t = (for(e <- s.track() if (e.chromStart > 100000)) yield e).toList
        s.copy2( nt = () => ExTrack(t.iterator)) }
       

----------


GROUPING EXAMPLE #1

GMQL:

GROUPS = GROUP(Tumor_type; region_aggregate: Min AS MIN(score)) EXP;

This GMQL statement groups samples according to the value of
Tumor_type and computes the minimum score of each group.


Synchrony, based on gmql.scala:

(db: SampleDB[Sample]) =>
  db.groupBy(_.meta("tumorType"))(
    smallest(_.track().aggregateBy(smallest(_.score))))


Synchrony, direct query:

(db: SampleDB[Sample]) =>
  db.samples.groupBy(_.meta("tumorType"))(
     smallest((x:Sample)=> x.track().aggregateBy(smallest(_.score))))


----------


PROJECTION EXAMPLE #1


GMQL:

RES = PROJECT(region_update: length AS right - left) DS;

This GMQL statement creates a new dataset called RES by 
preserving all region attributes and creating a new region
attribute called length by subtracting the left coordinate 
of a region from its right coordinate. This simple operation
computes the length of the region in terms of number of bases.


Synchrony, based on gmql.scala:


(db: SampleDB[Sample]) => 
   db.regionAddField(
         "length", 
         (x:Bed) => x.chromEnd - x.chromStart)


Synchrony, direct query:

(db: SampleDB[Sample]) => 
   for(s <- db.samples)
   yield {
        val t = (for(e <- s.track())
                yield (e.addMisc("length", e.chromEnd - e.chromStart))).toList
        s.copy2( nt = () => ExTrack(t.iterator)) }
   


----------


MAP EXAMPLE #1

GMQL:

GENES_EXP = MAP(avg_score AS AVG(score)) GENES EXP;

Given a dataset GENES, containing a single sample with a known
set of genes, and another dataset EXP containing results from
a genomic experiment on the same species, this GMQL statement 
counts the number of regions (e.g., peaks) in each sample from
the experiment which overlap with a known gene, and computes
the average (AVG) score value across such regions, saving results
in the output as a region attribute (feature) called avg_score


Synchrony, based on gmql.scala:

This is the first example that uses synchronous scans. The 
synchronization of exp to genes let us scan the two tracks
in lock step.

(ref:Sample, db: SampleDB[Sample]) => 
  SyncedSampleDB(ref, db.samples).joinRegionAggregateBy(
    label = "avgScore",
    aggr = _ => average(_.score), 
    cs = GenomeLocus.overlap)


Synchrony, semi-direct query:
 
(ref:Sample, db: SampleDB[Sample]) => { 
  val (_, lmTrack, syncedDB) = db.connect(ref = ref, cs = GenomeLocus.overlap)
  for ( g <- lmTrack; 
        s <- syncedDB;
        r = s.syncedTrack syncWith g;
        a <- r.aggregateByWithFilter(average(_.score)))
  yield (g, s.meta += { "avgScore" -> a })
}


----------

MAP EXAMPLE #2

GMQL:

OUT = MAP (minScore AS MIN(score); joinby: cell_tissue) REF EXP;

This GMQL statement counts the number of regions in each sample
from EXP that overlap with a REF region, and for each REF region
it computes the minimum score of all the regions in each EXP sample
that overlap with it. The MAP joinby option ensures that only the
EXP samples referring to the same cell_tissue of a REF sample are
mapped on such REF sample; EXP samples with no cell_tissue metadata
attribute, or with such metadata but with a different value from
the one(s) of REF sample(s), are disregarded.


Synchrony, based on gmql.scala:


(ref:Sample, db: SampleDB[Sample]) => 
  SyncedSampleDB(ref, db.samples).joinRegionAggregateBy(
    join = (x:Sample, y:Sample) => x.meta("cellTissue") == y.meta("cellTissue"),
    label = "minScore",
    aggr = _ => smallest(_.score), 
    cs = GenomeLocus.overlap)


Synchrony, semi-direct query:
 
(ref:Sample, db: SampleDB[Sample]) => { 
  val (_, lmTrack, syncedDB) = db.connect(
    ref = ref, 
    join = (x:Sample, y:Sample) => x.meta("cellTissue") == y.meta("cellTissue"),
    cs = GenomeLocus.overlap)

  for ( g <- lmTrack; 
        s <- syncedDB;
        r = s.syncedTrack syncWith g;
        a <- r.aggregateByWithFilter(smallest(_.score)))
  yield (g, s.meta += { "minScore" -> a })
}




----------


JOIN EXAMPLE #1

GMQL:

HM_TSS = JOIN(MD(1), DGE(120000); output: RIGHT; joinby: provider) TSS HM;

Given a dataset HM of ChIP-seq experiment samples regarding
Histone Modifications and one called TSS with a sample including
Transcription Start Site annotations, this GMQL statement searches
for those regions of HM that are at a minimal distance from a 
transcription start site (TSS) and takes the first/closest one 
for each TSS, provided that such distance is greater than 120K
bases and joined TSS and HM samples are obtained from the
same provider (joinby clause).


Synchrony, based on gmql.scala:

Hmm... this is a GMQL query that Synchrony is not intended to
do, since it explicitly requires matching a locus in one track
to far-away loci in other tracks. Nonetheless, if an upperbound
limit  (say plus/minus 500,000 bp) is provided on how far to
look, it can be expressed.

(ref:Sample, db: SampleDB[Sample]) => 
  SyncedSampleDB(ref, db.samples).joinRegionAggrBy(
    join = (x:Sample, y:Sample) => x.meta("provider") == y.meta("provider"),
    region = (x:Bed, y:Bed) => x.locus.isFarFrom(y.locus, 120000),
    aggr = (x:Bed) => minimize(_.locus distFrom x.locus), 
    cs = (x:GenomeLocus, y:GenomeLocus) => x.startNearStartOf(y, 500000),
    ex = false)


Synchrony, semi-direct query:
 
(ref:Sample, db: SampleDB[Sample]) => { 
  val (_, lmTrack, syncedDB) = db.connect(
    ref = ref, 
    join = (x:Sample, y:Sample) => x.meta("provider") == y.meta("provider"),
    cs = (x:GenomeLocus, y:GenomeLocus) => x.startNearStartOf(y, 500000),
    ex = false)

    for ( g <- lmTrack; 
          s <- syncedDB;
          r = s.syncedTrack syncWith g;
          a <- r.aggregateByWithFilter(
                  minimize(_.locus distFrom g.locus),
                  _.locus.isFarFrom(g.locus, 120000)))
    yield (g, s.meta, a)
}


----------


JOIN EXAMPLE #2

GMQL:

TF_HM_OVERLAP = JOIN(DLE(-1); output: INT; joinby: cell) TFBS HM;

Given a dataset TFBS that contains peak regions of
transcription factor binding sites (TFBSs) for a certain
TF, and another dataset named HM that contains regions
resulting from experiments targeting specific histone
modifications (for instance methylations), this JOIN
statement returns as output the intersection of histone 
modification regions with the transcription factor 
binding sites that overlap in the same cell line 
(indicated by the joinby parameter)


Synchrony, based on gmql.scala:

I am not sure I completely understand the last part:
TF binding sites are practically points; hence their
intersection with anything are also points.  I interpret 
this GMQL to mean finding peaks in a TFBS that overlap
any histone modification in the same cell line.  Here, 
I use TFBS as ref, and histone modifications as SampleDB.

(ref:Sample, db: SampleDB[Sample]) => { 
  SyncedSampleDB(ref, db.samples).joinRegionQuery(
    join = (x:Sample, y:Sample) => x.meta("cell") == y.meta("cell"),
    cs = GenomeLocus.overlap,
    ex = false,
    query = (g:Bed, m:Meta, t:Vector[Bed]) =>
      for(r <- t;
          i <- g.locus intersect r.locus)
      yield (g, m, r, i))
}


Synchrony, semi-direct query:

(ref:Sample, db: SampleDB[Sample]) => { 
  val (_, lmTrack, syncedDB) = db.connect(
    ref = ref, 
    join = (x:Sample, y:Sample) => x.meta("cell") == y.meta("cell"),
    cs = GenomeLocus.overlap,
    ex = false)

    for ( g <- lmTrack; 
          s <- syncedDB;
          r <- s.syncedTrack syncWith g;
          i <- g.locus intersect r.locus)
    yield (g, s.meta, r, i)
}


----------


COVER EXAMPLE #1

GMQL:

RES = COVER(2, ANY) EXP;

This GMQL statement produces an output dataset with a
single output sample. The COVER operation considers all
areas defined by a minimum of two overlapping regions 
in the input samples, up to any amount of overlapping 
regions.


Synchrony, semi-direct query:

I guess the intent of this query is to find, say in the
context of TFBS analysis, those gene promoter regions
that are bound by at least two TFs with very close binding
sites in the same cell line. Here, I use genes as ref, 
and TFs as SampleDB.


(ref:Sample, db: SampleDB[Sample]) => { 
  val (_, lmTrack, syncedDB) = db.connect(
    ref = ref, 
    join = (x:Sample, y:Sample) => x.meta("cell") == y.meta("cell"),
    cs = GenomeLocus.canSee)

    for ( g <- lmTrack; 
          s1 <- syncedDB;
          s2 <- syncedDB;
          if (s1.meta("TF name") != s2.meta("TF name"));
          r1 <- s1.syncedTrack syncWith g;
          r2 <- s2.syncedTrack syncWith g;
          if (r1.locus.startNearStartOf(r2.locus)))
    yield (g, s1.meta, s2.meta, r1, r2, r1.locus union r2.locus)
}


----------


REMARKS


1/ As can be seen, the gmql-like queries and the direct Synchrony
queries are comparable in length. 

2/ Even though I implemented the gmql-like functions myself, I 
can't yet easily remember what each function does. Thus, I can't
yet easily understand the gmql-like queries. Maybe this will
improve when I use these functions more often.

3/ On the other, I find the direct Synchrony queries often a bit
more straightforward to understand by someone who already know
a little Scala. 



4/ Synchrony iterators can be a lot more efficient than joins.
We use the last example (the one mirroring GMQL's cover query)
for illustration.  The main part of the Synchrony query is
reproduced below:

    for ( g <- lmTrack; 
          s1 <- syncedDB;
          s2 <- syncedDB;
          if (s1.meta("TF name") != s2.meta("TF name"));
          r1 <- s1.syncedTrack syncWith g;
          r2 <- s2.syncedTrack syncWith g;
          if (r1.locus.startNearStartOf(r2.locus)))
    yield (g, s1.meta, s2.meta, r1, r2, r1.locus union r2.locus)

Assuming there are
- G genes on lmTrack, 
- S samples in syncedDB,
- K items in each sample in syncedDB,
- each gene g on lmTrack canSee k items in each samples, and
- no two genes see any common items in any samples.

The complexity of this Synchrony query is O(G * S^2 * k^2 + S * K').
The S * K' part is because each of the S sample in syncedDB
has to be read once, assuming it takes O(K') time to scan
once.

 
Notice that this complexity does not depend on the total
number of items in each sample, even though it depends 
on the number of items in each sample that a gene on
lmTrack.


5/ Without Synchrony iterators, the same query can be expressed
as a nested-loop join like this:
 
    for ( g <- lmTrack; 
          s1 <- syncedDB;
          s2 <- syncedDB;
          if (s1.meta("TF name") != s2.meta("TF name"));
          r1 <- s1.track();
          if r1.locus canSee g.locus;
          r2 <- s2.track();
          if r2.locus canSee g.locus;
          if (r1.locus.startNearStartOf(r2.locus)))
    yield (g, s1.meta, s2.meta, r1, r2, r1.locus union r2.locus)

Assuming there are K items in each sample, the complexity 
of this query is O(G * S^2 * K^2). It is O(K^2 / k^2) fold
less efficient than Synchrony iterators, as K is typically
in the hundreds of thousands and k is typically in the tens.


6/ Assuming somehow some indexing can be used, given a gene g,
the complexity of finding the k items that it can see is
probably O(log K), and so the overall complexity of this
query with index is O(G * S^2 * k^2 + G * log(K) * S). 

How does the S * G * log(K) index-access overhead compares
with the S * K' scanning overhead? Since the S part cancels,
we only need to compare G * log(K) with K'. It is fast to
read a file. So the two overheads are probably similar.


7/ There are some simple engineering tricks that can be
used to speed things up, at least for this queries.
Assume there is an artificially created "ruler" landmark
track where landmarks are placed 50 bp apart. We first
intersect this ruler with gene promoters (this is easy
to express using Synchrony iterator). Then use this
intersection in place of genes landmark track in the 
Synchrony query above, and set canSee to plus/minus
50 bp (this is actually a bit wider than what near
means, and so more than enough for what is needed). 

In a plus/minus 50 bp region, there will be k/10 items
in a sample that canSee it. And there will be about
10-20 such regions in a gene promoter. The Synchrony query
complexity likely becomes a order faster at:

O(10 * G * S^2 * (k/10)^2 + (S + 1) * K') 

the + 1 K' part at the end is because computing the 
intersection of the ruler with gene promoters is about
the same complexity of a linear scan.
 


-----End-----


                   


