
/** This module provides several types of thread pool managers
 *  to support parallelization of the GMQL emulation.
 *
 *  Wong Limsoon
 *  21/3/2021
 */ 



package synchrony.pargmql


object ExecutionManagers {

  import java.util.concurrent.{ Executors, ExecutorService, ForkJoinPool }
  import scala.concurrent.{ 
    Future, Await, ExecutionContextExecutorService, ExecutionContext }
  import scala.concurrent.duration.Duration
  import synchrony.iterators.SyncCollections.EIterator


  class ExecutionMgr(mkPool: Int => ExecutorService) {

    private val concurrency = Runtime.getRuntime.availableProcessors()

    def run[A](codes: => Vector[() => A]): EIterator[A] =
      new EIterator[A] {
	val executors: ExecutorService = mkPool(concurrency)
        implicit val pool = ExecutionContext.fromExecutorService(executors)
        val futures = codes.map(a => Future{ a() }).iterator
        override def myHasNext = futures.hasNext
        override def myNext() = Await.result(futures.next(), Duration.Inf)
        override def myClose() = pool.shutdownNow()
    }

    def runBig[A](codes: => EIterator[() => A]): EIterator[A] = 
      new EIterator[A] {
        val executors: ExecutorService = mkPool(concurrency)
        implicit val pool = ExecutionContext.fromExecutorService(executors)
        val todo = codes
        val groups: EIterator[Vector[() => A]] = todo.grouped(25)
             // 300 at-a-time in parallel.
        val futures = for (
                        grp <- groups; 
                        g <- grp.map(a => Future{ a() }))
                      yield g
        override def myHasNext = futures.hasNext
        override def myNext() = Await.result(futures.next(), Duration.Inf)
        override def myClose() = { todo.close(); pool.shutdownNow() }
    }

    def runVec[A](codes: => Vector[() => A]): Vector[A] = {
        val executors: ExecutorService = mkPool(concurrency)
        implicit val pool = ExecutionContext.fromExecutorService(executors)
        val futures = Future.sequence(codes.map(a => Future{ a() }))
        val result = Await.result(futures, Duration.Inf)
        pool.shutdownNow()
        result
    }

    def runGlobalVec[A](codes: => Vector[() => A]): Vector[A] = {
        implicit val pool = ExecutionContext.global
        val futures = Future.sequence(codes.map(a => Future{ a() }))
        val result = Await.result(futures, Duration.Inf)
        result
    }
  }

//
// For somewhat IO-intensive applications
//

  def mkCachedThreads(cc: Int) = Executors.newCachedThreadPool()

  val ParGMQLMgr = new ExecutionMgr(mkCachedThreads _)


//
// For heavy/blocking IO-intensive applications
//

  private def mkFixedThreads(cc: Int) = Executors.newFixedThreadPool(cc * 5)
  
  val IOHeavyMgr = new ExecutionMgr(mkFixedThreads _)


//
// For cpu-intensive applications
//

  private def mkFJThreads(cc: Int) = new ForkJoinPool(cc)

  val FJMgr = new ExecutionMgr(mkFJThreads _)


}
