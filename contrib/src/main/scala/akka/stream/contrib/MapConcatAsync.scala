/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.contrib

import akka.stream._
import akka.stream.scaladsl.{ Flow, Keep, Source }
import akka.stream.stage.{ GraphStage, GraphStageLogic, InHandler, OutHandler }

import scala.concurrent.Future
import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }
import scala.collection.immutable

/**
 * Created by szilard on 2/14/17.
 */
object MapConcatAsync {

  implicit class FlowWithMapConcatAsync[IN, FO, OUT, M](val self: Flow[IN, FO, M]) {
    def mapConcatAsync(loader: FO => Future[immutable.Iterable[OUT]]): Flow[IN, OUT, M] = {
      self.viaMat(new MapConcatAsync[FO, OUT](loader))(Keep.left)
    }
  }

  implicit class SourceWithMapConcatAsync[SO, OUT, M](val self: Source[SO, M]) {
    def mapConcatAsync(loader: SO => Future[immutable.Iterable[OUT]]): Source[OUT, M] = {
      self.viaMat(new MapConcatAsync[SO, OUT](loader))(Keep.left)
    }
  }

}

private[akka] class MapConcatAsync[I, O](loader: I => Future[immutable.Iterable[O]]) extends GraphStage[FlowShape[I, O]] {

  val in: Inlet[I] = Inlet[I]("MapConcatAsync.In")
  val out: Outlet[O] = Outlet("MapConcatAsync.Out")

  override def shape: FlowShape[I, O] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    var items: immutable.Iterable[O] = immutable.Iterable.empty[O]
    var stageCompleting = false
    var waitingForResult = false

    setHandler(in, new InHandler {
      override def onPush(): Unit = {
        val input = grab(in)
        val contentLoader = getAsyncCallback(loadContent)
        waitingForResult = true
        loader(input).onComplete(result => contentLoader.invoke(result))(scala.concurrent.ExecutionContext.Implicits.global)
      }

      override def onUpstreamFinish(): Unit = {
        stageCompleting = true
        completeStageWhenDone()
      }
    })

    private def loadContent(result: Try[immutable.Iterable[O]]): Unit = result match {
      case Success(sources) =>
        items = sources
        waitingForResult = false
        pushWhileCan()
        pullIfNeed()
        completeStageWhenDone()
      case Failure(NonFatal(e)) =>
        throw e
    }

    setHandler(out, new OutHandler {
      override def onPull(): Unit = {
        pushWhileCan()
        pullIfNeed()
        completeStageWhenDone()
      }
    })

    private def pushWhileCan(): Unit = {
      while (items.nonEmpty && isAvailable(out)) {
        val item = items.head
        items = items.tail
        push(out, item)
      }
    }

    private def pullIfNeed(): Unit = {
      if (items.isEmpty && !stageCompleting && !waitingForResult && !hasBeenPulled(in)) {
        pull(in)
      }
    }

    private def completeStageWhenDone(): Unit = {
      if (items.isEmpty && stageCompleting && !waitingForResult) {
        completeStage()
      }
    }
  }
}
