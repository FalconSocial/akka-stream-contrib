/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.contrib

import akka.actor.ActorSystem
import akka.stream.contrib.MapConcatAsync._
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import org.scalatest.{FlatSpec, GivenWhenThen}

import scala.collection.{immutable, mutable}
import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.language.postfixOps

/**
 * Created by szilard on 2/15/17.
 */
class MapConcatAsyncSpec extends FlatSpec with GivenWhenThen {

  implicit val system = ActorSystem("Test")
  implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system))
  implicit val dispatcher: ExecutionContext = system.dispatcher

  def echo[O](expectedOutput: O, delay: FiniteDuration = 50 millis): Future[O] = {
    val p = Promise[O]
    system.scheduler.scheduleOnce(delay = delay)(p.success(expectedOutput))
    p.future
  }

  behavior of "mapConcatAsync"

  it should "complete the stage only after all item was emitted" in {
    testSingleInput(1, 1 to 5)
  }

  it should "complete the stage only after all item was emitted in case of single item too" in {
    testSingleInput(1, Seq(1))
  }

  it should "complete the stage only after all item was emitted in case of zero item too" in {
    testSingleInput(1, Seq.empty[Int])
  }

  private def testSingleInput[T, O](input: T, expectedOutput: immutable.Seq[O]) = {
    testMultipleInput(Seq(input -> expectedOutput))
  }

  it should "consume each input and emit all output before completion" in {
    testMultipleInput(Seq((1, Seq(11, 12)), (2, Seq(21, 22)), (3, Seq.empty[Int]), (4, Seq(41))))
  }

  private def testMultipleInput[T, O](inputOutput: immutable.Seq[(T, immutable.Seq[O])]) = {
    Given("input")
    val queue = mutable.Queue.empty[Seq[O]]
    queue.enqueue(inputOutput.map(_._2): _*)
    def loader: T => Future[Seq[O]] = { _ => echo(queue.dequeue()) }

    When("mapConcatAsync receive the inputs")
    var testProbe = Source.fromIterator(() => inputOutput.map(_._1).toIterator)
      .mapConcatAsync(loader)
      .runWith(TestSink.probe[O])
    val expectedResults = inputOutput.map(_._2).flatten
    testProbe.request(expectedResults.size.toLong + 1)
    Then("it should consume all input and emit all result from the loader before completion")
    for (expectedResult <- expectedResults) {
      testProbe = testProbe.expectNext(expectedResult)
    }
    testProbe.expectComplete()
  }

}
