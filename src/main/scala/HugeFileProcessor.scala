/**
  * Original C# code copyright (c) 2016 Mikhail Barg.
  * Scala port copyright (c) 2016 Alexander Terenin.
  * All rights reserved.
  *
  * This work is licensed under the terms of the MIT license.
  * For a copy, see https://opensource.org/licenses/MIT.
  * /
  */
import java.io.{BufferedWriter, FileOutputStream, OutputStreamWriter}
import scala.io.Source.fromFile
import scala.util.{Random, Sorting}

object HugeFileProcessor {
  final val VERBOSE_LINES_COUNT = 100000

  def main(args: Array[String]) = args.headOption.getOrElse("") match {
    case "-shuffle" if args.length == 3 || args.length == 4 =>
      mainShuffle(args(1), args(2).toInt, if(args.length == 4) args(3) else s"${args(1)}.shuffled")
    case "-split" if args.length == 3 || args.length == 4 =>
      val splitFracStr = args(2).split("""/""") //a fraction of test data in form "1/10". To skip creation of test data, use "0/1"
    val testFracUp = splitFracStr.head.toInt
      val testFracDown = splitFracStr(1).toInt
      val processLinesLimit = if (args.length == 4) args(3).toInt else Int.MaxValue //number of lines to process from file or -1 if all.

      mainSplit(args(1), testFracUp, testFracDown, processLinesLimit);
    case "-count" if args.length == 2 =>
      val (linesCount, singlePassTime) = countLines(args(1))
      println(s"$linesCount")
    case _ =>
      println(
        """Usage :
          |-split <sourceFile> <test>/<base> [<linesLimit>]
          |   splits <sourceFile> to test and train, so that test file get (<test>/<base>) fraction of lines.
          |   Set 0 to <test> to skip test file creation.
          |   <linesLimit> - total number of lines to proces from <sourceFile>, set to -1 or skip to read all lines.
          |-shuffle <sourceFile> <batchSize> [<outFile>]
          |   shuffles lines from <sourceFile> to <outFile>.
          |   <batchSize> is in lines.
          |-count <sourceFile>
          |   just count lines int <sourceFile>
        """.stripMargin)
  }

  def mainShuffle(sourceFileName: String, batchSizeLines: Int, targetFileName: String) {
    println(s"Shuffling lines from $sourceFileName to $targetFileName in batches of $batchSizeLines")

    val (linesCount, singlePassTime) = countLines(sourceFileName)
    val batchesCount = math.ceil(linesCount.toDouble / batchSizeLines.toDouble).toInt

    println(s"Expecting $batchesCount batches, that would take ${(singlePassTime * batchesCount).millisToFormattedString}")

    val orderArray = getOrderArray(linesCount)

    println("Writing to file")
    val start = System.currentTimeMillis()
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(targetFileName)))

    var batchIndex = 0
    for{
      batchStart <- 0 until linesCount by batchSizeLines
    } {
      batchIndex = batchIndex + 1
      println(s"Starting batch $batchIndex")
      val batchEnd = if(batchStart + batchSizeLines - 1 >= linesCount)
        linesCount - 1
      else
        batchStart + batchSizeLines - 1
      processBatch(sourceFileName, orderArray, batchStart, batchEnd, writer)
      val took = System.currentTimeMillis() - start
      println(s"Batch done, took $took, speed is ${math.round(batchEnd.toDouble / took.millisToSeconds)} lps. Remaining ${(took * (batchesCount - batchIndex) / batchIndex).millisToFormattedString}")
    }
    writer.close()
    println(s"Done, took ${(System.currentTimeMillis() - start).millisToFormattedString}")
  }

  def processBatch(sourceFileName: String, orderArray: Array[Int], batchStart: Int, batchEnd: Int, writer: BufferedWriter): Unit = {
    val batchSize = batchEnd - batchStart + 1

    val batchLines = Array.ofDim[(Int, Int)](batchEnd - batchStart + 1)
    for(i <- 0 until batchSize) {
      batchLines(i) = (orderArray(batchStart + i), i)
    }
    Sorting.quickSort(batchLines) // tuples are ordered by the ordering inside the elements in the tuple

    val writeLines = Array.ofDim[String](batchSize)

    val reader = fromFile(sourceFileName).getLines()
    var lineIndex = -1
    for((key, value) <- batchLines) {
      var s = ""
      while(lineIndex < key) {
        s = reader.next()
        lineIndex = lineIndex + 1
      }
      writeLines(value) = s
    }

    for(writeLine <- writeLines)
      writer.write(s"$writeLine\n")

    System.gc()
  }

  def countLines(fileName: String): (Int, Long) = {
    println(s"Counting lines in $fileName")
    val start = System.currentTimeMillis()

    var linesCount = 0
    for(s <- fromFile(fileName).getLines() if s.trim().length != 0) {
      linesCount = linesCount + 1

      if (linesCount % VERBOSE_LINES_COUNT == 0) {
        val took = System.currentTimeMillis() - start
        println(s"Current count is $linesCount, took $took, speed is ${math.round(linesCount.toDouble / took.millisToSeconds.toDouble)} lps");
      }
    }

    val totalTime = System.currentTimeMillis() - start
    println(s"Done. Lines count is $linesCount, took $totalTime, speed is ${math.round(linesCount.toDouble / totalTime.millisToSeconds.toDouble)} lps");

    (linesCount, totalTime)
  }


  def getOrderArray(linesCount: Int) = {
    println("Creating order array")
    val start = System.currentTimeMillis()

    val orderArray = (0 until linesCount).toArray
    for(i <- 0 until (linesCount - 1)) {
      val j = i + Random.nextInt(linesCount - i)
      val tmp = orderArray(i)
      orderArray(i) = orderArray(j)
      orderArray(j) = tmp
    }

    println(s"Done, took ${(System.currentTimeMillis() - start).millisToFormattedString}")
    orderArray
  }


  def mainSplit(sourceFileName: String, testFracUp: Int, testFracDown: Int, processLinesLimit: Int) {
    if (testFracUp > 0)
      println(s"Splitting lines from $sourceFileName into .test and .train parts. Test gets $testFracUp/$testFracDown, train gets ${testFracDown - testFracUp}/$testFracDown")
    else
      println(s"Processing lines from $sourceFileName into .train")

    val outFileInfix = if (processLinesLimit == Int.MaxValue)
      ""
    else {
      println(s"Limiting total lines number to $processLinesLimit")
      s".$processLinesLimit"
    }

    val trainWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(s"$sourceFileName$outFileInfix.train")))
    val testWriter = if (testFracUp > 0)
      Option(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(s"$sourceFileName$outFileInfix.test"))))
    else
      None

    var lineIndex = 0
    for {
      sourceLine <- fromFile(sourceFileName).getLines() if lineIndex < processLinesLimit
    } {
      lineIndex = lineIndex + 1
      if ((lineIndex - 1) % testFracDown < testFracUp)
        testWriter.foreach(_.write(s"$sourceLine\n"))
      else
        trainWriter.write(s"$sourceLine\n")

      if (lineIndex % VERBOSE_LINES_COUNT == 0)
        println(s"Processed $lineIndex lines")

      if (lineIndex >= processLinesLimit)
        println(s"Processed up to limit. Stopping")
    }
    trainWriter.close()
    testWriter.foreach(_.close())
    println(s"Finished. Processed $lineIndex lines")
  }

  implicit class TimeImplicits(t: Long) {
    def millisToSeconds = t.toDouble / 1000.0
    def millisToFormattedString = {
      val hourPortion = t - (t % 3600000L)
      val minutePortion = (t - hourPortion) - ((t - hourPortion) % 60000L)
      val secondPortion = t - hourPortion - minutePortion
      val hours = hourPortion / 3600000L
      val minutes = minutePortion / 60000L
      val seconds = secondPortion / 1000L
      s"${hours}h ${minutes}m ${seconds}s"
    }
  }
}