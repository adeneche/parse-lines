// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details. You should have received a copy
// of the GNU Lesser General Public License along with this program. If not,
// see <http://www.gnu.org/licenses/>.

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;

import readers.BufferedFileReader;
import readers.BufferedOldReader;
import readers.CharBufferFileReader;
import readers.FastBufferedReader;
import readers.FastLineReader;
import readers.FileReader;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.Tags;

final class TextImporter2 {

  private static final Logger LOG = LoggerFactory.getLogger(TextImporter2.class);

  private static final Runtime runtime = Runtime.getRuntime();
  private static final DecimalFormat sizeFormatter = new DecimalFormat("#,##0.#");

  public static void main(String[] args) throws Exception {
    Options myOptions = new Options();
    CmdLineParser parser = new CmdLineParser(myOptions);

    try {
      parser.parseArgument(args);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      System.exit(-1);
    }

    LOG.info("path: {}", myOptions.input);
    LOG.info("buffer size: {}", myOptions.bufferSize);
    LOG.info("reader: {}", myOptions.reader);

    if (myOptions.showMem) {
      runtime.gc();
    }

    final long start_time = System.nanoTime();
    long points = 0;

    for (int i = 0; i < myOptions.repetitions; i++) {
      points += importFile(myOptions);
    }

    displayAvgSpeedAndMemory(start_time, points, false);
  }

  private static String formatSize(long size) {
    if(size <= 0) return "0";
    final String[] units = new String[] { "B", "kB", "MB", "GB", "TB" };
    int digitGroups = (int) (Math.log10(size)/Math.log10(1024));
    return sizeFormatter.format(size/Math.pow(1024, digitGroups)) + " " + units[digitGroups];
  }

  private static String formatPoints(long points) {
    if(points <= 0) return "0";
    final String[] units = new String[] { " p/s", "k p/s", "M p/s", "G p/s", "T p/s" };
    int digitGroups = (int) (Math.log10(points) / Math.log10(1024));
    return sizeFormatter.format(points / Math.pow(1024, digitGroups)) + " " + units[digitGroups];
  }
  private static void displayAvgSpeedAndMemory(final long start_time, final long points, boolean showMem) {
    final double time_delta = (System.nanoTime() - start_time) / 1000000000.0;
    final long usedMem = showMem ? runtime.totalMemory() - runtime.freeMemory() : 0;

    LOG.info(String.format("Average speed: %s data points in %.3fs (%s) %s",
      formatPoints(points), time_delta, formatPoints((long)(points / time_delta)), showMem ? formatSize(usedMem):""));
  }

  private static FileReader newReader(Options options) {
    switch (options.reader) {
      case BUFFERED:
        return new BufferedFileReader(options.bufferSize);
      case CHAR_BUFFER:
        return new CharBufferFileReader();
      case FAST_LINE:
        return new FastLineReader();
      case BUFFERED_OLD:
        return new BufferedOldReader(options.bufferSize);
      case FAST_BUFFER:
        return new FastBufferedReader(options.bufferSize);
      default:
        throw new IllegalArgumentException("Unkown Reader " + options.reader);
    }
  }
  /**
   * Imports a given file to TSDB
   * @return number of points imported from file
   * @throws IOException
   */
  private static long importFile(final Options options) throws IOException {

    final FileReader reader = newReader(options);

    long points = 0;
    long words = 0;
    final long start_time = System.nanoTime();

    try {
      reader.readFile(options.input);
      while (reader.readln()) {
        words += processAndImportLine(reader);

        points++;

        if (points % 1_000_000 == 0) {
          displayAvgSpeedAndMemory(start_time, points, options.showMem);
        }
      }
    } catch (RuntimeException e) {
      LOG.error("Error processing point " + points);
      throw e;
    }

    displayAvgSpeedAndMemory(start_time, points, options.showMem);
    System.out.printf("%ntotal words read %d%ntotal points read %d%n", words, points);

    return points;
  }

  private static long processAndImportLine(final FileReader reader) {
    long words = 0;

    final String metric = reader.next();
    words++;
    if (metric.length() <= 0) {
      throw new RuntimeException("invalid metric: " + metric);
    }

    long timestamp = reader.nextLong();
    words++;
    if (timestamp <= 0) {
      throw new RuntimeException("invalid timestamp: " + timestamp);
    }

    final String value = reader.next();
    words++;
    if (value.length() <= 0) {
      throw new RuntimeException("invalid value: " + value);
    }

    final HashMap<String, String> tags = new HashMap<>();
    while (reader.hasNext()) {
      Tags.parse(tags, reader.next());
      words++;
    }

    return words;
  }

  private static class Options {
    @Option(name = "-input", required = true)
    String input;
    @Option(name = "-repeat")
    int repetitions = 1;
    @Option(name = "-buffer")
    int bufferSize = 8192;
    @Option(name = "-reader")
    Reader reader = Reader.BUFFERED;
    @Option(name = "-memory")
    boolean showMem = false;

    private enum Reader {
      BUFFERED, CHAR_BUFFER, FAST_LINE, BUFFERED_OLD, FAST_BUFFER
    }
  }
}