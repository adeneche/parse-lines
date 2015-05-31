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
import java.util.HashMap;

import freader.BufferedFileReader;
import freader.CharBufferFileReader;
import freader.FastLineReader;
import freader.FileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.Tags;

final class TextImporter2 {

  private static final Logger LOG = LoggerFactory.getLogger(TextImporter2.class);

  public static void main(String[] args) throws Exception {

    if (args.length == 0) {
      System.err.println("usage: path [buffer_size]");
      System.exit(-1);
    }

    final String path = args[0];
    final int bufferSize = args.length > 1 ? Integer.parseInt(args[1]) : 8192;

    LOG.info("path: {}", path);
    LOG.info("buffer size: {}", bufferSize);

    final long start_time = System.nanoTime();
    int points = 0;

    points += importFile(path, bufferSize);

    displayAvgSpeed(start_time, points);
  }

  private static void displayAvgSpeed(final long start_time, final int points) {
    final double time_delta = (System.nanoTime() - start_time) / 1000000000.0;
    LOG.info(String.format("Average speed: %d data points in %.3fs (%.1f points/s)",
      points, time_delta, (points / time_delta)));
  }

  /**
   * Imports a given file to TSDB
   * @return number of points imported from file
   * @throws IOException
   */
  private static int importFile(final String path, final int bufferSize) throws IOException {

    final FileReader reader = new FastLineReader();
//    final FileReader reader = new CharBufferFileReader();
//    final FileReader reader = new BufferedFileReader(bufferSize);
    String line = null;

    int points = 0;

    final long start_time = System.nanoTime();

    try {
      reader.readFile(path);
      while (reader.readln()) {
        processAndImportLine(reader);

        points++;

        if (points % 1000000 == 0) {
          displayAvgSpeed(start_time, points);
        }
      }
    } catch (RuntimeException e) {
      LOG.error("Exception caught while processing file " + path + " line=" + line);
      throw e;
    }

    return points;
  }

  private static void processAndImportLine(final FileReader reader) {
    final String metric = reader.next();
    if (metric.length() <= 0) {
      throw new RuntimeException("invalid metric: " + metric);
    }

    long timestamp = reader.nextLong();
    if (timestamp <= 0) {
      throw new RuntimeException("invalid timestamp: " + timestamp);
    }

    final String value = reader.next();
    if (value.length() <= 0) {
      throw new RuntimeException("invalid value: " + value);
    }

    final HashMap<String, String> tags = new HashMap<>();
    while (reader.hasNext()) {
      Tags.parse(tags, reader.next());
    }
  }

}