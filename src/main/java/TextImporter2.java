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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.zip.GZIPInputStream;

import com.google.common.base.Splitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class TextImporter2 {

  private static final Logger LOG = LoggerFactory.getLogger(TextImporter2.class);

  private static final Splitter splitter = Splitter.on(" ").trimResults();

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

//    final BufferedReader in = open(path, bufferSize);
    final FileReader reader = new FileReader();
    String line = null;

    int points = 0;

    final long start_time = System.nanoTime();

    try {
      reader.readFile(path);
      while ((line = reader.readln()) != null) {
        processAndImportLine(splitter.split(line));

        points++;

        if (points % 1000000 == 0) {
          displayAvgSpeed(start_time, points);
        }
      }
    } catch (RuntimeException e) {
      LOG.error("Exception caught while processing file " + path + " line=" + line);
      throw e;
    } finally {
//      in.close();
    }

    return points;
  }

  private static void processAndImportLine(final Iterable<String> words) {
    Iterator<String> iterator = words.iterator();
    final String metric = iterator.next();
    if (metric.length() <= 0) {
      throw new RuntimeException("invalid metric: " + metric);
    }

    long timestamp = Tags.parseLong(iterator.next());
    if (timestamp <= 0) {
      throw new RuntimeException("invalid timestamp: " + timestamp);
    }

    final String value = iterator.next();
    if (value.length() <= 0) {
      throw new RuntimeException("invalid value: " + value);
    }

    final HashMap<String, String> tags = new HashMap<String, String>();
    while (iterator.hasNext()) {
      Tags.parse(tags, iterator.next());
    }
  }

  /**
   * Opens a file for reading, handling gzipped files.
   * @param path The file to open.
   * @return A buffered reader to read the file, decompressing it if needed.
   * @throws IOException when shit happens.
   */
//  private static BufferedReader open(final String path, final int bufferSize) throws IOException {
//    InputStream is = new FileInputStream(path);
//    if (path.endsWith(".gz")) {
//      is = new GZIPInputStream(is);
//    }
//    // I <3 Java's IO library.
//    return new BufferedReader(new InputStreamReader(is), bufferSize);
//  }

}