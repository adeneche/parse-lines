package freader;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

public class BufferedFileReader implements FileReader {
  private BufferedReader reader;

  private final int bufferSize;

  public BufferedFileReader(final int bufferSize) {
    this.bufferSize = bufferSize;
  }

  @Override
  public void readFile(String fileName) throws IOException {
    InputStream is = new FileInputStream(fileName);
    if (fileName.endsWith(".gz")) {
      is = new GZIPInputStream(is);
    }
    // I <3 Java's IO library.
    reader =  new BufferedReader(new InputStreamReader(is), bufferSize);
  }

  @Override
  public String readln() throws IOException {
    return reader.readLine();
  }
}
