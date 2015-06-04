package readers;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import utils.Tags;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.zip.GZIPInputStream;

public class BufferedFileReader extends FileReader {
  private BufferedReader reader;

  private final int bufferSize;

  private Iterator<String> words;
  private final Splitter splitter = Splitter.on(" ").trimResults();


  public BufferedFileReader(final int bufferSize) {
    this.bufferSize = bufferSize;
  }

  @Override
  public void readFile(String fileName) throws IOException {
    InputStream is = new FileInputStream(fileName);
    if (fileName.endsWith(".gz")) {
      is = new GZIPInputStream(is);
    }
    reader =  new BufferedReader(new InputStreamReader(is, Charsets.UTF_8), bufferSize);
  }

  @Override
  public boolean readln() throws IOException {
    String line = reader.readLine();
    if (line == null) {
      return false;
    }

    words = splitter.split(line).iterator();
    return true;
  }

  @Override
  public boolean hasNext() {
    return words.hasNext();
  }

  @Override
  public String next() {
    assert words.hasNext();
    return words.next();
  }

  @Override
  public long nextLong() {
    assert words.hasNext();
    return Tags.parseLong(words.next());
  }
}
