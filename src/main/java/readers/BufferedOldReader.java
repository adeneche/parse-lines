package readers;

import com.google.common.base.Charsets;
import utils.Tags;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

public class BufferedOldReader extends FileReader {
  private BufferedReader reader;

  private final int bufferSize;

  private String[] words;
  private int fieldId;


  public BufferedOldReader(final int bufferSize) {
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

    words = Tags.splitString(line, ' ');
    fieldId = 0;
    return true;
  }

  @Override
  public boolean hasNext() {
    return fieldId < words.length;
  }

  @Override
  public String next() {
    assert hasNext();
    return words[fieldId++];
  }

  @Override
  public long nextLong() {
    return Tags.parseLong(next());
  }
}
