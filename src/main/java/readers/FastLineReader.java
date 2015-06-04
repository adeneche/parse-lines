package readers;

import utils.FastLine;

import java.io.FileInputStream;
import java.io.IOException;

public class FastLineReader extends FileReader {

  private FastLine.FastLineReader reader;
  private FastLine line;
  private int field;

  @Override
  public void readFile(String fileName) throws IOException {
    reader = new FastLine.FastLineReader(new FileInputStream(fileName));
  }

  @Override
  public boolean readln() throws IOException {
    field = 0;
    line = reader.read();
    return line != null;
  }

  @Override
  public long nextLong() {
    return line.getLong(field++);
//    return Tags.parseLong(line.getString(field++));
  }

  @Override
  public boolean hasNext() {
    return field < line.getSize();
  }

  @Override
  public String next() {
    return line.getString(field++);
  }
}
