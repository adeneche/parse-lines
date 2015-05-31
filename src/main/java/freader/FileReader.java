package freader;

import java.io.IOException;
import java.util.Iterator;

public abstract class FileReader implements Iterator<String> {
  public abstract void readFile(String fileName) throws IOException;

  public abstract boolean readln() throws IOException;

  public abstract long nextLong();

  @Override
  public void remove() {
    throw new UnsupportedOperationException("FileReader.remove() not supported");
  }
}
