package freader;

import java.io.IOException;

public interface FileReader {
  void readFile(String fileName) throws IOException;

  String readln() throws IOException;
}
