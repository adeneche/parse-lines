package readers;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import utils.MyFastBufferedInputStream;
import utils.Tags;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class FastBufferedReader extends FileReader {

  private final byte[] bytes;
  private int pos;
  private int avail;


  private String[] words;
  private int fieldId;

  private MyFastBufferedInputStream reader;

  public FastBufferedReader(final int bufferSize) {
    Preconditions.checkArgument(bufferSize > 0, "bufferSize cannot be negative");
    this.bytes = new byte[bufferSize];
  }

  @Override
  public void readFile(String fileName) throws IOException {
    final InputStream in = new FileInputStream(fileName);
    reader = new MyFastBufferedInputStream(in, bytes);
  }

  @Override
  public boolean readln() throws IOException {
    int length = 0;
    //TODO when we reach the end of the buffer we need to move the current line to the beginning of the buffer
    int b = -1;
    do {
      if (avail == 0) {
        avail = reader.readBuffer();
        pos = 0;
      }
      if (pos < avail) {
        b = bytes[pos++];
        avail--;
      }
    } while(b != '\n' && b != -1);
    while ( (b = bytes[pos++]) != -1 && b != '\n') {
      length++;
      if (pos == bytes.length) {
        reader.readBuffer();
      }
    }

    if (length == 0) {
      return false;
    }

    words = splitString(bytes, pos, length, ' ');
    fieldId = 0;
    pos += length + 1; // +1 for the new_line byte

    return true;
  }

  private static String[] splitString(final byte[] bytes, final int offset, final int length, final char c) {
    int num_substrings = 1;
    for (int i = 0; i < length; i++) {
      if (bytes[offset + i] == c) {
        num_substrings++;
      }
    }

    final String[] result = new String[num_substrings];
    int start = 0;  // starting index in chars of the current substring.
    int pos = 0;    // current index in chars.
    int i = 0;      // number of the current substring.
    for (; pos < length; pos++) {
      if (bytes[offset + pos] == c) {
        result[i++] = new String(bytes, offset + start, pos - start, Charsets.UTF_8);
        start = pos + 1;
      }
    }
    result[i] = new String(bytes, offset + start, pos - start, Charsets.UTF_8);
    return result;
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
