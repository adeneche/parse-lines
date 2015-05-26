import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;

public class FileReader {
  private char[] chars;
  private int offset = 0;
  private int index = 0;

  public void readFile(String fileName) {
    long t1  = System.nanoTime();
    try (FileChannel fChan = (FileChannel) Files.newByteChannel(Paths.get(fileName))) {
      long fSize = fChan.size();
      MappedByteBuffer mbb = fChan.map(FileChannel.MapMode.READ_ONLY, 0, fSize);
      mbb.order(ByteOrder.LITTLE_ENDIAN);
      CharBuffer buffer = Charset.forName("UTF-8").decode(mbb);
      if (buffer.hasArray())
        chars = buffer.array();
    } catch (IOException e) {
      System.out.println(e.toString());
    }
    System.out.println("readFile: " + (System.nanoTime() - t1) / 1_000_000_000.0);
  }

  public String readln() {
    while (index < chars.length && chars[index] != '\n') index++;
    String str = index < chars.length ? new String(chars, offset, chars[index] == '\r' ? index - offset : index - offset + 1) : null;
    offset = index + 2;
    index = offset;
    return str;
  }
}