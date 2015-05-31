package freader;

import com.google.common.base.Splitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.Tags;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;

public class CharBufferFileReader extends FileReader {

  private static final Logger LOG = LoggerFactory.getLogger(CharBufferFileReader.class);

  private char[] chars;
  private int offset = 0;
  private int index = 0;


  private Iterator<String> words;
  private final Splitter splitter = Splitter.on(" ").trimResults();

  @Override
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
    LOG.info("readFile: {}", (System.nanoTime() - t1) / 1_000_000_000.0);
  }

  @Override
  public boolean readln() {
    while (index < chars.length && chars[index] != '\n') index++;
    if (index >= chars.length) {
      return false;
    }
    String line = new String(chars, offset, chars[index] == '\r' ? index - offset : index - offset + 1);
    words = splitter.split(line).iterator();

    offset = index + 2;
    index = offset;
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