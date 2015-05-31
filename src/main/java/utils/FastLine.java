package utils;

import com.google.common.base.Charsets;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public final class FastLine {
  private static final int MAX_IDX = 1000;
  public static final int SEPARATOR_CHAR = ' ';

  private final ByteBuffer base;
  private final int[] start = new int[MAX_IDX];
  private final int[] length = new int[MAX_IDX];
  private int count;

  private FastLine(ByteBuffer base) {
    this.base = base;
  }

  public int getSize() {
    return count;
  }

  public static FastLine read(ByteBuffer buf) {
    FastLine r = new FastLine(buf);
    r.start[r.count] = buf.position();
    int offset = buf.position();
    while (offset < buf.limit()) {
      int ch = buf.get();
      offset = buf.position();
      switch (ch) {
        case '\n':
          r.length[r.count] = offset - r.start[r.count] - 1;
          return r;
        case SEPARATOR_CHAR:
          r.length[r.count] = offset - r.start[r.count] - 1;
          r.start[++r.count] = offset;
          break;
        default:
          // nothing to do for now
      }
    }
    throw new IllegalArgumentException("Not enough bytes in buffer");
  }

  public long getLong(int field) {
    final int n = length[field];  // Will NPE if necessary.
    final int offset = start[field];
    if (n == 0) {
      throw new NumberFormatException("Empty string");
    }
    byte c = base.get(offset);  // Current character.
    int i = 1;  // index in `s'.
    if (c < '0' && (c == '+' || c == '-')) {  // Only 1 test in common case.
      if (n == 1) {
        throw new NumberFormatException("Just a sign, no value: " + getString(field));
      } else if (n > 20) {  // "+9223372036854775807" or "-9223372036854775808"
        throw new NumberFormatException("Value too long: " + getString(field));
      }
      c = base.get(offset + 1);
      i = 2;  // Skip over the sign.
    } else if (n > 19) {  // "9223372036854775807"
      throw new NumberFormatException("Value too long: " + getString(field));
    }
    long v = 0;  // The result (negated to easily handle MIN_VALUE).
    do {
      if ('0' <= c && c <= '9') {
        v -= c - '0';
      } else {
        throw new NumberFormatException("Invalid character '" + c + "' in " + getString(field));
      }
      if (i == n) {
        break;
      }
      v *= 10;
      c = base.get(offset + i++);
    } while (true);
    if (v > 0) {
      throw new NumberFormatException("Overflow in " + getString(field));
    } else if (base.get(offset) == '-') {
      return v;  // Value is already negative, return unchanged.
    } else if (v == Long.MIN_VALUE) {
      throw new NumberFormatException("Overflow in " + getString(field));
    } else {
      return -v;  // Positive value, need to fix the sign.
    }
  }

  public double getDouble(int field) {
    int offset = start[field];
    int size = length[field];
    switch (size) {
      case 1:
        return base.get(offset) - '0';
      case 2:
        return (base.get(offset) - '0') * 10 + base.get(offset + 1) - '0';
      default:
        double r = 0;
        for (int i = 0; i < size; i++) {
          r = 10 * r + base.get(offset + i) - '0';
        }
        return r;
    }
  }

  public String getString(int field) {
    return new String(base.array(), start[field], length[field], Charsets.UTF_8);
  }

  public static final class FastLineReader implements Closeable {
    private final InputStream in;
    private final ByteBuffer buf = ByteBuffer.allocate(100000);

    public FastLineReader(InputStream in) throws IOException {
      this.in = in;
      buf.limit(0);
      fillBuffer();
    }

    public FastLine read() throws IOException {
      fillBuffer();
      if (buf.remaining() > 0) {
        return FastLine.read(buf);
      } else {
        return null;
      }
    }

    private void fillBuffer() throws IOException {
      if (buf.remaining() < 10000) {
        buf.compact();
        int n = in.read(buf.array(), buf.position(), buf.remaining());
        if (n == -1) {
          buf.flip();
        } else {
          buf.limit(buf.position() + n);
          buf.position(0);
        }
      }
    }

    @Override
    public void close() throws IOException {
      in.close();
    }
  }}