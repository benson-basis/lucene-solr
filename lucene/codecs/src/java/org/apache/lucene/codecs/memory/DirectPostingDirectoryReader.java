package org.apache.lucene.codecs.memory;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FilterAtomicReader;
import org.apache.lucene.index.FilterDirectoryReader;

import java.io.IOException;

/**
 * A {@link org.apache.lucene.index.FilterDirectoryReader} that sets up in-memory caching of the index via
 * {@link org.apache.lucene.codecs.memory.DirectPostingsFormatDelegatingCodec}.
 */
public class DirectPostingDirectoryReader extends FilterDirectoryReader {

  private final static int DEFAULT_MIN_SKIP_COUNT = 8;
  private final static int DEFAULT_LOW_FREQ_CUTOFF = 32;

  private final int minSkipCount;
  private final int lowFreqCutoff;

  public DirectPostingDirectoryReader(DirectoryReader in, int minSkipCount, int lowFreqCutoff) {
    super(in, new DPFReaderWrapper(minSkipCount, lowFreqCutoff));
    this.minSkipCount = minSkipCount;
    this.lowFreqCutoff = lowFreqCutoff;
  }

  public DirectPostingDirectoryReader(DirectoryReader in) {
    this(in, DEFAULT_MIN_SKIP_COUNT, DEFAULT_LOW_FREQ_CUTOFF);
  }

  @Override
  protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) {
    return new DirectPostingDirectoryReader(in, minSkipCount, lowFreqCutoff);
  }

  /**
   * A filter around a single SegmentReader that hold the postings in memory
   * to avoid re-reading then.
   */
  static class DPFAtomicFilterReader extends FilterAtomicReader {
    private final int minSkipCount;
    private final int lowFreqCutoff;

    /**
     * <p>Construct a FilterAtomicReader based on the specified base reader.
     * <p>Note that base reader is closed if this FilterAtomicReader is closed.</p>
     *
     * @param in specified base reader.
     */
    public DPFAtomicFilterReader(AtomicReader in, int minSkipCount, int lowFreqCutoff) {
      super(in);
      this.minSkipCount = minSkipCount;
      this.lowFreqCutoff = lowFreqCutoff;
    }

    // The only override of the default delegation is the fields.
    @Override
    public Fields fields() throws IOException {
      return new DirectPostingsFormat.DirectFields(in.getFieldInfos(), in.fields(), minSkipCount, lowFreqCutoff);
    }
  }

  /**
   * Glue class to fit {@link org.apache.lucene.codecs.memory.DirectPostingDirectoryReader.DPFAtomicFilterReader}
   * into {@link org.apache.lucene.index.FilterDirectoryReader}.
   */
  static class DPFReaderWrapper extends SubReaderWrapper {
    private final int minSkipCount;
    private final int lowFreqCutoff;

    DPFReaderWrapper(int minSkipCount, int lowFreqCutoff) {
      this.minSkipCount = minSkipCount;
      this.lowFreqCutoff = lowFreqCutoff;
    }

    @Override
    public AtomicReader wrap(AtomicReader reader) {
      return new DPFAtomicFilterReader(reader, minSkipCount, lowFreqCutoff);
    }
  }
}
