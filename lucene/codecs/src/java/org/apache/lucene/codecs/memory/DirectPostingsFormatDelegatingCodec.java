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

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IOContext;

import java.io.IOException;

/**
 * An ephemeral codec, for use in a FilterAtomicReader, that keeps postings in memory.
 * Probably a dud, but gives me something to look at.
 */
public class DirectPostingsFormatDelegatingCodec extends Codec {
  private final int minSkipCount;
  private final int lowFreqCutoff;
  private final Codec codec;

  private final static int DEFAULT_MIN_SKIP_COUNT = 8;
  private final static int DEFAULT_LOW_FREQ_CUTOFF = 32;

  public DirectPostingsFormatDelegatingCodec(Codec codec, int minSkipCount, int lowFreqCutoff) {
    super("DirectPostingFilteringCodec");
    this.codec = codec;
    this.minSkipCount = minSkipCount;
    this.lowFreqCutoff = lowFreqCutoff;
  }

  public DirectPostingsFormatDelegatingCodec(Codec codec) {
    super("DirectPostingFilteringCodec");
    this.codec = codec;
    this.minSkipCount = DEFAULT_MIN_SKIP_COUNT;
    this.lowFreqCutoff = DEFAULT_LOW_FREQ_CUTOFF;
  }

  @Override
  public PostingsFormat postingsFormat() {
    return new PostingsFormat("Direct") {
      @Override
      public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        // whatever the underlying codec's posting format says is OK with us.
        return codec.postingsFormat().fieldsConsumer(state);
      }

      @Override
      public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
        FieldsProducer postings = codec.postingsFormat().fieldsProducer(state);
        if (state.context.context != IOContext.Context.MERGE) {
          FieldsProducer loadedPostings;
          try {
            postings.checkIntegrity();
            loadedPostings = new DirectPostingsFormat.DirectFields(state, postings, minSkipCount, lowFreqCutoff);
          } finally {
            postings.close();
          }
          return loadedPostings;
        } else {
          // Don't load postings for merge:
          return postings;
        }
      }
    };
  }

  @Override
  public DocValuesFormat docValuesFormat() {
    return codec.docValuesFormat();
  }

  @Override
  public StoredFieldsFormat storedFieldsFormat() {
    return codec.storedFieldsFormat();
  }

  @Override
  public TermVectorsFormat termVectorsFormat() {
    return codec.termVectorsFormat();
  }

  @Override
  public FieldInfosFormat fieldInfosFormat() {
    return codec.fieldInfosFormat();
  }

  @Override
  public SegmentInfoFormat segmentInfoFormat() {
    return codec.segmentInfoFormat();
  }

  @Override
  public NormsFormat normsFormat() {
    return codec.normsFormat();
  }

  @Override
  public LiveDocsFormat liveDocsFormat() {
    return codec.liveDocsFormat();
  }

}
