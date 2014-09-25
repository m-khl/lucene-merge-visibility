package org.apache.lucene.index;

import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.asserting.AssertingCodec;
import org.apache.lucene.codecs.asserting.AssertingDocValuesFormat;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.store.NRTCachingDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

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

@SuppressWarnings("resource")
@org.apache.lucene.util.LuceneTestCase.SuppressSysoutChecks(bugUrl="")
public class TestNumDValUpdVsReaderVisibility extends LuceneTestCase {

  private static Directory dir;
  private static IndexWriter writer;
  
  private static  boolean hardReopenBeforeDVUpdate = true/* PASS */ /*false FAIL*/;
  
  private static int docPairsToTest=1000;

  private Document doc(int id) {
    Document doc = new Document();
    doc.add(new StringField("id", "doc-" + id, Store.NO));
    // make sure we don't set the doc's value to 0, to not confuse with a document that's missing values
    doc.add(new NumericDocValuesField("val", -1));
    return doc;
  }
  
  @BeforeClass
  public static void openWriter() throws IOException{
    dir = newDirectory();
    IndexWriterConfig conf= makeConf();
    writer = new IndexWriter(dir, conf);
  }

  private static IndexWriterConfig makeConf() {
    IndexWriterConfig conf;
    conf = newIndexWriterConfig(new MockAnalyzer(random()));
    conf.setMergeScheduler(new SerialMergeScheduler());
    LogByteSizeMergePolicy mergePolicy = new LogByteSizeMergePolicy();
    mergePolicy.setMergeFactor(10);
    conf.setMergePolicy(mergePolicy);
    conf.setUseCompoundFile(false);
    conf.setInfoStream(System.out);
    // make sure random config doesn't flush on us
    conf.setMaxBufferedDocs(1000);
    conf.setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
    return conf;
  }

  @AfterClass
  public static void closeWriter() throws IOException{
    writer.close();
    dir.close();
  }

  @Test
  public void testSimple() throws Exception {

    DirectoryReader reader =null;
    for(int addDoc=0; addDoc<docPairsToTest; ){
      // write a pair of docs 
      Term pk=null;
      int pkDocNum=-1;
      for(int times=0;times<2;times++){
        if(pk==null){
          pk = new Term("id", "doc-"+addDoc);
          pkDocNum = addDoc;
        }
        writer.addDocument(doc(addDoc++)); 
      }
      writer.commit();
      
      reader = hardReopenBeforeDVUpdate  ? readerReopenHard(reader, writer, dir) 
          : readerReopenIfChanged(reader, writer, dir);
      
      int segmantsBeforeDVCommit = reader.leaves().size();
      
      IndexSearcher indexSearcher = new IndexSearcher(reader);
      // find recently added doc, and update it with its' docNum
        {
          final TopDocs forUpdate = indexSearcher.search(new TermQuery(pk), 2);
          assertEquals(1, forUpdate.totalHits);
          writer.updateNumericDocValue(pk, "val", forUpdate.scoreDocs[0].doc);
        }
        //apply docval upd 
      writer.commit();
      // look on it again
      reader = readerReopenIfChanged(reader, writer, dir);
      
      int segmentsAfterDVCommit = reader.leaves().size();
      
      if(segmantsBeforeDVCommit!=segmentsAfterDVCommit){
        System.err.println("dv upd commit exposed merged solid segment");
        assertEquals(1, segmentsAfterDVCommit);
        /// it fails at assertEquals(checkingDoc, dvAct); below anyway
      }
      
      indexSearcher = new IndexSearcher(reader);
      final TopDocs toCheck = indexSearcher.search(new TermQuery(pk), 2);
      assertEquals(1, toCheck.totalHits);
      int checkingDoc = toCheck.scoreDocs[0].doc;

      LeafReaderContext segment = reader.leaves().get(
          ReaderUtil.subIndex(checkingDoc, reader.leaves()));
      long dvAct = segment.reader().getNumericDocValues("val").get(checkingDoc-segment.docBase);
      assertEquals(checkingDoc, dvAct);
      
      if(pkDocNum==0){ // need to have a hole to detect the merge
        writer.deleteDocuments(pk);
      }
    }
      reader.close();
  }

  private DirectoryReader readerReopenIfChanged(DirectoryReader oldReader, IndexWriter writer2,
      Directory dir2) throws IOException {
    if(oldReader==null){
      return DirectoryReader.open(dir);
    }else{
      DirectoryReader reopenReader = DirectoryReader.openIfChanged(oldReader);
      assertNotNull(reopenReader);
      assertNotSame(reopenReader, oldReader);
      oldReader.close();
      return reopenReader; 
    }
  }
  
  private DirectoryReader readerReopenHard(DirectoryReader oldReader, IndexWriter writer2,
      Directory dir2) throws IOException {
    if(oldReader==null){
      return DirectoryReader.open(dir);
    }else{
      // let it flush merged segment
      writer2.close();
      writer = new IndexWriter(dir, makeConf());
      oldReader.close();
      DirectoryReader reopenReader = DirectoryReader.open(dir);
      return reopenReader; 
    }
  }
}
