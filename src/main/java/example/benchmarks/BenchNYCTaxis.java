package example.benchmarks;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Paths;

public class BenchNYCTaxis {
    public static void main(String[] args) throws IOException {
        try (IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get("/home/ec2-user/test_rucene")))) {

            IndexSearcher indexSearcher = new IndexSearcher(reader);

            long startTime = System.nanoTime();

            for (int i = 0; i < 500; i++) {
                indexSearcher.search(new MatchAllDocsQuery(), 2000);
            }

            double elapsed = (System.nanoTime() - startTime) / 1_000_000_000.0;

            System.out.println("Total time taken for match all docs query is " + elapsed + " seconds");

            startTime = System.nanoTime();

            for (int i = 0; i < 500; i++) {
                indexSearcher.search(DoublePoint.newRangeQuery("totalAmount", 5.0, 15.0), 2000);
            }

            elapsed = (System.nanoTime() - startTime) / 1_000_000_000.0;

            System.out.println("Total time taken for double range query is " + elapsed + " seconds");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}