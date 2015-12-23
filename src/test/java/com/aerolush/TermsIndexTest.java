package com.aerolush;

import com.aerolush.lucene.store.AeroDirectory;
import com.aerolush.test.Aerospike;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spikeify.Spikeify;
import com.spikeify.annotations.Generation;
import com.spikeify.annotations.UserKey;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class TermsIndexTest {

	private final Aerospike spike;

	public TermsIndexTest() {

		spike = new Aerospike();
	}

	private Spikeify getSfy() {

		return spike.getSfy();
	}

	public static class Media {
		@UserKey
		public String id;

		@Generation
		public Integer generation;

		public List<String> gameIds;

		public String channelId;

		public Media() {
		}

		public Media(String id, List<String> gameIds, String channelId) {
			this.id = id;
			this.gameIds = gameIds;
			this.channelId = channelId;
		}
	}

	@Before
	public void prepareIndex() throws IOException {
		getSfy().truncateNamespace("test");
		List<String> gameIds = Arrays.asList("game1", "game2", "game3", "game4", "GAME1");
		List<String> channelIds = Arrays.asList("ch1", "ch2", "ch3", "ch4", "ch5");
		Random randomizer = new Random();

		List<Media> list = new ArrayList<>();
		for (int i = 0; i < 100000; i++) {
			List<String> games = new ArrayList<>();
			while (games.size() != 2) {
				String game = gameIds.get(randomizer.nextInt(gameIds.size()));
				if (!games.contains(game)) {
					games.add(game);
				}
			}
			list.add(new Media(i + "", games, channelIds.get(randomizer.nextInt(channelIds.size()))));
		}

		// create index
		StandardAnalyzer analyzer = new StandardAnalyzer();
		Directory index = new AeroDirectory(getSfy());
		IndexWriterConfig config = new IndexWriterConfig(analyzer);
		IndexWriter w = new IndexWriter(index, config);

		long startTime = System.currentTimeMillis();

		int count = 0;
		for (Media media : list) {
			if (count % 1000 == 0) {
				System.out.println("Indexing: " + count);
			}

			addDoc(w, media);

			count++;
		}
		w.close();
		index.close();


		long timeSpent = System.currentTimeMillis() - startTime;
		System.out.println("Time spent: " + timeSpent);
	}

	/**
	 * storing two things here ... the whole line ... and the line number
	 */
	private static void addDoc(IndexWriter w, Media media) throws IOException {
		Document doc = new Document();
		doc.add(new StringField("channelId", media.channelId, Field.Store.YES));
		for (String gameId : media.gameIds) {
			doc.add(new StringField("gameIds", gameId, Field.Store.YES));
		}
		w.addDocument(doc);
	}

	@Test
	public void testSearch() throws IOException {

		find(null, "ch1");
		find("game1", null);
		find("game1", "ch1");
	}

	private void find(String gameId, String channelId) throws IOException {
		System.out.println("Game ID: " + gameId + ", Channel ID: " + channelId);
		BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();
		if (gameId != null) {
			PhraseQuery.Builder builder = new PhraseQuery.Builder();
			builder = builder.add(new Term("gameIds", gameId));
			booleanQueryBuilder = booleanQueryBuilder.add(builder.build(), BooleanClause.Occur.MUST);
		}
		if (channelId != null) {
			PhraseQuery.Builder builder = new PhraseQuery.Builder();
			builder = builder.add(new Term("channelId", channelId));
			booleanQueryBuilder = booleanQueryBuilder.add(builder.build(), BooleanClause.Occur.MUST);
		}

		int hitsPerPage = 1000;

		Directory index = new AeroDirectory(getSfy());
		IndexReader reader = DirectoryReader.open(index);
		IndexSearcher searcher = new IndexSearcher(reader);

		TopScoreDocCollector collector = TopScoreDocCollector.create(hitsPerPage);
		Long time = System.currentTimeMillis();
		searcher.search(booleanQueryBuilder.build(), collector);
		TopDocs data = collector.topDocs();
		ScoreDoc[] hits = data.scoreDocs;

		// 4. display results
		System.out.println("*************************");
		System.out.println("FOUND: " + hits.length);
		System.out.println("TOTAL: " + data.totalHits);
		System.out.println("TIME in ms: " + (System.currentTimeMillis() - time));
		System.out.println("*************************");

		ObjectMapper objectMapper = new ObjectMapper();

		for (int i = 0; i < hits.length; ++i) {

			int docId = hits[i].doc;
			Document d = searcher.doc(docId);
			// System.out.println("DOC ID: " + docId + ", gameIds: " + objectMapper.writeValueAsString(d.getValues("gameIds")) + ", channel ID: " + objectMapper.writeValueAsString(d.getValues("channelId")));
		}
		// System.out.println("*************************");

		// reader can only be closed when there
		// is no need to access the documents any more.
		reader.close();
	}

}
