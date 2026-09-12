import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import java.io.File;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Generates the fictitious sample data set example/data/books.avro: a catalog of made-up books from
 * made-up publishers. Deterministic (fixed seed), so re-running it reproduces the same file.
 *
 * Run with the importer jar on the classpath (Avro is bundled in it; Avro needs slf4j-api at runtime):
 *
 *   java -cp "target/solr-index-importer-0.1-full.jar:$HOME/.m2/repository/org/slf4j/slf4j-api/2.0.17/slf4j-api-2.0.17.jar" example/SampleData.java
 *
 * Optional arguments: the output file (default example/data/books.avro) and the record count (default 1000).
 * For example "example/SampleData.java build/data/books.avro 500000" produces a larger file for throughput measurements.
 */
public class SampleData {

    static final int COUNT = 1000;

    static final String[] TOPICS = {"検索エンジン", "分散システム", "データベース", "機械学習", "ストリーム処理", "コンパイラ",
            "ネットワーク", "暗号技術", "関数型プログラミング", "オペレーティングシステム", "統計モデリング", "グラフ理論"};
    static final String[] PREFIXES = {"はじめての", "実践", "詳解", "入門", "図解", "徹底理解", "現場で使う", "ゼロから学ぶ"};
    static final String[] SUFFIXES = {"", " 第2版", " 実装編", " 設計編", " 演習問題集"};
    static final String[] CATEGORIES = {"programming", "infrastructure", "data", "theory", "security"};
    static final String[] PUBLISHERS = {"架空技術出版", "サンプル書房", "ダミー社", "例示出版", "仮想メディア"};
    static final String[] SURNAMES = {"架空", "仮名", "例示", "試験", "見本", "模擬", "仮想", "設例"};
    static final String[] GIVEN_NAMES = {"太郎", "花子", "一郎", "美咲", "健", "さくら", "翔", "葵"};
    static final String[] TAGS = {"初心者向け", "中級者向け", "上級者向け", "演習付き", "サンプルコード付き", "理論", "実践"};

    static final Schema SCHEMA = SchemaBuilder.record("Book").namespace("example").fields()
            .requiredString("id")
            .requiredString("title")
            .requiredString("author")
            .requiredString("publisher")
            .requiredString("category")
            .requiredLong("price")
            .name("published").type(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG))).noDefault()
            .name("tags").type().array().items().stringType().noDefault()
            .optionalString("description")
            .endRecord();

    public static void main(final String[] args) throws Exception {
        final File out = new File(args.length > 0 ? args[0] : "example/data/books.avro");
        final int count = args.length > 1 ? Integer.parseInt(args[1]) : COUNT;
        out.getParentFile().mkdirs();
        final Random random = new Random(20260912L);
        try (final DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>(SCHEMA))) {
            writer.setCodec(CodecFactory.snappyCodec());
            writer.create(SCHEMA, out);
            for (int i = 1; i <= count; i++) {
                final String topic = pick(random, TOPICS);
                final GenericRecord record = new GenericData.Record(SCHEMA);
                record.put("id", String.format("B%06d", i));
                record.put("title", pick(random, PREFIXES) + topic + pick(random, SUFFIXES));
                record.put("author", pick(random, SURNAMES) + " " + pick(random, GIVEN_NAMES));
                record.put("publisher", pick(random, PUBLISHERS));
                record.put("category", pick(random, CATEGORIES));
                record.put("price", 1800L + 200L * random.nextInt(20));
                record.put("published", LocalDate.of(2015 + random.nextInt(11), 1 + random.nextInt(12), 1 + random.nextInt(28))
                        .atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli());
                record.put("tags", tags(random));
                record.put("description", random.nextInt(10) == 0 ? null
                        : topic + "の基礎から応用までを架空の事例で解説するサンプル書籍です。");
                writer.append(record);
            }
        }
        System.out.println("wrote " + count + " records to " + out);
    }

    static String pick(final Random random, final String[] values) {
        return values[random.nextInt(values.length)];
    }

    static List<String> tags(final Random random) {
        final List<String> tags = new ArrayList<>();
        final int count = 1 + random.nextInt(3);
        while (tags.size() < count) {
            final String tag = pick(random, TAGS);
            if (!tags.contains(tag)) {
                tags.add(tag);
            }
        }
        return tags;
    }

}
