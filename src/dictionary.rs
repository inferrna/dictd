use std::fmt::format;
use std::io::{Read};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::fs::File;
use tokio::io;
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader, ReadBuf};
use async_trait::async_trait;
use egzreader::EgzReader;
use regex::Regex;
use sqlite_zstd::rusqlite;
use sqlite_zstd::LogLevel;

struct Dictionary {
    name: String,
    conn: rusqlite::Connection,
}

impl Dictionary {
    fn new(name: String) -> Self {
        let conn =  rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "PRAGMA journal_mode = OFF;
              PRAGMA synchronous = 0;
              PRAGMA cache_size = 1000000;
              PRAGMA locking_mode = EXCLUSIVE;
              PRAGMA temp_store = MEMORY;",
        )
            .expect("PRAGMA failed");
        sqlite_zstd::load(&conn).unwrap();
        Self {
            name,
            conn
        }
    }
    fn create_dictionary(&mut self) {
        self.conn.execute(&format!("CREATE TABLE {}(word TEXT, meaning TEXT)", &self.name), []).unwrap();
        self.conn
            .execute(
                &format!("CREATE INDEX IF NOT EXISTS wordix ON {}(word);", &self.name),
                [],
            )
            .unwrap();
    }
    fn push_word(&self, word: String, text: String) {
        //eprintln!("Pushing word: '{}'", &word);
        //eprintln!("With text: '{}'", &text);
        self.conn.execute(&format!(r#"INSERT INTO {table_name} VALUES("{word}", "{text}")"#, table_name=&self.name), []).unwrap();
    }
    fn push_words(&self, words_texts: Vec<(String, String)>) {
        let stmts: Vec<String> = words_texts.into_iter().map(|(word, text)|{
            format!(r#"INSERT INTO {table_name} VALUES("{word}", "{text}");"#, table_name=&self.name)
        }).collect();
        let stmts_raw = stmts.join("\n");
        let full_batch_stmt = &format!(r#"BEGIN;
        {stmts_raw}
        COMMIT;"#);
        //eprintln!("{}", full_batch_stmt);
        self.conn.execute_batch(full_batch_stmt).unwrap();
    }
}

#[async_trait(?Send)]
trait DictLoader {
    async fn load_from_reader<T: AsyncRead + Unpin>(&mut self, reader: io::BufReader<T>);
}

#[async_trait(?Send)]
impl DictLoader for Dictionary {
    async fn load_from_reader<T: AsyncRead + Unpin>(&mut self, reader: io::BufReader<T>) {
        let re = Regex::new(r"<k>(&.+?;)?(?P<word>.+?)</k>").unwrap();
        let mut lines = reader.lines();
        let mut last_text = "".to_string();
        let mut last_word: Option<String> = None;

        let mut defs_reday2push: Vec<(String, String)> = vec![];

        //Swap buffers
        let mut defs2send = vec![];
        let mut txt2push = "".to_string();

        self.create_dictionary();
        let mut cnt = 0;
        while let Ok(Some(line)) = lines.next_line().await {
            eprintln!("Processing line {}", &line);
            let mut prev_end = 0;
            for m in re.find_iter(&line) {
                last_text = format!("{}{}", &last_text, &line[prev_end..m.start()]);
                if let Some(word) = last_word.take() {
                    (txt2push, last_text) = (last_text, "".to_string());
                    defs_reday2push.push((word, txt2push));
                    cnt += 1;
                }
                if defs_reday2push.len()>128 {
                    (defs_reday2push, defs2send) = (vec![], defs_reday2push);
                    self.push_words(defs2send);
                }
                prev_end = m.end();
                last_word = Some(re.replace(&line[m.start()..m.end()], "${word}").to_string());
            }
            last_text = format!("{}{}", &last_text, &line[prev_end..]); //Add remains of line to current text
        }
        self.push_words(defs_reday2push);
        eprintln!("Inserted {} definiions", cnt);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_read_dict() {
    let path = "/media/Data/Data/Dicts/stardict-eng_rus_full-2.4.2/eng_rus_full.dict";
    let mut dict_file = File::open(path).await.unwrap();
    let mut reader = BufReader::new(dict_file);

    let mut dictionary = Dictionary::new("eng_rus_full".to_string());
    dictionary.load_from_reader(reader).await;
}

struct AsyncFileReader {
    egzr: EgzReader<std::fs::File>
}

impl AsyncRead for AsyncFileReader {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        let cap = buf.capacity();
        let mut bytes_done= cap;
        let mut byteskeeper = vec![0u8; cap];
        while bytes_done==cap && buf.remaining()>0 {
            //bytes_done = self.egzr.read(buf.initialize_unfilled()).unwrap();
            bytes_done = self.egzr.read(&mut byteskeeper).unwrap();
            byteskeeper[bytes_done..].fill(0);
            buf.put_slice(&byteskeeper[0..bytes_done]);
            eprintln!("Read {bytes_done} bytes");
        }
        Poll::Ready(Ok(()))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_read_compressed() {
    let path = "/media/Data/Data/Dicts/stardict-rus_eng_full-2.4.2/rus_eng_full.dict.gz";
    let egzr = EgzReader::new(std::fs::File::open(path).unwrap());
    let afr = AsyncFileReader { egzr };
    let mut dictionary = Dictionary::new("rus_eng_full".to_string());
    dictionary.load_from_reader(BufReader::new(afr)).await;
}