use std::fmt::format;
use tokio::fs::File;
use tokio::io;
use tokio::io::{AsyncBufReadExt, BufReader};
use async_trait::async_trait;
use regex::Regex;
use sqlite_zstd::rusqlite;
use sqlite_zstd::LogLevel;

struct Dictionary {
    conn: rusqlite::Connection,
}

impl Dictionary {
    fn new() -> Self {
        let conn =  rusqlite::Connection::open_in_memory().unwrap();
        sqlite_zstd::load(&conn).unwrap();
        Self {
            conn
        }
    }
    fn create_dictionary(&mut self, name: &str) {
        self.conn.execute(&format!("CREATE TABLE {name}(word TEXT, meaning TEXT)"), []).unwrap();
        self.conn
            .execute(
                &format!("CREATE INDEX IF NOT EXISTS wordix ON {name}(word);"),
                [],
            )
            .unwrap();
    }
    fn push_word(&self, table_name: &str, word: String, text: String) {
        eprintln!("Pushing word: '{}'", &word);
        eprintln!("With text: '{}'", &text);
        self.conn.execute(&format!(r#"INSERT INTO {table_name} VALUES("{word}", "{text}")"#), []).unwrap();
    }
}

#[async_trait(?Send)]
trait DictLoader {
    async fn load_from_reader(&mut self, name: &str, reader: io::BufReader<File>);
}

#[async_trait(?Send)]
impl DictLoader for Dictionary {
    async fn load_from_reader(&mut self, name: &str, reader: io::BufReader<File>) {
        let re = Regex::new(r"<k>(&.+?;)?(?P<word>.+?)</k>").unwrap();
        let mut lines = reader.lines();
        let mut last_text = "".to_string();
        let mut last_word: Option<String> = None;
        self.create_dictionary(name);
        while let Ok(Some(line)) = lines.next_line().await {
            let mut prev_end = 0;
            for m in re.find_iter(&line) {
                last_text = format!("{}{}", &last_text, &line[prev_end..m.start()]);
                if let Some(word) = last_word.take() {
                    self.push_word(name, word, last_text.clone());
                    last_text = "".to_string();
                }
                prev_end = m.end();
                last_word = Some(re.replace(&line[m.start()..m.end()], "${word}").to_string());
            }
            last_text = format!("{}{}", &last_text, &line[prev_end..]); //Add remains of line to current text
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_read_dict() {
    let path = "/media/Data/Data/Dicts/stardict-eng_rus_full-2.4.2/eng_rus_full.dict";
    let mut dict_file = File::open(path).await.unwrap();
    let mut reader = BufReader::new(dict_file);

    let mut dictionary = Dictionary::new();
    dictionary.load_from_reader("eng_rus_full", reader).await;
}