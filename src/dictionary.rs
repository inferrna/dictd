use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::MAIN_SEPARATOR;
use std::sync::{Mutex};
use egzreader::EgzReader;
use regex::Regex;
use sqlite_zstd::rusqlite;

pub struct Dictionary {
    name: String,
    long_name: String,
    conn: Mutex<rusqlite::Connection>,
}

impl Dictionary {
    fn new_empty(name: String, long_name: String) -> Self {
        let conn =  rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "PRAGMA journal_mode = OFF;
              PRAGMA synchronous = 0;
              PRAGMA cache_size = 1000000;
              PRAGMA locking_mode = EXCLUSIVE;
              PRAGMA temp_store = MEMORY;",
        ).expect("PRAGMA failed");
        sqlite_zstd::load(&conn).unwrap();
        Self {
            name,
            long_name,
            conn: Mutex::new(conn)
        }
    }
    pub(crate) fn get_word_meaning(&self, word: &str) -> Option<String> {
        self.conn.try_lock()
            .expect("Lock prepare")
            .prepare(&format!("SELECT meaning FROM {} WHERE WORD = '{word}' LIMIT 1", self.name))
            .ok()?
            .query([])
            .ok()?
            .next().ok().flatten()?
            .get(0).ok()
    }
    pub(crate) fn get_both_names(&self) -> String {
        format!("{} \"{}\"", self.name(), self.long_name())
    }
    pub(crate) fn get_word_matches(&self, word: &str, strategy: MatchStrategy) -> Option<Vec<String>> {
        let conn = self.conn.try_lock()
            .expect("Lock prepare");

        let expression = match strategy {
            MatchStrategy::EXACT => format!("SELECT word FROM {} WHERE word = '{word}' LIMIT 1", self.name),
            MatchStrategy::PREFIX => format!("SELECT word FROM {} WHERE word LIKE '{word}%' LIMIT 1", self.name),
        };

        let mut stmt = conn
            .prepare(&expression)
            .ok()?;
        let mut qres = stmt
            .query([])
            .ok()?;
        let mut res = vec![];
        while let Some(row) = qres.next().ok()? {
            let matched_word: String = row.get(0).ok()?;
            res.push(matched_word);
        }
        Some(res)
    }
    fn execute(&self, expression: String) -> usize {
        self.conn.try_lock().expect("Lock execute").execute(&expression, []).unwrap()
    }
    fn execute_batch(&self, expression: String) {
        self.conn.try_lock().expect("Lock execute_batch").execute_batch(&expression).unwrap()
    }
    fn create_dictionary(&mut self) {
        self.execute(format!("CREATE TABLE {}(word TEXT, meaning TEXT)", &self.name));
        self.execute(format!("CREATE INDEX IF NOT EXISTS wordix ON {}(word);", &self.name));
    }
    fn push_word(&self, word: String, text: String) {
        //eprintln!("Pushing word: '{}'", &word);
        //eprintln!("With text: '{}'", &text);
        self.execute(format!(r#"INSERT INTO {table_name} VALUES("{word}", "{text}")"#, table_name=&self.name));
    }
    fn push_words(&self, words_texts: Vec<(String, String)>) {
        let stmts: Vec<String> = words_texts.into_iter().map(|(word, text)|{
            format!(r#"INSERT INTO {table_name} VALUES("{word}", "{text}");"#, table_name=&self.name)
        }).collect();
        let stmts_raw = stmts.join("\n");
        let full_batch_stmt = format!(r#"BEGIN;
        {stmts_raw}
        COMMIT;"#);
        //eprintln!("{}", full_batch_stmt);
        self.execute_batch(full_batch_stmt);
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn long_name(&self) -> &str {
        &self.long_name
    }
}

pub trait DictLoader {
    fn from_dict_file(path: String, long_name: String) -> Self;
    fn load_from_reader<T: Read>(&mut self, reader: BufReader<T>);
}

fn load_dict_uncompressed(name: String, long_name: String, filepath: &str) -> Dictionary {
    let mut dict_file = File::open(filepath).unwrap();
    let mut reader = BufReader::new(dict_file);

    let mut dictionary = Dictionary::new_empty(name, long_name);
    dictionary.load_from_reader(reader);
    dictionary
}
fn load_dict_compressed(name: String, long_name: String, filepath: &str) -> Dictionary {
    let egzr = EgzReader::new(File::open(filepath).unwrap());
    let mut dictionary = Dictionary::new_empty(name, long_name);
    dictionary.load_from_reader(BufReader::new(egzr));
    dictionary
}

impl DictLoader for Dictionary {
    fn from_dict_file(path: String, long_name: String) -> Self {
        let name = path.split(MAIN_SEPARATOR)
            .last()
            .unwrap()
            .split(".")
            .next()
            .unwrap()
            .to_string();
        let is_compressed = path.ends_with("z");
        match is_compressed {
            true => load_dict_compressed(name, long_name, &path),
            false => load_dict_uncompressed(name, long_name, &path)
        }
    }

    fn load_from_reader<T: Read>(&mut self, reader: BufReader<T>) {
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
        while let Some(Ok(line)) = lines.next() {
            //eprintln!("Processing line {}", &line);
            let mut prev_end = 0;
            for m in re.find_iter(&line) {
                last_text = format!("{}{}", &last_text, &line[prev_end..m.start()]);
                if let Some(word) = last_word.take() {
                    (txt2push, last_text) = (last_text, "".to_string());
                    defs_reday2push.push((word, txt2push));
                    cnt += 1;
                }
                if defs_reday2push.len()>1280 {
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

#[test]
fn test_read_dict() {
    let path = "/media/Data/Data/Dicts/stardict-eng_rus_full-2.4.2/eng_rus_full.dict".to_string();
    Dictionary::from_dict_file(path);
}
#[test]
fn test_read_dict_compressed() {
    let path = "/media/Data/Data/Dicts/stardict-eng_rus_full-2.4.2/eng_rus_full.dict.gz".to_string();
    Dictionary::from_dict_file(path);
}

#[test]
fn test_read_compressed_egz() {
    let path = "/media/Data/Data/Dicts/stardict-rus_eng_full-2.4.2/rus_eng_full.dict.gz";
    let egzr = EgzReader::new(std::fs::File::open(path).unwrap());
    let mut dictionary = Dictionary::new_empty("rus_eng_full".to_string(), "".to_uppercase());
    dictionary.load_from_reader(BufReader::new(egzr));
}

use sqlite_zstd::rusqlite::{Rows, Statement};
use crate::MatchStrategy;
