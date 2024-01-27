use std::fmt::Debug;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::sync::{Mutex};
use egzreader::EgzReader;
use memory_stats::memory_stats;
use regex::Regex;
use crate::config::DatabaseConfig;
use crate::MatchStrategy;
use sqlite_zstd::rusqlite::Connection;
use sqlite_zstd::rusqlite::types::FromSql;


pub struct Dictionary {
    short_name: String,
    name: String,
    conn: Mutex<Connection>,
}

impl Dictionary {
    fn new_empty(name: String, long_name: String) -> Self {
        let mut conn: Connection = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
              PRAGMA synchronous = 0;
              PRAGMA cache_size = 10000;
              PRAGMA busy_timeout=2000;
              PRAGMA locking_mode = EXCLUSIVE;
              PRAGMA auto_vacuum = full;
              pragma auto_vacuum=full;
              pragma journal_mode = WAL;
              PRAGMA temp_store = MEMORY;",
        ).expect("PRAGMA failed");
        sqlite_zstd::load(&conn).unwrap();
        Self {
            short_name: name,
            name: long_name,
            conn: Mutex::new(conn)
        }
    }
    pub(crate) fn get_word_meaning(&self, word: &str) -> Option<String> {
        self.conn.try_lock()
            .expect("Lock prepare")
            .prepare(&format!("SELECT meaning FROM {} WHERE WORD = '{word}' LIMIT 1", self.short_name))
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
            MatchStrategy::EXACT => format!("SELECT word FROM {} WHERE word = '{word}' LIMIT 1", self.short_name),
            MatchStrategy::PREFIX => format!("SELECT word FROM {} WHERE word LIKE '{word}%' LIMIT 1", self.short_name),
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
    pub(crate) fn query_stub<T: FromSql + Copy + Clone + Debug>(&self, expression: String) {
        let conn = self.conn.try_lock()
            .expect("Lock prepare");
        let mut expr = conn.prepare(&expression)
            .unwrap_or_else(|e| panic!("Failed to prepare '{expression}' got '{:?}'", e));
        let mut res = expr
            .query([])
            .unwrap_or_else(|e| panic!("Failed to execute '{expression}' got '{:?}'", e));
        #[cfg(debug_assertions)]
        match res.next() {
            Ok(v) => {
                if let Some(r) = v.as_ref() {
                    let r_opt: Option<T> = r.get(0).ok();
                    if let Some(r) = r_opt {
                        eprintln!("Got result '{:?}' for '{expression}'", r)
                    }
                }
            }
            Err(e) => {
                eprintln!("Got error '{:?}' for '{expression}'", e)
            }
        }
    }
    fn execute(&self, expression: String) -> usize {
        self.conn.try_lock().expect("Lock execute").execute(&expression, [])
            .unwrap_or_else(|e| panic!("Failed to execute '{expression}' got '{:?}'", e))
    }
    fn execute_batch(&self, expression: String) {
        self.conn.try_lock().expect("Lock execute_batch").execute_batch(&expression)
            .unwrap_or_else(|e| panic!("Failed to execute '{expression}' got '{:?}'", e))
    }
    fn execute_pragma(&self, name: &str, value: String) {
        self.conn.try_lock().expect("Lock execute_batch").pragma_update(None, name,&value)
            .unwrap_or_else(|e| panic!("Failed to execute pragma {} = {} got '{:?}'", name, value, e))
    }
    fn create_dictionary(&self) {
        self.execute(format!("CREATE TABLE {}(id INTEGER PRIMARY KEY AUTOINCREMENT, word TEXT, meaning TEXT)", &self.short_name));
        self.execute(format!("CREATE INDEX IF NOT EXISTS wordix ON {}(word);", &self.short_name));
        self.query_stub::<bool>(format!(r#"SELECT zstd_enable_transparent('{{"table": "{}", "column": "meaning", "compression_level": 7, "dict_chooser": "''a''"}}');"#, &self.short_name));
        self.query_stub::<bool>(format!(r#"SELECT zstd_enable_transparent('{{"table": "{}", "column": "word", "compression_level": 6, "dict_chooser": "''a''"}}');"#, &self.short_name));
    }
    fn compress_dictionary(&self) {
        if let Some(usage) = memory_stats() {
            println!("Current physical memory usage: {}", usage.physical_mem);
            println!("Current virtual memory usage: {}", usage.virtual_mem);
        } else {
            println!("Couldn't get the current memory usage :(");
        }
        self.execute_pragma("auto_vacuum", "full".to_string());
        self.execute_pragma("journal_mode", "WAL".to_string());
        self.query_stub::<bool>("SELECT zstd_incremental_maintenance(null, 1.0);".to_string());
        self.execute_pragma("VACUUM", "".to_string());
        if let Some(usage) = memory_stats() {
            println!("Current physical memory usage: {}", usage.physical_mem);
            println!("Current virtual memory usage: {}", usage.virtual_mem);
        } else {
            println!("Couldn't get the current memory usage :(");
        }
    }
    fn push_word(&self, word: String, text: String) {
        self.execute(format!(r#"INSERT INTO {table_name} VALUES("{word}", "{text}")"#, table_name=&self.short_name));
    }
    fn push_words(&self, words_texts: Vec<(String, String)>) {
        let stmts: Vec<String> = words_texts.into_iter().map(|(word, text)|{
            format!(r#"INSERT INTO {table_name}(word, meaning) VALUES("{word}", "{text}");"#, table_name=&self.short_name)
        }).collect();
        let stmts_raw = stmts.join("\n");
        let full_batch_stmt = format!(r#"BEGIN;
        {stmts_raw}
        COMMIT;"#);
        self.execute_batch(full_batch_stmt);
    }
    pub fn name(&self) -> &str {
        &self.short_name
    }
    pub fn long_name(&self) -> &str {
        &self.name
    }
}

pub(crate) trait DictLoader {
    fn from_dict_file(dbc: &DatabaseConfig) -> Self;
    fn load_from_reader<T: Read>(&mut self, reader: BufReader<T>);
}

fn load_dict_uncompressed(name: String, long_name: String, filepath: &str) -> Dictionary {
    let dict_file = File::open(filepath).unwrap();
    let reader = BufReader::new(dict_file);

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
    fn from_dict_file(dbc: &DatabaseConfig) -> Self {
        let is_compressed = dbc.path().ends_with("z");
        match is_compressed {
            true => load_dict_compressed(dbc.short_name(), dbc.name(), dbc.path()),
            false => load_dict_uncompressed(dbc.short_name(), dbc.name(), dbc.path())
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
        eprintln!("Inserted {} definitions", cnt);
        self.compress_dictionary();
    }
}

#[test]
fn test_read_compressed_egz() {
    let path = "/media/Data/Data/Dicts/stardict-rus_eng_full-2.4.2/rus_eng_full.dict.gz";
    let egzr = EgzReader::new(std::fs::File::open(path).unwrap());
    let mut dictionary = Dictionary::new_empty("rus_eng_full".to_string(), "".to_uppercase());
    dictionary.load_from_reader(BufReader::new(egzr));
}