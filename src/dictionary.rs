use std::fmt::Debug;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::MAIN_SEPARATOR;
use std::sync::{Mutex};
use egzreader::EgzReader;
use memory_stats::memory_stats;
use regex::Regex;
use zstd_map::map::ZstdMap;

pub struct Dictionary {
    short_name: String,
    name: String,
    map: Mutex<ZstdMap<'static, String, String>>,
}

impl Dictionary {
    fn new_empty(name: String, long_name: String) -> Self {
        Self {
            short_name: name,
            name: long_name,
            map: Mutex::new(ZstdMap::new(11, 512))
        }
    }
    pub(crate) fn get_word_meaning(&self, word: &str) -> Option<String> {
        self.map.try_lock()
            .expect("Lock prepare")
            .get(word.to_string())
    }
    pub(crate) fn get_both_names(&self) -> String {
        format!("{} \"{}\"", self.name(), self.long_name())
    }
    pub(crate) fn get_word_matches(&self, word: &str, strategy: MatchStrategy) -> Option<Vec<String>> {
        match strategy {
            MatchStrategy::EXACT => Some(vec![self.get_word_meaning(word)?]),
            MatchStrategy::PREFIX => {
                let res = (1..word.len())
                    .rev()
                    .filter_map(|x| self.get_word_meaning(&word[0..x]))
                    .take(1)
                    .last()?;
                Some(vec![res])
            }
        }
    }
    pub(crate) fn compress_dictionary(&mut self) {
        self.map.try_lock()
            .expect("Lock prepare")
            .compress();
    }
    fn push_word(&self, word: String, text: String) {
        self.map.try_lock()
            .expect("Lock prepare")
            .insert(word, text);
    }
    fn push_words(&self, words_texts: Vec<(String, String)>) {
        self.map.try_lock()
            .expect("Lock prepare")
            .extend(words_texts);
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

        let mut defs_ready2push: Vec<(String, String)> = vec![];

        //Swap buffers
        let mut defs2send = vec![];
        let mut txt2push = "".to_string();

        let mut cnt = 0;
        while let Some(Ok(line)) = lines.next() {
            let mut prev_end = 0;
            for m in re.find_iter(&line) {
                last_text = format!("{}{}", &last_text, &line[prev_end..m.start()]);
                if let Some(word) = last_word.take() {
                    (txt2push, last_text) = (last_text, "".to_string());
                    defs_ready2push.push((word, txt2push));
                    cnt += 1;
                }
                if defs_ready2push.len()>1280 {
                    (defs_ready2push, defs2send) = (vec![], defs_ready2push);
                    self.push_words(defs2send);
                }
                prev_end = m.end();
                last_word = Some(re.replace(&line[m.start()..m.end()], "${word}").to_string());
            }
            last_text = format!("{}\n{}", &last_text, &line[prev_end..]); //Add remains of line to current text
        }
        self.push_words(defs_ready2push);
        eprintln!("Inserted {} definitions", cnt);
        if let Some(usage) = memory_stats() {
            println!("Current physical memory usage: {}", usage.physical_mem);
            println!("Current virtual memory usage: {}", usage.virtual_mem);
        } else {
            println!("Couldn't get the current memory usage :(");
        }
        self.compress_dictionary();
        if let Some(usage) = memory_stats() {
            println!("Current physical memory usage: {}", usage.physical_mem);
            println!("Current virtual memory usage: {}", usage.virtual_mem);
        } else {
            println!("Couldn't get the current memory usage :(");
        }
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

use crate::config::DatabaseConfig;
use crate::MatchStrategy;
