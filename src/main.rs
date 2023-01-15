//Inspired by https://gist.github.com/gkbrk/bea6dee7c0478395b718
//and tokio chat.rs example
//extern crate sqlite_zstd;

mod dictionary;

use std::collections::HashMap;
use std::sync::Arc;
use tokio::{net::{
    TcpListener,
    TcpStream
}, io::BufReader, fs::File, spawn};
use tokio::io::{AsyncBufReadExt};
use tokio_util::codec::{Framed, LinesCodec, LinesCodecError};
use futures::SinkExt;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use rayon::{iter::IntoParallelIterator, iter::ParallelIterator, ThreadPoolBuilder};
use tokio_rayon::AsyncThreadPool;
use strum_macros::EnumString;
use crate::dictionary::{Dictionary, DictLoader};

#[derive(EnumString)]
enum Command {
    DEFINE,
    MATCH,
    SHOW,
    CLIENT,
    STATUS,
    OPTION,
    AUTH,
    SASLAUTH,
    SASLRESP,
    QUIT
}

#[derive(EnumString)]
enum ItemToShow {
    #[strum(serialize = "DATABASES", serialize = "DB")]
    DATABASES,
    #[strum(serialize = "STRATEGIES", serialize = "STRAT")]
    STRATEGIES,
    INFO,
    SERVER,
}
#[derive(Clone, Copy, EnumString)]
enum MatchStrategy {
    EXACT,
    PREFIX,
}
/*
#[derive(EnumString)]
enum Response {
    #[strum(serialize = "550 Invalid database, use \"SHOW DB\" for list of databases")]
    InvalidDB550,
    #[strum(serialize = "551 Invalid strategy, use \"SHOW STRAT\" for a list of strategies")]
    InvalidStrat551,
    #[strum(serialize = "552 No match")]
    NoMatch552,
    #[strum(serialize = "152 N matches found")]
    NMatchesFound152,
    #[strum(serialize = "250 ok ")]
    Ok250,
}
*/
async fn get_definitions(word: String) -> Vec<String>{
    let mut dict = File::open("dict.txt").await.unwrap();
    let mut reader = BufReader::new(dict);
    let mut matches: Vec<String> = Vec::new();
    let mut lines = reader.lines();
    while let Ok(Some(line)) = lines.next_line().await {
        eprintln!("Found in dictionary: {}", &line);
        let parts: Vec<&str> = line.trim().splitn(2, ": ").collect();
        let dictword = parts[0];
        let definition = parts[1];
        if dictword.to_lowercase() == word.to_lowercase(){
            matches.push(definition.to_string());
        } else {
            eprintln!("{} do not match {}", &dictword, &word);
        }
    }
    return matches;
}

async fn handle_client(mut stream: TcpStream, dicts: Dictionaries) -> Result<(), LinesCodecError> {
    let mut lines = Framed::new(stream, LinesCodec::new());
    lines.send("220 dict 0.1.0").await?;
    loop {
        if let Some(external_input) = lines.next().await {
            match external_input {
                Ok(line) => {
                    eprintln!("Client says: '{}'", &line);
                    let pieces: Vec<&str> = line.trim().split(' ').collect();

                    let command_string = pieces[0];
                    let command_result: Result<Command, _> = pieces[0].to_uppercase().parse();

                    match command_result {
                        Ok(command) => match command {
                            Command::DEFINE => {
                                let word = pieces[2].replace('\"', "");
                                let definitions = dicts.lookup_word(word.clone(), None);
                                lines.send("250 ok").await?;
                                lines.send(format!("150 {} definitions received", definitions.len())).await?;
                                for definition in definitions.iter() {
                                    lines.send(format!("151 \"{}\" \"default dictionary\"\n{}\n.", word, definition)).await?;
                                }
                                lines.send("250 ok").await?;
                            },
                            Command::QUIT => {
                                lines.send("221 bye").await?;
                                break;
                            },
                            _ => {
                                lines.send(format!("502 '{}' unimplemented", command_string)).await?;
                                break;
                            }
                        },
                        Err(_) => {
                            lines.send(format!("500 Unknown command '{}'", command_string)).await?;
                            break;
                        }
                    }
                }
                Err(err) => {}
            }
        } else{
            println!("Client disconnected");
            break;
        }
    }
    Ok(())
}

#[derive(Clone)]
struct Dictionaries {
    dicts: Arc<HashMap<String, Dictionary>>
}

impl Dictionaries {
    fn match_word(&self, word: String, strategy: MatchStrategy, dict_names: Option<Vec<String>>) -> Vec<String> {
        todo!()
    }
    fn lookup_word(&self, word: String, dict_names: Option<Vec<String>>) -> Vec<String> {
        let dicts2lookup: Vec<String> = if let Some(dn) = dict_names {
            self.dicts
                .keys()
                .filter(|k| dn.contains(k))
                .cloned()
                .collect()
        } else {
            self.dicts
                .keys()
                .cloned()
                .collect()
        };
        dicts2lookup.into_par_iter()
            .filter_map(|dict_name|  self.dicts.get(&dict_name)
                .unwrap()
                .get_word_meaning(&word))
            .collect()
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let listener = TcpListener::bind("0.0.0.0:2628").await.unwrap();
    let path_er = "/media/Data/Data/Dicts/stardict-eng_rus_full-2.4.2/eng_rus_full.dict".to_string();
    let path_re = "/media/Data/Data/Dicts/stardict-rus_eng_full-2.4.2/rus_eng_full.dict.gz".to_string();
    let dicts_fnames = vec![path_er, path_re];


    let dictionaries: HashMap<String, Dictionary> = futures::stream::iter(dicts_fnames)
        .map(|fnm| async move {
            let d = Dictionary::from_dict_file(fnm).await;
            let name = d.name().to_string();
            (name, d)
        })
        .buffer_unordered(4)
        .collect()
        .await;

    let dictionaries = Dictionaries {dicts: Arc::new(dictionaries)};

    loop {
        match listener.accept().await {
            Ok((stream, socket)) => {
                let cloned_dicts = dictionaries.clone();
                tokio::spawn(async move {
                    eprintln!("New connection at '{}:{}'", &socket.ip(), &socket.port());
                    handle_client(stream, cloned_dicts).await.unwrap();
                });
            }
            Err(e) => {
                eprintln!("Exited with error: '{:?}'", e)
            }
        }
    }
}
