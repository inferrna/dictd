//Inspired by https://gist.github.com/gkbrk/bea6dee7c0478395b718
//and tokio chat.rs example
//extern crate sqlite_zstd;

mod dictionary;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::{net::{
    TcpListener,
    TcpStream
}, io::BufReader, fs::File};
use tokio::io::{AsyncBufReadExt};
use tokio_util::codec::{Framed, LinesCodec, LinesCodecError};
use futures::SinkExt;
use futures_util::StreamExt;
use newline_converter::unix2dos;
use rayon::{iter::IntoParallelIterator, iter::ParallelIterator};
use rayon::iter::IntoParallelRefIterator;
use strum::{EnumMessage, IntoEnumIterator, ParseError};
use strum_macros::{EnumString, EnumIter, EnumMessage};
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
    CLIENT
}
#[derive(Debug, Clone, Copy, EnumString, EnumIter, EnumMessage)]
enum MatchStrategy {
    #[strum(message = "Match headwords exactly")]
    EXACT,
    #[strum(message = "Match prefixes")]
    PREFIX,
}

trait Unquote {
    fn unquote(&self) -> String;
}

impl Unquote for String {
    fn unquote(&self) -> String {
        self.replace("\"", "").replace("\'", "")
    }
}
impl Unquote for &str {
    fn unquote(&self) -> String {
        self.replace("\"", "").replace("\'", "")
    }
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
const HELLO_DICT_220: &str = "220 dict 0.1.0\r";
const INVALID_DB_550: &str = "550 invalid database, use SHOW DB for list\r";
const NO_MATCH_552: &str = "552 No match\r";
const BYE_DICT_250: &str = "250 ok\r";
const ENDING_DOT: &str = ".\r";
const UNKNOWN_STRAT_551: &str = "551 invalid strategy, use SHOW STRAT for a list\r";

async fn handle_client(mut stream: TcpStream, dicts: Dictionaries) -> Result<(), LinesCodecError> {
    //To debug use
    //socat -v -dddd TCP-LISTEN:2628 TCP:dict.org:2628
    let mut lines = Framed::new(stream, LinesCodec::new());
    lines.send(HELLO_DICT_220).await?;
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
                                let word = pieces[2].unquote();
                                let dict_name = pieces[1].unquote();
                                let maybe_definitions = dicts.lookup_word(word.clone(), dict_name);
                                match maybe_definitions {
                                    Err(e) => {
                                        match e {
                                            WordSearchError::DbNotFoundErr => {
                                                lines.send(INVALID_DB_550).await?;
                                                break;
                                            },
                                            WordSearchError::WordNotFoundErr => {
                                                lines.send(NO_MATCH_552).await?;
                                            },
                                        }
                                    }
                                    Ok(definitions) => {
                                        //lines.send("250 ok").await?;
                                        lines.send(format!("150 {} definitions retrieved\r", definitions.len())).await?;
                                        for (dictionary, definition) in definitions.iter() {
                                            lines.send(format!("151 \"{word}\" {dictionary}\r")).await?;
                                            let definition = unix2dos(definition);
                                            lines.send(format!("{definition}\r")).await?;
                                            lines.send(ENDING_DOT).await?;
                                        }
                                        lines.send(BYE_DICT_250).await?;
                                    }
                                }
                            },
                            Command::MATCH => {
                                let word = pieces[3].unquote();
                                let strat_str = pieces[2].unquote();
                                let maybe_strat = strat_str.parse::<MatchStrategy>();
                                let strategy = match maybe_strat {
                                    Ok(strat) => strat,
                                    Err(_) => {
                                        lines.send(UNKNOWN_STRAT_551).await?;
                                        break;
                                    }
                                };

                                let dict_name = pieces[1].unquote();
                                let maybe_matches = dicts.match_word(word.clone(), dict_name, strategy);
                                match maybe_matches {
                                    Err(e) => {
                                        match e {
                                            WordSearchError::DbNotFoundErr => {
                                                lines.send(INVALID_DB_550).await?;
                                                break;
                                            },
                                            WordSearchError::WordNotFoundErr => {
                                                lines.send(NO_MATCH_552).await?;
                                            },
                                        }
                                    }
                                    Ok(matches) => {
                                        //lines.send("250 ok").await?;
                                        lines.send(format!("152 {} mathes found\r", matches.len())).await?;
                                        for (dictionary, match_word) in matches.iter() {
                                            lines.send(format!("151 \"{dictionary}\" \"{match_word}\"\n.\r")).await?;
                                        }
                                        lines.send(BYE_DICT_250).await?;
                                    }
                                }
                            }
                            Command::QUIT => {
                                break;
                            },
                            Command::CLIENT => {
                                lines.send(BYE_DICT_250).await?;
                                continue;
                            },
                            Command::SHOW => {
                                let what2show_word = pieces[1].unquote();
                                let what2show_result: Result<ItemToShow, _> = what2show_word.to_uppercase().parse();
                                match what2show_result {
                                    Ok(what2show) => {
                                        match what2show {
                                            ItemToShow::DATABASES => {
                                                eprintln!("Gonna show");
                                                let dblist = dicts.show_databases();
                                                lines.send(format!("110 {} databases present\r", dblist.len())).await?;
                                                for (db_name, db_long_name) in dblist.iter() {
                                                    lines.send(format!("{} \"{}\"\r", db_name, db_long_name)).await?;
                                                }
                                                lines.send("all \"All databases\"\r").await?;
                                                lines.send(ENDING_DOT).await?;
                                                lines.send(BYE_DICT_250).await?;
                                            },
                                            ItemToShow::STRATEGIES => {
                                                lines.send(format!("111 {} strategies present\r", MatchStrategy::iter().len())).await?;
                                                for strat in MatchStrategy::iter() {
                                                    lines.send(format!("{:?} \"{}\"\r", strat, strat.get_message().unwrap_or("No description"))).await?;
                                                }
                                                lines.send(ENDING_DOT).await?;
                                                lines.send(BYE_DICT_250).await?;
                                            },
                                            _ => {
                                                lines.send(format!("502 '{}' unimplemented\r", command_string)).await?;
                                                break;
                                            }
                                        }
                                    },
                                    Err(_) => lines.send("501 syntax error, illegal parameters\r").await?
                                }
                            },
                            _ => {
                                lines.send(format!("502 '{}' unimplemented\r", command_string)).await?;
                                break;
                            }
                        },
                        Err(_) => {
                            let msg = format!("500 Unknown command '{}'\r", command_string);
                            eprintln!("{}", &msg);
                            lines.send(&msg).await?;
                            break;
                        }
                    }
                }
                Err(err) => {
                    eprintln!("Error decoding line: '{:?}'", &err);
                }
            }
        } else{
            println!("Client disconnected");
            break;
        }
    }
    lines.send("221 bye").await?;
    Ok(())
}

#[derive(Debug)]
enum WordSearchError {
    DbNotFoundErr,
    WordNotFoundErr,
}

#[derive(Clone)]
struct Dictionaries {
    dicts: Arc<HashMap<String, Dictionary>>
}

impl Dictionaries {
    fn show_databases(&self) -> Vec<(String, String)> {
        self.dicts.values().map(|d|(d.name().to_string(), d.long_name().to_string())).collect()
    }
    fn filter_dicts(&self, dict_name: String) -> Vec<String> {
        if !["*", "all"].contains(&dict_name.as_str()) {
            self.dicts
                .keys()
                .filter(|&k| {
                    eprintln!("Matching '{}' against '{}'", dict_name, k);
                    dict_name.eq(k)
                })
                .take(1)
                .cloned()
                .collect()
        } else {
            self.dicts
                .keys()
                .cloned()
                .collect()
        }
    }
    fn match_word(&self, word: String, dict_name: String, strategy: MatchStrategy) -> Result<Vec<(String, String)>, WordSearchError> {
        let dicts2lookup: Vec<String> = self.filter_dicts(dict_name);
        if dicts2lookup.len()==0 {
            return Err(WordSearchError::DbNotFoundErr)
        }
        let res: Vec<Option<Vec<(String, String)>>> = dicts2lookup.par_iter()
            .map(|dn| {
                self.dicts.get(dn)
                    .unwrap()
                    .get_word_matches(&word, strategy)
                    .map(|a| a
                        .into_iter()
                        .map(|txt| (dn.clone(), txt))
                        .collect()
                    )
            }).collect();
        let res: Vec<(String, String)> = res.into_iter()
            .filter_map(|v| v)
            .flatten()
            .collect();
        if res.len() > 0 {
            Ok(res)
        } else {
            return Err(WordSearchError::WordNotFoundErr)
        }
    }
    fn lookup_word(&self, word: String, dict_name: String) -> Result<Vec<(String, String)>, WordSearchError> {
        eprintln!("Looking for '{}' in '{}'", &word, &dict_name);
        let dicts2lookup: Vec<String> = self.filter_dicts(dict_name);
        if dicts2lookup.len()==0 {
            return Err(WordSearchError::DbNotFoundErr)
        }
        let res: Vec<Option<(String, String)>> = dicts2lookup.into_par_iter()
            .map(|dn| {
                let dct = self.dicts.get(&dn)
                    .unwrap();
                dct.get_word_meaning(&word)
                    .map(|txt| (dct.get_both_names(), txt))
            })
            .collect();
        let res: Vec<(String, String)> = res.into_iter()
            .flatten()
            .collect();
        if res.len() > 0 {
            Ok(res)
        } else {
            return Err(WordSearchError::WordNotFoundErr)
        }
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:2628").await.unwrap();
    let path_er = ("/media/Data/Data/Dicts/stardict-eng_rus_full-2.4.2/eng_rus_full.dict".to_string(), "English - Russian".to_string());
    let path_re = ("/media/Data/Data/Dicts/stardict-rus_eng_full-2.4.2/rus_eng_full.dict.gz".to_string(), "Russian - English".to_string());
    let dicts_fnames = vec![path_er, path_re];

    let now_b4load = Instant::now();

    let dictionaries: HashMap<String, Dictionary> = dicts_fnames.into_par_iter()
        .map(|(fnm, long_nm)| {
            let d = Dictionary::from_dict_file(fnm, long_nm);
            let name = d.name().to_string();
            (name, d)
        })
        .collect();

    eprintln!("Loaded {} dictionaries for {} milliseconds", dictionaries.len(), now_b4load.elapsed().as_millis());

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
