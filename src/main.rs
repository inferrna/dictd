//Inspired by https://gist.github.com/gkbrk/bea6dee7c0478395b718
//and tokio chat.rs example


mod dictionary;

use std::thread;
use std::net::SocketAddr;
use tokio::{
    net::{
        TcpListener,
        TcpStream
    },
    io::BufReader,
    fs::File
};
use tokio::io::{AsyncBufReadExt};
use tokio_util::codec::{Framed, LinesCodec, LinesCodecError};
use tokio_stream::StreamExt;
use futures::SinkExt;
use std::str::FromStr;
use strum_macros::EnumString;

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

async fn handle_client(mut stream: TcpStream) -> Result<(), LinesCodecError> {
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
                                let definitions = get_definitions(word.clone()).await;
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

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let listener = TcpListener::bind("0.0.0.0:2628").await.unwrap();
    loop {
        match listener.accept().await {
            Ok((stream, socket)) => {
                tokio::spawn(async move {
                    eprintln!("New connection at '{}:{}'", &socket.ip(), &socket.port());
                    handle_client(stream).await;
                });
            }
            Err(e) => {
                eprintln!("Exited with error: '{:?}'", e)
            }
        }
    }
}
