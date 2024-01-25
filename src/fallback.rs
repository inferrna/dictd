use custom_error::custom_error;
use futures_util::SinkExt;
use tokio::io;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LinesCodec, LinesCodecError};

custom_error! {pub FallbackError
    IOError{source: io::Error } = "IO error",
    LinesError{source: LinesCodecError } = "Lines error",
}

#[inline]
fn is_5or2(a: &char) -> bool {
    "25".contains(|x: char| x.eq(a))
}
pub(crate) async fn query_dictd_server(server_address: &str, dictionary: &str, word: &str, output: &mut Framed<TcpStream, LinesCodec>) -> Result<(), FallbackError> {
    // Connect to the dictd server
    let mut stream = TcpStream::connect(server_address).await?;

    // Formulate the DEFINE command
    let command = format!("DEFINE {} {}\n", dictionary, word);

    // Send the command to the server
    stream.write_all(command.as_bytes()).await?;

    // Read the response from the server
    let reader = io::BufReader::new(&mut stream);
    let mut lines = reader.lines();

    while let Some(line) = lines.next_line().await? {
        // Check for a line starting with "2" (success) or "5" (error)
        let should_break = if line.len() > 2 {
            let first = is_5or2(&line.chars().nth(0).unwrap());
            let second = line.chars().nth(1).unwrap().is_digit(10);
            let third = line.chars().nth(3).unwrap().is_digit(10);
            first && second && third
        } else {
            false
        };
        output.send(line).await?;
        if should_break {
            break
        }
    }

    Ok(())
}