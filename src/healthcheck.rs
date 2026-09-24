use reqwest::StatusCode;
use reqwest::blocking::Client;
use std::fmt::{Display, Formatter};

#[derive(Debug, PartialEq)]
pub enum HealthcheckError {
    RequestFailed,
    HttpStatus(StatusCode),
}

impl Display for HealthcheckError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RequestFailed => formatter.write_str("request failed"),
            Self::HttpStatus(status) => write!(formatter, "endpoint returned HTTP {status}"),
        }
    }
}

impl std::error::Error for HealthcheckError {}

pub fn call_healthcheck(client: &Client, url: &str) -> Result<StatusCode, HealthcheckError> {
    let response = client
        .get(url)
        .send()
        .map_err(|_| HealthcheckError::RequestFailed)?;
    let status = response.status();
    if !status.is_success() {
        return Err(HealthcheckError::HttpStatus(status));
    }
    Ok(status)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn calls_healthcheck_with_get() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut request = [0_u8; 1024];
            let bytes_read = stream.read(&mut request).unwrap();
            assert!(String::from_utf8_lossy(&request[..bytes_read]).starts_with("GET /ping "));
            stream
                .write_all(
                    b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .unwrap();
        });
        let client = Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();

        let status = call_healthcheck(&client, &format!("http://{address}/ping")).unwrap();
        assert_eq!(status, StatusCode::NO_CONTENT);
        server.join().unwrap();
    }

    #[test]
    fn reports_unsuccessful_http_status_without_url() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0_u8; 1024];
            let bytes_read = stream.read(&mut request).unwrap();
            assert!(bytes_read > 0);
            stream
                .write_all(b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .unwrap();
        });
        let client = Client::new();

        let error =
            call_healthcheck(&client, &format!("http://{address}/secret-token")).unwrap_err();
        assert_eq!(
            error,
            HealthcheckError::HttpStatus(StatusCode::INTERNAL_SERVER_ERROR)
        );
        assert!(!error.to_string().contains("secret-token"));
        server.join().unwrap();
    }
}
