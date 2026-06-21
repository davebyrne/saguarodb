use std::path::Path;
use std::sync::Arc;

use common::{DbError, Result};
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::rustls::crypto::ring;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};

/// Build a server-side TLS acceptor from a PEM certificate chain and private
/// key on disk. The acceptor presents the certificate and requires no client
/// certificate (server-side TLS only). The `ring` crypto provider is selected
/// explicitly so the build does not depend on a process-default provider.
pub fn build_acceptor(cert_file: &Path, key_file: &Path) -> Result<TlsAcceptor> {
    let certs = load_certs(cert_file)?;
    let key = load_key(key_file)?;

    let config = ServerConfig::builder_with_provider(Arc::new(ring::default_provider()))
        .with_safe_default_protocol_versions()
        .map_err(|err| DbError::io(format!("failed to configure TLS protocol versions: {err}")))?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|err| DbError::io(format!("invalid TLS certificate or key: {err}")))?;

    Ok(TlsAcceptor::from(Arc::new(config)))
}

fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>> {
    let pem = std::fs::read(path).map_err(|err| {
        DbError::io(format!(
            "failed to read TLS cert file {}: {err}",
            path.display()
        ))
    })?;
    let certs = rustls_pemfile::certs(&mut pem.as_slice())
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|err| {
            DbError::io(format!(
                "failed to parse TLS cert file {}: {err}",
                path.display()
            ))
        })?;
    if certs.is_empty() {
        return Err(DbError::io(format!(
            "TLS cert file {} contains no certificates",
            path.display()
        )));
    }
    Ok(certs)
}

fn load_key(path: &Path) -> Result<PrivateKeyDer<'static>> {
    let pem = std::fs::read(path).map_err(|err| {
        DbError::io(format!(
            "failed to read TLS key file {}: {err}",
            path.display()
        ))
    })?;
    rustls_pemfile::private_key(&mut pem.as_slice())
        .map_err(|err| {
            DbError::io(format!(
                "failed to parse TLS key file {}: {err}",
                path.display()
            ))
        })?
        .ok_or_else(|| {
            DbError::io(format!(
                "TLS key file {} contains no private key",
                path.display()
            ))
        })
}

#[cfg(test)]
mod tests {
    use super::build_acceptor;

    fn write_self_signed(dir: &std::path::Path) -> (std::path::PathBuf, std::path::PathBuf) {
        let generated = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let cert_path = dir.join("server.crt");
        let key_path = dir.join("server.key");
        std::fs::write(&cert_path, generated.cert.pem()).unwrap();
        std::fs::write(&key_path, generated.signing_key.serialize_pem()).unwrap();
        (cert_path, key_path)
    }

    #[test]
    fn builds_acceptor_from_valid_cert_and_key() {
        let dir = tempfile::tempdir().unwrap();
        let (cert, key) = write_self_signed(dir.path());

        assert!(build_acceptor(&cert, &key).is_ok());
    }

    #[test]
    fn missing_cert_file_is_an_io_error() {
        let dir = tempfile::tempdir().unwrap();
        let (_, key) = write_self_signed(dir.path());

        let err = build_acceptor(&dir.path().join("absent.crt"), &key)
            .err()
            .unwrap();
        assert_eq!(err.kind, common::ErrorKind::Io);
    }

    #[test]
    fn empty_cert_file_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let (cert, key) = write_self_signed(dir.path());
        std::fs::write(&cert, b"").unwrap();

        assert!(build_acceptor(&cert, &key).is_err());
    }
}
