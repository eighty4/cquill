use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose, SanType, string::Ia5String,
};
use rcgen::{Certificate, Issuer, SigningKey};
use std::fs::write;
use std::net::{IpAddr, Ipv4Addr};
use std::path::PathBuf;
use temp_dir::TempDir;

#[derive(Debug)]
pub struct StandardTlsCerts {
    pub server_cert: PathBuf,
    pub server_key: PathBuf,
    pub ca_cert: PathBuf,
    pub temp_dir: TempDir,
}

#[derive(Debug)]
pub struct MutualTlsCerts {
    pub server_cert: PathBuf,
    pub server_key: PathBuf,
    pub ca_cert: PathBuf,
    pub client_cert: PathBuf,
    pub client_key: PathBuf,
    pub client_truststore: PathBuf,
    pub temp_dir: TempDir,
}

pub fn gen_tls_certs() -> StandardTlsCerts {
    let (ca_issuer, ca_cert) = make_server_ca_cert();
    let (server_cert, server_key_pair) = make_server_cert_and_key_pair(&ca_issuer);
    let temp_dir = TempDir::new().unwrap();
    StandardTlsCerts {
        ca_cert: write_cert(&temp_dir, "ca_cert.pem", ca_cert.pem()),
        server_cert: write_cert(&temp_dir, "server_cert.pem", server_cert.pem()),
        server_key: write_cert(&temp_dir, "server_key.pem", server_key_pair.serialize_pem()),
        temp_dir,
    }
}

pub fn gen_mtls_certs() -> MutualTlsCerts {
    let (ca_issuer, ca_cert) = make_server_ca_cert();
    let (server_cert, server_key_pair) = make_server_cert_and_key_pair(&ca_issuer);
    let (client_ca_issuer, client_ca_cert) = make_client_ca_cert();
    let (client_cert, client_key_pair) = make_client_cert_and_key(&client_ca_issuer);
    let temp_dir = TempDir::new().unwrap();
    MutualTlsCerts {
        ca_cert: write_cert(&temp_dir, "ca_cert.pem", ca_cert.pem()),
        server_cert: write_cert(&temp_dir, "server_cert.pem", server_cert.pem()),
        server_key: write_cert(&temp_dir, "server_key.pem", server_key_pair.serialize_pem()),
        client_truststore: write_cert(&temp_dir, "client_truststore.pem", client_ca_cert.pem()),
        client_cert: write_cert(&temp_dir, "client_cert.pem", client_cert.pem()),
        client_key: write_cert(&temp_dir, "client_key.pem", client_key_pair.serialize_pem()),
        temp_dir,
    }
}

fn write_cert(temp_dir: &TempDir, filename: &str, content: String) -> PathBuf {
    let abs_path = temp_dir.child(filename);
    write(&abs_path, content).unwrap();
    abs_path
}

fn make_server_ca_cert() -> (Issuer<'static, KeyPair>, Certificate) {
    make_ca_cert("Database Root CA")
}

fn make_client_ca_cert() -> (Issuer<'static, KeyPair>, Certificate) {
    make_ca_cert("Database Client CA")
}

fn make_ca_cert(common_name: &str) -> (Issuer<'static, KeyPair>, Certificate) {
    let mut distinguished_name = DistinguishedName::new();
    distinguished_name.push(DnType::CommonName, common_name);
    distinguished_name.push(DnType::CountryName, "US");
    let mut params = CertificateParams::default();
    params.distinguished_name = distinguished_name;
    params.key_identifier_method = rcgen::KeyIdMethod::Sha256;
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    let ca_keypair = KeyPair::generate().unwrap();
    let ca_cert = params.self_signed(&ca_keypair).unwrap();
    (Issuer::new(params, ca_keypair), ca_cert)
}

fn make_server_cert_and_key_pair(
    ca_issuer: &Issuer<'static, impl SigningKey>,
) -> (Certificate, KeyPair) {
    let mut params = CertificateParams::default();
    params.key_identifier_method = rcgen::KeyIdMethod::Sha256;
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, "apacheway");
    params.distinguished_name = dn;
    params.subject_alt_names = vec![
        SanType::DnsName(Ia5String::try_from("localhost").unwrap()),
        SanType::IpAddress(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))),
    ];
    params.is_ca = IsCa::NoCa;
    let keypair = KeyPair::generate().unwrap();
    let cert = params.signed_by(&keypair, ca_issuer).unwrap();
    (cert, keypair)
}

fn make_client_cert_and_key(
    ca_issuer: &Issuer<'static, impl SigningKey>,
) -> (Certificate, KeyPair) {
    let mut params = CertificateParams::default();
    params.key_identifier_method = rcgen::KeyIdMethod::Sha256;
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, "cassandra");
    dn.push(DnType::OrganizationName, "eighty4");
    dn.push(DnType::OrganizationalUnitName, "Dx");
    params.distinguished_name = dn;
    // params.subject_alt_names = vec![SanType::Rfc822Name(
    //     Ia5String::try_from("user@example.com").unwrap(),
    // )];
    params.is_ca = IsCa::NoCa;
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    let key_pair = KeyPair::generate().unwrap();
    let cert = params.signed_by(&key_pair, ca_issuer).unwrap();
    (cert, key_pair)
}
