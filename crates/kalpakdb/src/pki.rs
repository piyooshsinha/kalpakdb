//! Mesh PKI: a cluster CA and a node certificate for mutually-authenticated
//! node-to-node TLS. Presenting a certificate signed by the cluster CA *is*
//! mesh membership — the internal trust boundary, replacing "whoever can
//! reach the port".

use rcgen::{BasicConstraints, CertificateParams, CertifiedIssuer, DnType, IsCa, KeyPair};

pub struct MeshPki {
    pub ca_pem: String,
    /// Node certificate (signed by the CA) and its key, used as both the
    /// mesh server identity and the mesh client identity.
    pub cert_pem: String,
    pub key_pem: String,
}

/// Generate a cluster CA plus one mesh certificate valid for `hosts`.
pub fn generate_mesh_pki(hosts: &[String]) -> Result<MeshPki, Box<dyn std::error::Error>> {
    let mut ca_params = CertificateParams::new(Vec::<String>::new())?;
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "kalpak-mesh-ca");
    let ca = CertifiedIssuer::self_signed(ca_params, KeyPair::generate()?)?;

    let mut leaf_params = CertificateParams::new(hosts.to_vec())?;
    leaf_params
        .distinguished_name
        .push(DnType::CommonName, "kalpak-mesh-node");
    let leaf_key = KeyPair::generate()?;
    let leaf = leaf_params.signed_by(&leaf_key, &ca)?;

    Ok(MeshPki {
        ca_pem: ca.pem(),
        cert_pem: leaf.pem(),
        key_pem: leaf_key.serialize_pem(),
    })
}

/// Write the PKI to `<dir>/mesh-{ca,cert,key}.pem`, returning the paths.
pub fn write_mesh_pki(
    dir: &str,
    hosts: &[String],
) -> Result<(String, String, String), Box<dyn std::error::Error>> {
    let pki = generate_mesh_pki(hosts)?;
    std::fs::create_dir_all(dir)?;
    let ca = format!("{dir}/mesh-ca.pem");
    let cert = format!("{dir}/mesh-cert.pem");
    let key = format!("{dir}/mesh-key.pem");
    std::fs::write(&ca, &pki.ca_pem)?;
    std::fs::write(&cert, &pki.cert_pem)?;
    std::fs::write(&key, &pki.key_pem)?;
    Ok((ca, cert, key))
}
