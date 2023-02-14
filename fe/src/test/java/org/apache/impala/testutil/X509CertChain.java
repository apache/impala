// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.testutil;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.Security;
import java.security.Signature;
import java.security.SignatureException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Date;

import org.bouncycastle.asn1.ASN1EncodableVector;
import org.bouncycastle.asn1.ASN1Encoding;
import org.bouncycastle.asn1.ASN1Integer;
import org.bouncycastle.asn1.DERBitString;
import org.bouncycastle.asn1.DERNull;
import org.bouncycastle.asn1.DERSequence;
import org.bouncycastle.asn1.pkcs.PKCSObjectIdentifiers;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.AlgorithmIdentifier;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.ExtendedKeyUsage;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.ExtensionsGenerator;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.asn1.x509.TBSCertificate;
import org.bouncycastle.asn1.x509.Time;
import org.bouncycastle.asn1.x509.V3TBSCertificateGenerator;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.MiscPEMGenerator;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemWriter;

/**
 * Stateful class that generates X509 CA and leaf certificates.
 */
public class X509CertChain {

  private static final String SHA256_WITH_RSA = "SHA256withRSA";
  private static final AlgorithmIdentifier SIGNATURE_SHA256_RSA = new
      AlgorithmIdentifier(PKCSObjectIdentifiers.sha256WithRSAEncryption,
      DERNull.INSTANCE);

  private static final KeyUsage KEY_USAGE_CERT_SIGN = new KeyUsage(KeyUsage.keyCertSign |
      KeyUsage.cRLSign);
  private static final KeyUsage KEY_USAGE_SERVER_AUTH = new KeyUsage(
      KeyUsage.digitalSignature | KeyUsage.keyEncipherment);
  private static final BasicConstraints CONSTRAINT_CA = new BasicConstraints(true);

  private final KeyPair rootCaKp_;
  private final KeyPair leafKp_;
  private final X509Certificate rootCert_;
  private final X509Certificate leafCert_;

  public X509CertChain(String rootCaCertCN, String rootLeafCertCN)
      throws NoSuchAlgorithmException, NoSuchProviderException,
      InvalidKeyException, SignatureException, IOException, CertificateException  {
    Security.addProvider(new BouncyCastleProvider());

    KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA", "BC");
    kpg.initialize(2048);
    this.rootCaKp_ = kpg.generateKeyPair();
    this.leafKp_ = kpg.generateKeyPair();

    rootCert_ = generateRootCACert(rootCaCertCN, this.rootCaKp_);
    leafCert_ = generateLeafCert(rootLeafCertCN, this.leafKp_, this.rootCert_,
        this.rootCaKp_.getPrivate());
  }

  /**
   * Generates a string representation of the PEM-encoded root certificate.
   */
  public String rootCertAsPemString() throws CertificateEncodingException, IOException {
    return this.certToPem(this.rootCert_);
  }

  /**
   * Generates a string representation of the PEM-encoded leaf certificate.
   */
  public String leafCertAsPemString() throws CertificateEncodingException, IOException {
    return this.certToPem(this.leafCert_);
  }

  /**
   * Writes the PEM encoded root certificate to the provided java.io.Writer. The writer
   * is not flushed after the cert is written to it.
   *
   * @param w java.io.Writer where the PEM-encoded root certificate will be written.
   */
  public void writeRootCertAsPem(Writer w)
      throws CertificateEncodingException, IOException {
    this.certToPem(this.rootCert_, w);
  }

  /**
   * Writes the PEM-encoded leaf certificate to the provided java.io.Writer. The writer
   * is not flushed after the cert is written to it.
   *
   * @param w java.io.Writer where the PEM encoded leaf certificate will be written.
   */
  public void writeLeafCertAsPem(Writer w)
      throws CertificateEncodingException, IOException {
    this.certToPem(this.leafCert_, w);
  }

  /**
   * Writes the PEM-encoded RSA private key of the leaf certificate to the provided
   * java.io.Writer.  The writer is not fluished after the cert is written to it.
   *
   * @param w java.io.Writer where the PEM encoded leaf private key will be written.
   */
  public void writeLeafPrivateKeyAsPem(Writer w) throws IOException {
    PemObject o = new PemObject("RSA PRIVATE KEY",
        this.leafKp_.getPrivate().getEncoded());
    PemWriter pw = new PemWriter(w);

    pw.writeObject(o);
    pw.close();
  }

  public X509Certificate getRootCert() {
    return this.rootCert_;
  }

  public X509Certificate getLeafCert() {
    return this.leafCert_;
  }

  private X509Certificate generateRootCACert(String commonName, KeyPair kp)
      throws NoSuchAlgorithmException, NoSuchProviderException, InvalidKeyException,
      SignatureException, IOException, CertificateException {
    V3TBSCertificateGenerator gen = new V3TBSCertificateGenerator();
    X500Name cn = new X500Name(String.format("CN=%s", commonName));
    ExtensionsGenerator extGenerator = new ExtensionsGenerator();
    SubjectPublicKeyInfo subj;

    subj = SubjectPublicKeyInfo.getInstance(kp.getPublic().getEncoded());

    extGenerator.addExtension(Extension.keyUsage, false, KEY_USAGE_CERT_SIGN);
    extGenerator.addExtension(Extension.basicConstraints, false, CONSTRAINT_CA);

    // set the certificate start to an hour ago and the certificate end to an hour
    // from now
    gen.setStartDate(new Time(new Date(System.currentTimeMillis() - 60 * 1000)));
    gen.setEndDate(new Time(new Date(System.currentTimeMillis() + 60 * 60 * 1000)));

    gen.setSerialNumber(new ASN1Integer(1));
    gen.setIssuer(cn);
    gen.setSubject(cn);
    gen.setSignature(SIGNATURE_SHA256_RSA);
    gen.setSubjectPublicKeyInfo(subj);
    gen.setExtensions(extGenerator.generate());

    TBSCertificate tbsCert = gen.generateTBSCertificate();
    Signature sig = Signature.getInstance(SHA256_WITH_RSA, "BC");

    sig.initSign(kp.getPrivate());
    sig.update(gen.generateTBSCertificate().getEncoded(ASN1Encoding.DER));

    ASN1EncodableVector v = new ASN1EncodableVector();

    v.add(tbsCert);
    v.add(SIGNATURE_SHA256_RSA);
    v.add(new DERBitString(sig.sign()));

    return (X509Certificate)CertificateFactory.getInstance("X.509", "BC")
        .generateCertificate(new ByteArrayInputStream(new DERSequence(v)
        .getEncoded(ASN1Encoding.DER)));
  }

  private X509Certificate generateLeafCert(String commonName, KeyPair kp, X509Certificate
      issuerCert, PrivateKey issuerPrivateKey) throws IOException,
      NoSuchAlgorithmException, NoSuchProviderException, InvalidKeyException,
      SignatureException, CertificateException {
    V3TBSCertificateGenerator gen = new V3TBSCertificateGenerator();
    X500Name cn = new X500Name(String.format("CN=%s", commonName));
    ExtensionsGenerator extGenerator = new ExtensionsGenerator();
    SubjectPublicKeyInfo subj;
    X500Name issuerSubj;

    issuerSubj = new X500Name(issuerCert.getSubjectX500Principal().getName());

    subj = SubjectPublicKeyInfo.getInstance(kp.getPublic().getEncoded());

    extGenerator.addExtension(Extension.keyUsage, false, KEY_USAGE_SERVER_AUTH);
    extGenerator.addExtension(Extension.extendedKeyUsage, false, new ExtendedKeyUsage(
        new KeyPurposeId[]{KeyPurposeId.id_kp_serverAuth,
        KeyPurposeId.id_kp_clientAuth}));

    // set the certificate start to an hour ago and the certificate end to an hour
    // from now
    gen.setStartDate(new Time(new Date(System.currentTimeMillis() - 60 * 1000)));
    gen.setEndDate(new Time(new Date(System.currentTimeMillis() + 60 * 60 * 1000)));

    gen.setSerialNumber(new ASN1Integer(2));
    gen.setIssuer(issuerSubj);
    gen.setSubject(cn);
    gen.setSignature(SIGNATURE_SHA256_RSA);
    gen.setSubjectPublicKeyInfo(subj);
    gen.setExtensions(extGenerator.generate());

    TBSCertificate leafCert = gen.generateTBSCertificate();
    Signature sig = Signature.getInstance(SHA256_WITH_RSA, "BC");

    sig.initSign(issuerPrivateKey);
    sig.update(gen.generateTBSCertificate().getEncoded(ASN1Encoding.DER));

    ASN1EncodableVector v = new ASN1EncodableVector();

    v.add(leafCert);
    v.add(SIGNATURE_SHA256_RSA);
    v.add(new DERBitString(sig.sign()));

    return (java.security.cert.X509Certificate)CertificateFactory
        .getInstance("X.509", "BC").generateCertificate(new ByteArrayInputStream(
        new DERSequence(v).getEncoded(ASN1Encoding.DER)));
  }

  private void certToPem(X509Certificate cert, Writer writer) throws IOException,
      CertificateEncodingException {
    X509CertificateHolder bundle = new X509CertificateHolder(cert.getEncoded());
    PemWriter pw = new PemWriter(writer);
    pw.writeObject(new MiscPEMGenerator(bundle));
    pw.close();
  }

  private String certToPem(X509Certificate cert) throws IOException,
      CertificateEncodingException {
    StringWriter pemOut = new StringWriter();
    this.certToPem(cert, pemOut);
    pemOut.flush();

    return pemOut.toString();
  }

}
