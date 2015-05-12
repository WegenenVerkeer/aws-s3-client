package be.wegenenverkeer.s3

import java.security.KeyFactory
import java.security.KeyPair
import java.security.PrivateKey
import java.security.PublicKey
import java.security.spec.PKCS8EncodedKeySpec
import java.security.spec.X509EncodedKeySpec
import org.apache.commons.io.FileUtils;

/**
 * Created by Karel Maesen, Geovise BVBA on 21/04/15.
 */
class KeyLoader {

    static def loadKeyPair(String privateKeyPath, String publicKeyPath) {
        PrivateKey pk = null
        PublicKey publicKey = null
        if (privateKeyPath) {
            byte[] bytes = FileUtils.readFileToByteArray(new File(privateKeyPath));
            KeyFactory kf = KeyFactory.getInstance("RSA");
            PKCS8EncodedKeySpec ks = new PKCS8EncodedKeySpec(bytes);
            pk = kf.generatePrivate(ks);
        }
        if (publicKeyPath) {
            byte[] bytes = FileUtils.readFileToByteArray(new File(publicKeyPath));
            publicKey = KeyFactory.getInstance("RSA").generatePublic(new X509EncodedKeySpec(bytes));
        }
        return new KeyPair(publicKey, pk);
    }

}
