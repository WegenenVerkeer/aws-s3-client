#! /bin/bash

## source is: http://codeartisan.blogspot.be/2009/05/public-key-cryptography-in-java.html

# generate a 2048-bit RSA private key
$ openssl genrsa -out private_key.pem 2048

# convert private Key to PKCS#8 format (so Java can read it)
$ openssl pkcs8 -topk8 -inform PEM -outform DER -in private_key.pem \
    -out private_key.der -nocrypt

# output public key portion in DER format (so Java can read it)
$ openssl rsa -in private_key.pem -pubout -outform DER -out public_key.der

#You keep private_key.pem around for reference, but you hand the DER versions to your Java programs.


