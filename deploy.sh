#!/usr/bin/env bash

if [ -z "$GPG_PRIVATE_KEY_ENCRYPTION_IV" ]; then
    exit 0
fi

openssl aes-256-cbc -K $GPG_PRIVATE_KEY_ENCRYPTION_KEY -iv $GPG_PRIVATE_KEY_ENCRYPTION_IV \
    -in gpg/paradoxical-io-private.gpg.enc -out gpg/paradoxical-io-private.gpg -d

if [ -n "$TRAVIS_TAG" ]; then
    echo "Deploying release version"
    mvn clean deploy --settings settings.xml -DskipTests -P release -Drevision=''
    exit $?
elif [ "$TRAVIS_BRANCH" = "master" ]; then
    echo "Deploying snapshot version"
    mvn clean deploy --settings settings.xml -DskipTests
    exit $?
fi
