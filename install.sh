#!/bin/bash
set -e

if [[ $TRAVIS_OS_NAME == 'linux' ]]; then
    #install pacakges
    sudo apt-get -y install lcov mpich zlib1g-dev libssl-dev libgtest-dev

    #install gtest
    cd /usr/src/gtest
    sudo cmake .
    sudo make
    sudo mv libgtest* /usr/lib/
    cd $TRAVIS_BUILD_DIR

    #install coveralls-lcov
    gem install coveralls-lcov
    
    #make tileDB
    make -j 2 

fi

if [[ $TRAVIS_OS_NAME == 'osx' ]]; then
    #install packages
    brew update
    brew install openssl lcov doxygen
    #brew reinstall gcc --without-multilib

    #install gtest
    wget https://github.com/google/googletest/archive/release-1.7.0.tar.gz
    tar xf release-1.7.0.tar.gz
    cd googletest-release-1.7.0
    CXX=g++-4.9 cmake .
    CXX=g++-4.9 make
    sudo mv include/gtest /usr/include/gtest
    sudo mv libgtest_main.a libgtest.a /usr/lib/
    cd $TRAVIS_BUILD_DIR

    #install coveralls-lcov
    gem install coveralls-lcov

    #make tileDB
    make CXX=g++-4.9 -j 2
fi
