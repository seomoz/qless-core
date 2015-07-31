#! /usr/bin/env bash

set -e

# Some dependencies
sudo apt-get update
sudo apt-get install -y libhiredis-dev libevent-dev python-pip python-dev

# Some configuration files
(
    cd /vagrant/provision
    sudo cp etc/redis.conf /etc/redis.conf
    sudo cp etc/init/redis.conf /etc/init/redis.conf
    sudo cp etc/sysctl.d/99-overcommit.conf /etc/sysctl.d/99-overcommit.conf
)

# Install redis
(
    # Make a redis user and space for redis
    sudo useradd -U -s /bin/false -d /dev/null redis
    sudo mkdir -p /var/redis
    sudo chown redis:redis /var/redis

    # Download and install the thing
    cd /tmp
    wget http://download.redis.io/releases/redis-2.8.19.tar.gz
    tar xf redis-2.8.19.tar.gz
    (
        cd redis-2.8.19
        make
        sudo make install
        sudo service redis start
    )
    sudo rm -r redis-2.8.19{,.tar.gz}
)

# Install python dependencies
(
    cd /vagrant/
    sudo pip install -r requirements.txt
)
