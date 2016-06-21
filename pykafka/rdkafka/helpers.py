def rdk_ssl_config(cluster):
    """Generate rdkafka config keys from cluster's ssl_config"""
    if cluster._ssl_config is None:  # plaintext connections
        return {}
    else:
        # We obtain the ciphers used by the existing BrokerConnections, so the
        # security level for our rdkafka connections is never lower than that
        ciphers = _get_ciphers_from_sockets(cluster.brokers)
        assert ciphers

        conf = cluster._ssl_config
        rdk_conf = {
            "security.protocol": "ssl",
            "ssl.cipher.suites": ciphers,
            "ssl.ca.location": conf.cafile,
            "ssl.certificate.location": conf.certfile,
            "ssl.key.location": conf.keyfile,
            "ssl.key.password": conf.password}
        # Some of the values above may not have been configured:
        return {k: v for k, v in rdk_conf.items() if v is not None}


def _get_ciphers_from_sockets(brokers):
    """Obtain ciphers currently used by pykafka BrokerConnections"""
    ciphers = set()
    for b in brokers.values():
        ciph = b._connection._socket.cipher()
        if ciph is not None:
            ciphers.add(ciph[0])
    return ','.join(ciphers)

