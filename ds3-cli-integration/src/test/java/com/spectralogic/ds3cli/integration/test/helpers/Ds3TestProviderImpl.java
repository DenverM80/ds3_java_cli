package com.spectralogic.ds3cli.integration.test.helpers;

import com.spectralogic.ds3cli.util.Ds3Provider;
import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers;

public class Ds3TestProviderImpl implements Ds3Provider{
    private final Ds3Client client;
    private final Ds3ClientHelpers helpers;

    public Ds3TestProviderImpl(final Ds3Client client, final Ds3ClientHelpers helpers) {
        this.client = client;
        this.helpers = helpers;
    }

    @Override
    public Ds3Client getClient() {
        return this.client;
    }

    @Override
    public Ds3ClientHelpers getClientHelpers() {
        return this.helpers;
    }
}
