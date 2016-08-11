/*
 * ******************************************************************************
 *   Copyright 2014-2015 Spectra Logic Corporation. All Rights Reserved.
 *   Licensed under the Apache License, Version 2.0 (the "License"). You may not use
 *   this file except in compliance with the License. A copy of the License is located at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file.
 *   This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 *   CONDITIONS OF ANY KIND, either express or implied. See the License for the
 *   specific language governing permissions and limitations under the License.
 * ****************************************************************************
 */

package com.spectralogic.ds3cli.integration;

import com.spectralogic.ds3cli.Arguments;
import com.spectralogic.ds3cli.Ds3Cli;
import com.spectralogic.ds3cli.Ds3ProviderImpl;
import com.spectralogic.ds3cli.FileUtilsImpl;
import com.spectralogic.ds3cli.integration.test.helpers.Ds3TestClientHelperImpl;
import com.spectralogic.ds3cli.integration.test.helpers.TempStorageIds;
import com.spectralogic.ds3cli.integration.test.helpers.TempStorageUtil;
import com.spectralogic.ds3cli.util.Ds3Provider;
import com.spectralogic.ds3cli.util.FileUtils;
import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.Ds3ClientBuilder;
import com.spectralogic.ds3client.models.ChecksumType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.security.SignatureException;
import java.util.UUID;

public class PerformanceIntegration_Test {
    private static final Logger LOG = LoggerFactory.getLogger(PerformanceIntegration_Test.class);

    private static final Ds3Client client = Ds3ClientBuilder.fromEnv().withHttps(false).build();
    private static final Ds3TestClientHelperImpl HELPERS = new Ds3TestClientHelperImpl(client);

    private static final String TEST_ENV_NAME = "FeatureIntegration_Test";
    private static TempStorageIds envStorageIds;
    private static UUID envDataPolicyId;

    private final String bucketName = "test_JavaCLI_performance";
    private String numberOfFiles;
    private String sizeOfFiles;
    private String numberOfThreads;

    @BeforeClass
    public static void startup() throws IOException, SignatureException {
        envDataPolicyId = TempStorageUtil.setupDataPolicy(TEST_ENV_NAME, false, ChecksumType.Type.MD5, client);
        envStorageIds = TempStorageUtil.setup(TEST_ENV_NAME, envDataPolicyId, client);
    }

    @AfterClass
    public static void teardown() throws IOException, SignatureException {
        TempStorageUtil.teardown(TEST_ENV_NAME, envStorageIds, client);
        client.close();
    }

    @Test
    public void testPerformance_10_x_1500mb_3_threads() throws Exception {
        LOG.info("Testing Performance for 10 x 1500mb files using 3 threads");
        numberOfFiles = "10";
        sizeOfFiles = "1500";
        numberOfThreads = "3";
        runPerformanceTest();

        LOG.info("  PUT: {}", HELPERS.putMbps());
        assert(HELPERS.putMbps() > 500);
        LOG.info("  GET: {}", HELPERS.getMbps());
        assert(HELPERS.getMbps() > 700);
    }

    private void runPerformanceTest() throws Exception {
        try {
            final Arguments args = new Arguments(new String[]{"--http",
                    "-c", "performance",
                    "-b", this.bucketName,
                    "-n", this.numberOfFiles,
                    "-nt", this.numberOfThreads,
                    "-s", this.sizeOfFiles});
            final Ds3Provider provider = new Ds3ProviderImpl(client, HELPERS);
            final FileUtils fileUtils = new FileUtilsImpl();
            final Ds3Cli runner = new Ds3Cli(provider, args, fileUtils);
            runner.call();
        } finally {
            Util.deleteBucket(client, bucketName);
        }
    }
}

