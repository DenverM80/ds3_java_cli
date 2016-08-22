/*
 * ******************************************************************************
 *   Copyright 2014-2016 Spectra Logic Corporation. All Rights Reserved.
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
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers;
import com.spectralogic.ds3client.models.ChecksumType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.SignatureException;
import java.util.UUID;

import static org.junit.Assert.assertTrue;

public class PerformanceIntegration_Test {
    private static final Logger LOG = LoggerFactory.getLogger(PerformanceIntegration_Test.class);

    private static final Ds3Client client = Ds3ClientBuilder.fromEnv().withHttps(false).build();

    private static final String TEST_ENV_NAME = "PerformanceIntegration_Test";
    private static TempStorageIds envStorageIds;
    private static UUID envDataPolicyId;

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
    public void testPerformance_2() throws Exception {
        final Ds3ClientHelpers helper = Ds3ClientHelpers.wrap(client);
        LOG.info("Testing Performance for 10 x 500mb files using 3 threads");
        final String bucketId = "perf2";
        final String numberOfFiles = "10";
        final String sizeOfFiles = "500";
        final String numberOfThreads = "3";
        runPerformanceTest(helper, bucketId, numberOfFiles, sizeOfFiles, numberOfThreads);
    }

    /*
    @Test
    public void testPerformance_10_x_1500mb_3_threads() throws Exception {
        final Ds3TestClientHelperImpl helper = new Ds3TestClientHelperImpl(client);
        LOG.info("Testing Performance for 10 x 1500mb files using 3 threads");
        final String bucketId = "performance_10_x_1500mb_3_threads";
        final String numberOfFiles = "10";
        final String sizeOfFiles = "1500";
        final String numberOfThreads = "3";
        runPerformanceTest(helper, bucketId, numberOfFiles, sizeOfFiles, numberOfThreads);

        LOG.info("  PUT AvgMps: {}", helper.putAvgPutsMbps());
        assertTrue(helper.putAvgPutsMbps() > 500);
        LOG.info("  GET AvgMps: {}", helper.getAvgGetsMbps());
        assertTrue(helper.getAvgGetsMbps() > 700);
    }
    */

    private void runPerformanceTest(
            final Ds3ClientHelpers helper,
            final String bucketId,
            final String numberOfFiles,
            final String sizeOfFiles,
            final String numberOfThreads) throws Exception {
        try {
            final Arguments args = new Arguments(
                new String[]{
                    "--http",
                    "-c", "performance",
                    "-b", bucketId,
                    "-n", numberOfFiles,
                    "-nt", numberOfThreads,
                    "-s", sizeOfFiles});
            final Ds3Provider provider = new Ds3ProviderImpl(client, helper);
            final FileUtils fileUtils = new FileUtilsImpl();
            final Ds3Cli runner = new Ds3Cli(provider, args, fileUtils);
            runner.call();
        } finally {
            Util.deleteBucket(client, TEST_ENV_NAME);
        }
    }
}

