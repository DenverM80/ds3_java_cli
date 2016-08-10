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

import com.spectralogic.ds3cli.*;
import com.spectralogic.ds3cli.exceptions.CommandException;
import com.spectralogic.ds3cli.integration.test.helpers.Ds3TestProviderImpl;
import com.spectralogic.ds3cli.integration.test.helpers.TempStorageIds;
import com.spectralogic.ds3cli.integration.test.helpers.TempStorageUtil;
import com.spectralogic.ds3cli.util.Ds3Provider;
import com.spectralogic.ds3cli.util.FileUtils;
import com.spectralogic.ds3cli.util.MemoryObjectChannelBuilder;
import com.spectralogic.ds3cli.util.Utils;
import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.Ds3ClientBuilder;
import com.spectralogic.ds3client.commands.PutBucketRequest;
import com.spectralogic.ds3client.helpers.DataTransferredListener;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers;
import com.spectralogic.ds3client.helpers.ObjectCompletedListener;
import com.spectralogic.ds3client.models.ChecksumType;
import com.spectralogic.ds3client.models.bulk.Ds3Object;
import com.spectralogic.ds3client.networking.FailedRequestException;
import com.spectralogic.ds3client.serializer.XmlProcessingException;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.security.SignatureException;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;


public class PerformanceIntegration_Test {
    private static final Logger LOG = LoggerFactory.getLogger(PerformanceIntegration_Test.class);

    private static final Ds3Client client = Ds3ClientBuilder.fromEnv().withHttps(false).build();
    private static final Ds3ClientHelpers HELPERS = Ds3ClientHelpers.wrap(client);

    private static final String TEST_ENV_NAME = "FeatureIntegration_Test";
    private static TempStorageIds envStorageIds;
    private static UUID envDataPolicyId;

    private final String bucketName = "test_JavaCLI_performance";
    private int numberOfFiles;
    private long sizeOfFiles;
    private int bufferSize;
    private int numberOfThreads;

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
        try {
            numberOfFiles = 10;
            sizeOfFiles = 1500;
            numberOfThreads = 3;
            bufferSize = 1048576; //default to 1MB

            //final Ds3Provider provider = new Ds3TestProviderImpl(client, Ds3ClientHelpers.wrap(client));
            //final FileUtils fileUtils = new FileUtilsImpl();

            try {
                final PutBucketRequest request = new PutBucketRequest(bucketName);
                client.putBucket(request);
            } catch(final FailedRequestException e) {
                if (e.getStatusCode() == 409) {
                    // ignore conflict and continue
                }
                throw new CommandException("Encountered a DS3 Error", e);
            }

            final List<Ds3Object> objList = Utils.getDs3Objects(numberOfFiles, sizeOfFiles);

            /**** PUT ****/
            final PerformanceListener putTransferPerformance = transfer(numberOfFiles, sizeOfFiles, objList, true);

            /**** GET ****/
            final PerformanceListener getTransferPerformance = transfer(numberOfFiles, sizeOfFiles, objList, false);

            assert(putTransferPerformance.getMbps() > 500);
            assert(getTransferPerformance.getMbps() > 700);
        }
        finally
        {
            Util.deleteBucket(client, bucketName);
        }
    }

    private PerformanceListener transfer(final int numberOfFiles,
                                         final long sizeOfFiles,
                                         final List<Ds3Object> objList,
                                         final boolean isPutCommand) throws SignatureException, IOException, XmlProcessingException {
        final Ds3ClientHelpers.Job job;
        if (isPutCommand) {
            job = HELPERS.startWriteJob(this.bucketName, objList);
        } else {
            job = HELPERS.startReadJob(this.bucketName, objList);
        }
        job.withMaxParallelRequests(this.numberOfThreads);

        final PerformanceListener transferPerformanceListener = new PerformanceListener(
                System.currentTimeMillis()/*,
                numberOfFiles,
                numberOfFiles * sizeOfFiles,
                isPutCommand */);
        job.attachObjectCompletedListener(transferPerformanceListener);
        job.attachDataTransferredListener(transferPerformanceListener);
        job.transfer(new MemoryObjectChannelBuilder(bufferSize, sizeOfFiles));
        return transferPerformanceListener;
    }

    private class PerformanceListener implements DataTransferredListener, ObjectCompletedListener {
        private final long startTime;
        //private final int totalNumberOfFiles;
        //private final long numberOfMB;
        //private final boolean isPutCommand;
        private long totalByteTransferred = 0;
        //private int numberOfFiles = 0;
        private double highestMbps = 0.0;
        private double time;
        private long content;
        private double mbps;

        public PerformanceListener(final long startTime /*, final int totalNumberOfFiles, final long numberOfMB, final boolean isPutCommand*/) {
            this.startTime = startTime;
            //this.totalNumberOfFiles = totalNumberOfFiles;
            //this.numberOfMB = numberOfMB;
            //this.isPutCommand = isPutCommand;
        }

        @Override
        public void dataTransferred(final long size) {
            final long currentTime = System.currentTimeMillis();
            synchronized (this) {
                totalByteTransferred += size;
                time = (currentTime - this.startTime == 0) ? 1.0 : (currentTime - this.startTime) / 1000D;
                content = totalByteTransferred / 1024L / 1024L;
                mbps = content / time;
                if (mbps > highestMbps) highestMbps = mbps;
            }
        }

        @Override
        public void objectCompleted(final String s) {
            synchronized (this) {
                numberOfFiles += 1;
            }
        }

        public double getMbps() {
            return this.mbps;
        }

        /*
        public double getHighestMbps() {
            return this.highestMbps;
        }
        */
    }
}

