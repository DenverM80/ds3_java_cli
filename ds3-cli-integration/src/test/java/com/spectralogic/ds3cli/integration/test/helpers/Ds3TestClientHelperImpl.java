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

package com.spectralogic.ds3cli.integration.test.helpers;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.helpers.DataTransferredListener;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers;
import com.spectralogic.ds3client.helpers.JobRecoveryException;
import com.spectralogic.ds3client.helpers.options.ReadJobOptions;
import com.spectralogic.ds3client.helpers.options.WriteJobOptions;
import com.spectralogic.ds3client.models.Contents;
import com.spectralogic.ds3client.models.bulk.Ds3Object;
import com.spectralogic.ds3client.serializer.XmlProcessingException;

import java.io.IOException;
import java.nio.file.Path;
import java.security.SignatureException;
import java.util.UUID;

/**
 * Decorate the Ds3ClientHelpersImpl class in order to register a DataTransferredListener for startWriteJob()
 * and startReadJob()
 */
public class Ds3TestClientHelperImpl extends Ds3ClientHelpers {
    private final Ds3ClientHelpers helper;
    private TestPerformanceListener testPutPerformanceListener = null;
    private TestPerformanceListener testGetPerformanceListener = null;

    public Ds3TestClientHelperImpl(final Ds3Client client) {
        this.helper = Ds3ClientHelpers.wrap(client);
    }

    public double getAvgGetsMbps() {
        return this.testGetPerformanceListener.getAvgMbps();
    }

    public double putAvgPutsMbps() {
        return this.testPutPerformanceListener.getAvgMbps();
    }

    /**
     * Catch dataTransferred events in order to monitor throughput
     */
    private class TestPerformanceListener implements DataTransferredListener {
        private final long startTime;
        private double time;
        private long totalByteTransferred = 0;
        private long content;
        private double mbps = 0;
        private double highestMbps = 0.0;

        public TestPerformanceListener() {
            this.startTime = System.currentTimeMillis();
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

        public double getAvgMbps() {
            return this.mbps;
        }
    }

    /**
     * Register a DataTransferred Listener in order to measure throughput
     */
    @Override
    public Job startWriteJob(final String s, final Iterable<Ds3Object> iterable) throws SignatureException, IOException, XmlProcessingException {
        final Ds3ClientHelpers.Job job = helper.startWriteJob(s, iterable);
        this.testPutPerformanceListener = new TestPerformanceListener();
        job.attachDataTransferredListener(testPutPerformanceListener);
        return job;
    }

    /**
     * Register a DataTransferred Listener in order to measure throughput
     */
    @Override
    public Job startWriteJob(final String s, final Iterable<Ds3Object> iterable, final WriteJobOptions writeJobOptions) throws SignatureException, IOException, XmlProcessingException {
        final Ds3ClientHelpers.Job job = helper.startWriteJob(s, iterable, writeJobOptions);
        this.testPutPerformanceListener = new TestPerformanceListener();
        job.attachDataTransferredListener(testPutPerformanceListener);
        return job;
    }

    /**
     * Register a DataTransferred Listener in order to measure throughput
     */
    @Override
    public Job startReadJob(final String s, final Iterable<Ds3Object> iterable) throws SignatureException, IOException, XmlProcessingException {
        final Ds3ClientHelpers.Job job = helper.startReadJob(s, iterable);
        this.testGetPerformanceListener = new TestPerformanceListener();
        job.attachDataTransferredListener(testGetPerformanceListener);
        return job;
    }

    /**
     * Register a DataTransferred Listener in order to measure throughput
     */
    @Override
    public Job startReadJob(final String s, final Iterable<Ds3Object> iterable, final ReadJobOptions readJobOptions) throws SignatureException, IOException, XmlProcessingException {
        final Ds3ClientHelpers.Job job = helper.startReadJob(s, iterable, readJobOptions);
        this.testGetPerformanceListener = new TestPerformanceListener();
        job.attachDataTransferredListener(testGetPerformanceListener);
        return job;
    }

    /*** All other methods are passed through unchanged ***/

    @Override
    public Job startReadAllJob(final String s) throws SignatureException, IOException, XmlProcessingException {
        return helper.startReadAllJob(s);
    }

    @Override
    public Job startReadAllJob(final String s, final ReadJobOptions readJobOptions) throws SignatureException, IOException, XmlProcessingException {
        return helper.startReadAllJob(s);
    }

    @Override
    public Job recoverWriteJob(final UUID uuid) throws SignatureException, IOException, XmlProcessingException, JobRecoveryException {
        return helper.recoverWriteJob(uuid);
    }

    @Override
    public Job recoverReadJob(final UUID uuid) throws SignatureException, IOException, XmlProcessingException, JobRecoveryException {
        return helper.recoverReadJob(uuid);
    }

    @Override
    public void ensureBucketExists(final String s) throws IOException, SignatureException {
        helper.ensureBucketExists(s);
    }

    @Override
    public void ensureBucketExists(final String s, final UUID uuid) throws IOException, SignatureException {
        helper.ensureBucketExists(s, uuid);
    }

    @Override
    public Iterable<Contents> listObjects(final String s) throws SignatureException, IOException {
        return helper.listObjects(s);
    }

    @Override
    public Iterable<Contents> listObjects(final String s, final String s1) throws SignatureException, IOException {
        return helper.listObjects(s, s1);
    }

    @Override
    public Iterable<Contents> listObjects(final String s, final String s1, final String s2) throws SignatureException, IOException {
        return helper.listObjects(s, s1, s2);
    }

    @Override
    public Iterable<Contents> listObjects(final String s, final String s1, final String s2, final int i) throws SignatureException, IOException {
        return helper.listObjects(s, s1, s2, i);
    }

    @Override
    public Iterable<Ds3Object> listObjectsForDirectory(final Path path) throws IOException {
        return helper.listObjectsForDirectory(path);
    }

    @Override
    public Iterable<Ds3Object> addPrefixToDs3ObjectsList(final Iterable<Ds3Object> iterable, final String s) {
        return helper.addPrefixToDs3ObjectsList(iterable, s);
    }

    @Override
    public Iterable<Ds3Object> removePrefixFromDs3ObjectsList(final Iterable<Ds3Object> iterable, final String s) {
        return helper.removePrefixFromDs3ObjectsList(iterable, s);
    }
}
