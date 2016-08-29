/*
 * *****************************************************************************
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
 * ***************************************************************************
 */

package com.spectralogic.ds3cli.models;

import com.google.common.collect.ImmutableList;
import com.spectralogic.ds3client.models.BulkObject;
import com.spectralogic.ds3client.models.SuspectBlobTape;

import java.util.List;

public class SuspectedObjectResult implements Result {

    private final ImmutableList<BulkObject> suspectBlobTapes;

    public SuspectedObjectResult(final ImmutableList<BulkObject> suspectBlobTapes) {
        this.suspectBlobTapes = suspectBlobTapes;
    }

    public ImmutableList<BulkObject> getSuspectBlobTapes() {
        return suspectBlobTapes;
    }
}