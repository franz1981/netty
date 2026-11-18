#!/bin/bash
# ----------------------------------------------------------------------------
# Copyright 2026 The Netty Project
#
# The Netty Project licenses this file to you under the Apache License,
# version 2.0 (the "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at:
#
#   https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
# ----------------------------------------------------------------------------
#
# Fails if a build that ran with -Dio.netty.allocator.type=pagestore logged that ByteBufUtil fell back to the adaptive
# allocator: the page store was not on the class path of a module (a missing netty-incubator-allocator test dependency),
# or it failed to load. The texts are the ones of ByteBufUtil.loadAllocator, with the class and the artifact of the
# page store: ByteBufUtilTest calls loadAllocator with other classes and logs the same texts for them.
set -e

NOT_FOUND='-Dio.netty.allocator.type needs netty-incubator-allocator: io.netty.incubator.allocator.AdaptiveByteBufAllocator was not found'
FAILED='-Dio.netty.allocator.type: io.netty.incubator.allocator.AdaptiveByteBufAllocator from netty-incubator-allocator failed to load: using the adaptive allocator'

if [ "$#" -ne 1 ]; then
    echo "Expected build log as argument"
    exit 1
fi

if [ ! -f "$1" ]; then
    echo "Build log $1 not found"
    exit 1
fi

if grep -q -F -e "$NOT_FOUND" -e "$FAILED" "$1" ; then
    echo "The page store was not the default allocator, please inspect build log:"
    grep -F -e "$NOT_FOUND" -e "$FAILED" "$1" | head -n 20
    exit 1
else
    echo "The page store was the default allocator in every module that logged"
    exit 0
fi
